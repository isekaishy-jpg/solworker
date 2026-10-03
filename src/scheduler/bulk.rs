//! Bounded owned-prefix preparation, commitment and publication.

use super::{
    Accounting, AccountingLease, Activation, CompletionSink, Decision, Envelope, Finish,
    JobEnvelope, JobHandle, OwnedScheduler, Record, SWBatchSpawnOptions, SWBatchSpawnRejected,
    SWBatchSpawnResult, SWCost, SWDependencyPolicy, SWProducerControl, SWSpawnError, SWTask,
    SWTaskStatus, Stage, SubmitExtras, SubmitRequest, enqueue_activation, finish_payload,
    map_reservation_error,
};
use crate::runtime::{RuntimeControl, config::SWExecutionClass};
use std::collections::VecDeque;
use std::sync::{Arc, Weak, atomic::Ordering};

pub(super) const QUANTUM: usize = 64;

#[cfg(test)]
#[path = "../../tests/unit/bulk_review.rs"]
mod review_tests;

#[cfg(test)]
#[path = "../../tests/unit/batch_singleton.rs"]
mod singleton_tests;

// This concrete allocation remains recoverable until class commitment. Coercing
// its Box to JobEnvelope after validation neither allocates nor captures a lock.
struct PreparedEnvelope<P, T> {
    payload: P,
    run: fn(P) -> T,
    application_failed: fn(&T) -> bool,
    sink: CompletionSink<T>,
}

impl<P: Send + 'static, T: Send + 'static> JobEnvelope for PreparedEnvelope<P, T> {
    fn settle(self: Box<Self>, decision: Decision) -> Finish {
        let Self {
            payload,
            run,
            application_failed,
            sink,
        } = *self;
        finish_payload(payload, run, application_failed, sink, decision)
    }
}

struct Prepared<P, T: Send + 'static> {
    job: JobHandle,
    envelope: Box<PreparedEnvelope<P, T>>,
    record: Record,
    receipt: (SWTask<T>, SWProducerControl),
}

impl<P, T: Send + 'static> Prepared<P, T> {
    fn recover(self) -> P {
        let Self {
            job,
            envelope,
            record,
            receipt,
        } = self;
        let PreparedEnvelope {
            payload,
            run: _,
            application_failed: _,
            sink,
        } = *envelope;
        // A provisional runtime token is the final lifetime marker. Refund
        // every scheduler/capacity charge before its terminal progress wake.
        let Record {
            admission,
            capacity,
            charge,
            subscriptions,
            completion,
            group,
            deferred_finish,
            work_set,
            ..
        } = record;
        drop((
            capacity,
            charge,
            subscriptions,
            completion,
            group,
            deferred_finish,
            work_set,
        ));
        drop(sink);
        drop(receipt);
        drop(job);
        drop(admission);
        payload
    }
}

impl Accounting {
    // Reserve an available positive prefix in two independently rolled-back
    // atomic domains. Each resulting token refunds exactly one member.
    fn acquire_many(
        self: &Arc<Self>,
        edges: usize,
        maximum: usize,
    ) -> Result<Vec<AccountingLease>, SWSpawnError> {
        if edges > self.limits.edges {
            return Err(SWSpawnError::TooLarge);
        }
        let previous = self
            .records
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                let members = maximum.min(self.limits.records.saturating_sub(count));
                (members != 0).then(|| count + members)
            })
            .map_err(|_| SWSpawnError::Full)?;
        let reserved = maximum.min(self.limits.records - previous);
        let members = match self.limits.edges.checked_div(edges) {
            None => reserved,
            Some(_) => match self
                .edges
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    let members = reserved.min(self.limits.edges.saturating_sub(count) / edges);
                    (members != 0).then(|| count + members * edges)
                }) {
                Ok(previous) => reserved.min((self.limits.edges - previous) / edges),
                Err(_) => {
                    self.records.fetch_sub(reserved, Ordering::AcqRel);
                    return Err(SWSpawnError::Full);
                }
            },
        };
        self.records.fetch_sub(reserved - members, Ordering::AcqRel);
        Ok((0..members)
            .map(|_| AccountingLease {
                accounting: Arc::clone(self),
                edges,
            })
            .collect())
    }
}

impl OwnedScheduler {
    pub(crate) fn submit_batch<'a, P: Send + 'static, T: Send + 'static>(
        self: &Arc<Self>,
        control: &Arc<RuntimeControl>,
        class: SWExecutionClass,
        options: SWBatchSpawnOptions<'a>,
        mut operations: Vec<P>,
        run: fn(P) -> T,
        application_failed: fn(&T) -> bool,
    ) -> SWBatchSpawnResult<'a, T, P> {
        let reject = |reason, accepted, remaining| SWBatchSpawnRejected {
            reason,
            accepted,
            remaining,
            options,
        };
        // Match single-job entry: forbidden native notification callbacks do
        // not touch admission ledgers or mask InvalidContext with capacity.
        if crate::notification::invocation_active() {
            return Err(reject(SWSpawnError::InvalidContext, Vec::new(), operations));
        }
        // Structural errors cannot admit a prefix. Sealing remains dynamic and
        // is finally arbitrated by add_many at each portion's acceptance point.
        if let Some(group) = options.group
            && (group.runtime != control.identity()
                || group.inner.class != class
                || !Weak::ptr_eq(&group.scheduler, &Arc::downgrade(self))
                || options
                    .prerequisites
                    .iter()
                    .any(|input| input.same_signal(&group.completion())))
        {
            return Err(reject(SWSpawnError::InvalidGroup, Vec::new(), operations));
        }
        if options.prerequisites.len() > self.limits.edges {
            return Err(reject(SWSpawnError::TooLarge, Vec::new(), operations));
        }
        if operations.len() == 1 {
            let notification = self.wake.notification_scope();
            let operation = operations.pop().expect("singleton input exists");
            let result = self.submit_payload_accounted(
                SubmitRequest {
                    control,
                    class,
                    group: options.group,
                    options: options.spawn,
                    prerequisites: options.prerequisites,
                    policy: options.dependency_policy,
                    allow_inline: false,
                },
                operation,
                run,
                application_failed,
                &mut None,
                SubmitExtras {
                    batch_member: true,
                    ..SubmitExtras::default()
                },
            );
            let result = match result {
                Ok(receipt) => Ok(vec![receipt]),
                Err(rejected) => {
                    operations.push(rejected.operation);
                    Err(reject(rejected.reason, Vec::new(), operations))
                }
            };
            drop(notification);
            return result;
        }
        let mut remaining: VecDeque<P> = operations.into();
        let mut accepted = Vec::with_capacity(remaining.len());
        while !remaining.is_empty() {
            // Native adapter notifications coalesce on this submitting thread,
            // then flush before the next portion. Other threads' completion
            // publications retain their independent notification scopes.
            let notification = self.wake.notification_scope();
            let maximum = QUANTUM.min(remaining.len());
            let mut admissions = match control.admit_owned_many(false, maximum) {
                Ok(admissions) => admissions,
                Err(error) => {
                    drop(notification);
                    return Err(reject(
                        match error {
                            crate::execution::SWExecutionError::Closed => SWSpawnError::Closed,
                            crate::execution::SWExecutionError::InvalidContext => {
                                SWSpawnError::InvalidContext
                            }
                            crate::execution::SWExecutionError::ClassDisabled(class) => {
                                SWSpawnError::ClassDisabled(class)
                            }
                        },
                        accepted,
                        remaining.into(),
                    ));
                }
            };
            if options.group.is_some_and(|group| !group.inner.is_open()) {
                drop(admissions);
                drop(notification);
                return Err(reject(
                    SWSpawnError::InvalidGroup,
                    accepted,
                    remaining.into(),
                ));
            }
            let mut capacities = if let Some(pool) = &self.capacity {
                match pool.try_reserve_ordinary_many(
                    SWCost::new(1, options.prerequisites.len(), 0, 0),
                    maximum,
                ) {
                    Ok(capacities) => capacities.into_iter().map(Some).collect::<Vec<_>>(),
                    Err(reason) => {
                        drop(admissions);
                        drop(notification);
                        return Err(reject(
                            map_reservation_error(reason),
                            accepted,
                            remaining.into(),
                        ));
                    }
                }
            } else {
                (0..maximum).map(|_| None).collect()
            };
            let mut charges = match self
                .accounting
                .acquire_many(options.prerequisites.len(), capacities.len())
            {
                Ok(charges) => charges,
                Err(reason) => {
                    drop(capacities);
                    drop(admissions);
                    drop(notification);
                    return Err(reject(reason, accepted, remaining.into()));
                }
            };
            capacities.truncate(charges.len());
            admissions.truncate(charges.len());
            let count = charges.len();
            let ids: Vec<_> = (0..count)
                .map(|_| Self::allocate_id(&self.next_id))
                .collect();
            let jobs = self.jobs.prepare_many(&ids, Arc::downgrade(self));
            let signals = self.signals.acquire_many(count);
            let range_release = count >= 2 && options.prerequisites.len() == 1;
            let subscriptions = self
                .subscriptions
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .acquire_many(
                    count,
                    if range_release {
                        0
                    } else {
                        options.prerequisites.len()
                    },
                );
            let group = options.group.map(|group| Arc::clone(&group.inner));
            let mut prepared = Vec::with_capacity(count);
            for ((((job, signal), subscriptions), admission), (capacity, charge)) in jobs
                .into_iter()
                .zip(signals)
                .zip(subscriptions)
                .zip(admissions)
                .zip(capacities.into_iter().zip(charges.drain(..)))
            {
                let (task, sink) = SWTask::pending_with_signal(signal);
                task.set_producer(control.identity(), job.id(), Arc::downgrade(self));
                if let Some(domain) = control.notification_domain() {
                    task.set_notification_source(domain);
                }
                let weak = Arc::downgrade(self);
                let id = job.id();
                let producer = SWProducerControl::new(Box::new(move || {
                    if let Some(scheduler) = weak.upgrade() {
                        scheduler.suppress_id(class, id, SWTaskStatus::Cancelled);
                    }
                }));
                prepared.push(Prepared {
                    job,
                    envelope: Box::new(PreparedEnvelope {
                        payload: remaining.pop_front().expect("selected input exists"),
                        run,
                        application_failed,
                        sink,
                    }),
                    record: Record {
                        completion: group.as_ref().map(|_| task.completion()),
                        group: group.clone(),
                        pending: options.prerequisites.len(),
                        failed: false,
                        policy: options.dependency_policy,
                        stage: Stage::Waiting,
                        eligibility: options.spawn.eligibility,
                        subscriptions,
                        prerequisite_range: None,
                        attaching: true,
                        deferred_finish: None,
                        admission,
                        work_set: None,
                        capacity,
                        resource: false,
                        runnable_reserved: options.prerequisites.is_empty(),
                        selection: None,
                        charge,
                    },
                    receipt: (task, producer),
                });
            }
            let mut portion = Vec::with_capacity(count);
            let failure = {
                let mut state = self.class_lock(class);
                let selected = if options.prerequisites.is_empty() {
                    count.min(
                        self.limits
                            .runnable_for(class)
                            .saturating_sub(state.ready.runnable + state.attaching_runnable),
                    )
                } else {
                    count
                };
                let failure = if self.abandoned.load(Ordering::Acquire) {
                    Some(SWSpawnError::Closed)
                } else if selected == 0 {
                    Some(SWSpawnError::Full)
                } else if group
                    .as_ref()
                    .is_some_and(|group| !group.add_many(selected))
                {
                    Some(SWSpawnError::InvalidGroup)
                } else {
                    None
                };
                if failure.is_none() {
                    if options.prerequisites.is_empty() {
                        state.attaching_runnable += selected;
                    }
                    // After membership commits no ordinary rejection remains.
                    // Controls and envelopes were allocated while still typed.
                    for item in prepared.drain(..selected) {
                        let Prepared {
                            mut job,
                            envelope,
                            record,
                            receipt,
                        } = item;
                        let envelope: Envelope = envelope;
                        job.initialize(
                            class,
                            group.as_ref().map(|group| group.id),
                            envelope,
                            record,
                        );
                        state.records.insert(job.id(), job.clone());
                        portion.push(job);
                        accepted.push(receipt);
                    }
                }
                failure
            };
            // Recover every precommit payload before dropping provisional result
            // sinks and charges. None of these drops occurs under a class guard.
            for item in prepared.into_iter().rev() {
                remaining.push_front(item.recover());
            }
            if let Some(reason) = failure {
                drop(notification);
                return Err(reject(reason, accepted, remaining.into()));
            }
            if let Some(group) = &group {
                group.register_members(portion.iter().map(JobHandle::downgrade).collect());
            }
            if self.demand_enabled {
                let parents: Vec<_> = options
                    .prerequisites
                    .iter()
                    .filter_map(|completion| completion.producer_identity())
                    .filter_map(|(runtime, id)| (runtime == control.identity()).then_some(id))
                    .collect();
                let mut demand = self.domain_lock(&self.demand, 3);
                for job in &portion {
                    demand
                        .graph
                        .register(job.id(), None, &parents, None)
                        .expect("ordinary demand registration");
                }
                self.demand_pending.store(true, Ordering::Release);
            }
            let range = range_release
                .then(|| super::prerequisite_range::PrerequisiteRange::new(self, &portion));
            if let Some(range) = &range {
                for (slot, job) in portion.iter().enumerate() {
                    let token = range.member(slot);
                    let unused = {
                        let mut guard = job.record_lock();
                        let record = guard.as_mut().expect("attachment retains accepted record");
                        if record.stage == Stage::Finalizing {
                            Some(token)
                        } else {
                            record.prerequisite_range = Some(token);
                            None
                        }
                    };
                    drop(unused);
                }
            }
            for job in &portion {
                #[cfg(test)]
                {
                    let hook = self
                        .attachment_hook
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .clone();
                    if let Some(hook) = hook {
                        hook(job.clone());
                    }
                }
                if range.is_some() {
                    continue;
                }
                for prerequisite in options.prerequisites {
                    let weak = Arc::downgrade(self);
                    let target = job.downgrade();
                    let subscription = prerequisite.subscribe_cancelable(Box::new(move |status| {
                        if let (Some(scheduler), Some(job)) = (weak.upgrade(), target.upgrade()) {
                            enqueue_activation(Activation::Prerequisite(scheduler, job, status));
                        }
                    }));
                    job.record_lock()
                        .as_mut()
                        .expect("attachment retains accepted record")
                        .subscriptions
                        .push(subscription);
                }
            }
            if let Some(range) = &range {
                range.subscribe(&options.prerequisites[0]);
            }
            self.publish_portion(class, &portion);
            self.dispatch_class(class);
            drop(portion);
            drop(notification);
            #[cfg(test)]
            {
                let hook = self
                    .batch_portion_hook
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                if let Some(hook) = hook {
                    hook(accepted.len());
                }
            }
        }
        Ok(accepted)
    }

    pub(super) fn publish_portion(self: &Arc<Self>, class: SWExecutionClass, jobs: &[JobHandle]) {
        let mut finishes = Vec::with_capacity(jobs.len());
        let mut suppressed = Vec::with_capacity(jobs.len());
        let mut ready_group = None;
        {
            let mut state = self.class_lock(class);
            for job in jobs {
                let mut guard = job.record_lock();
                let record = guard.as_mut().expect("attachment retains accepted record");
                record.attaching = false;
                if record.runnable_reserved {
                    state.attaching_runnable -= 1;
                    record.runnable_reserved = false;
                }
                if let Some(finish) = record.deferred_finish.take() {
                    finishes.push((job, finish));
                    continue;
                }
                if record.stage != Stage::Waiting || record.pending != 0 {
                    continue;
                }
                let status = if self.abandoned.load(Ordering::Acquire) {
                    Some(SWTaskStatus::Abandoned)
                } else if record.failed && record.policy == SWDependencyPolicy::SuccessOnly {
                    Some(SWTaskStatus::PrerequisiteFailed)
                } else {
                    None
                };
                if let Some(status) = status {
                    suppressed.push((job, status));
                } else if state.ready.runnable + state.attaching_runnable
                    < self.limits.runnable_for(class)
                {
                    record.stage = Stage::Ready;
                    self.push_ready(&mut state, job, record);
                    ready_group = record.group.clone();
                } else {
                    record.stage = Stage::DeferredReady;
                    state.deferred.push_back(job.id());
                }
            }
        }
        if let Some(group) = ready_group {
            group.notify_ready();
        }
        for (job, finish) in finishes {
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.finish_record(job, finish)
            })) {
                crate::cleanup::discard_panic(payload);
            }
        }
        for (job, status) in suppressed {
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.suppress(job, status)
            })) {
                crate::cleanup::discard_panic(payload);
            }
        }
    }
}
