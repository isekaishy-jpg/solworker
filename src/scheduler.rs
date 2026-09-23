//! Coordination of accepted owned work.
//!
//! Own the shared admission, record identity, claiming, and completion transitions
//! that must commit together. Child modules define policies within those
//! transactions rather than independent schedulers or unrelated lock domains.
//! User code, destructors, provider hooks, and backend calls run outside locks.

mod admission;
mod demand;
mod ready;
mod work_set;

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use crate::execution::ContextGuard;
use crate::execution::context;
use crate::execution::group::{GroupInner, SWGroup};
use crate::runtime::config::SWExecutionClass;
use crate::runtime::{OwnedAdmission, RuntimeControl};
use crate::task::{
    CompletionSink, SWCompletion, SWOutcome, SWProducerControl, SWTask, SWTaskStatus, Subscription,
};

pub use admission::{
    SWCallerEligibility, SWDependencyPolicy, SWOwnedConfigError, SWOwnedLimits, SWSpawnError,
    SWSpawnOptions, SWSpawnRejected, SWSpawnResult,
};
use ready::ReadyQueues;

type Finish = Box<dyn FnOnce() + Send>;
type Envelope = Box<dyn FnOnce(Decision) -> Finish + Send>;

enum Activation {
    Prerequisite(Arc<OwnedScheduler>, u64, SWTaskStatus),
    Finalize(Arc<OwnedScheduler>, u64),
}

struct ActivationQueue {
    draining: bool,
    pending: VecDeque<Activation>,
}

thread_local! {
    static ACTIVATIONS: RefCell<ActivationQueue> = const { RefCell::new(ActivationQueue {
        draining: false,
        pending: VecDeque::new(),
    }) };
}

struct DrainGuard;

impl Drop for DrainGuard {
    fn drop(&mut self) {
        ACTIVATIONS.with(|queue| queue.borrow_mut().draining = false);
    }
}

fn enqueue_activation(activation: Activation) {
    let start = ACTIVATIONS.with(|queue| {
        let mut queue = queue.borrow_mut();
        queue.pending.push_back(activation);
        if queue.draining {
            false
        } else {
            queue.draining = true;
            true
        }
    });
    if !start {
        return;
    }
    let _guard = DrainGuard;
    loop {
        let next = ACTIVATIONS.with(|queue| queue.borrow_mut().pending.pop_front());
        match next {
            Some(Activation::Prerequisite(scheduler, id, status)) => {
                scheduler.activate_prerequisite(id, status)
            }
            Some(Activation::Finalize(scheduler, id)) => scheduler.finalize_record(id),
            None => break,
        }
    }
}

#[derive(Clone, Copy)]
enum Decision {
    Run,
    Suppress(SWTaskStatus),
}

struct Job {
    id: u64,
    class: SWExecutionClass,
    scheduler: Weak<OwnedScheduler>,
    envelope: Mutex<Option<Envelope>>,
}

impl Job {
    fn take_envelope(&self) -> Option<Envelope> {
        self.envelope
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }

    fn run(self: &Arc<Self>) {
        if let Some(scheduler) = self.scheduler.upgrade() {
            scheduler.run_job(self);
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Stage {
    Waiting,
    DeferredReady,
    Ready,
    Handed,
    Running,
    Finalizing,
}

struct Record {
    job: Arc<Job>,
    class: SWExecutionClass,
    group: Option<Arc<GroupInner>>,
    pending: usize,
    failed: bool,
    policy: SWDependencyPolicy,
    stage: Stage,
    eligibility: SWCallerEligibility,
    edges: usize,
    subscriptions: Vec<Subscription>,
    attaching: bool,
    deferred_finish: Option<Finish>,
    admission: OwnedAdmission,
}

pub(crate) struct SubmitRequest<'a> {
    pub(crate) control: &'a Arc<RuntimeControl>,
    pub(crate) class: SWExecutionClass,
    pub(crate) group: Option<&'a SWGroup>,
    pub(crate) options: SWSpawnOptions,
    pub(crate) prerequisites: &'a [SWCompletion],
    pub(crate) policy: SWDependencyPolicy,
    pub(crate) allow_inline: bool,
}

struct State {
    records: HashMap<u64, Record>,
    next_id: u64,
    next_group: u64,
    edges: usize,
    ready: ReadyQueues,
    deferred: [VecDeque<u64>; 3],
    abandoned: bool,
}

/// One control domain for admission, dependencies, claims and terminal cleanup.
pub(crate) struct OwnedScheduler {
    control: Weak<RuntimeControl>,
    limits: SWOwnedLimits,
    state: Mutex<State>,
}

impl OwnedScheduler {
    pub(crate) fn new(control: Weak<RuntimeControl>, limits: SWOwnedLimits) -> Arc<Self> {
        Arc::new(Self {
            control,
            limits,
            state: Mutex::new(State {
                records: HashMap::new(),
                next_id: 1,
                next_group: 1,
                edges: 0,
                ready: ReadyQueues::new(),
                deferred: std::array::from_fn(|_| VecDeque::new()),
                abandoned: false,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn group(
        self: &Arc<Self>,
        class: SWExecutionClass,
    ) -> Result<SWGroup, SWSpawnError> {
        let control = self.control.upgrade().ok_or(SWSpawnError::Closed)?;
        let admission = control.admit_owned().map_err(|error| match error {
            crate::execution::SWExecutionError::Closed => SWSpawnError::Closed,
            crate::execution::SWExecutionError::InvalidContext => SWSpawnError::InvalidContext,
        })?;
        let mut state = self.lock();
        if state.abandoned {
            return Err(SWSpawnError::Closed);
        }
        let id = state.next_group;
        state.next_group = id.checked_add(1).expect("group identity exhausted");
        drop(state);
        drop(admission);
        Ok(SWGroup::new(
            Arc::new(GroupInner::new(id, class)),
            Arc::downgrade(self),
            control.identity(),
        ))
    }

    pub(crate) fn submit_payload<P, T>(
        self: &Arc<Self>,
        request: SubmitRequest<'_>,
        payload: P,
        run: fn(P) -> T,
        application_failed: fn(&T) -> bool,
    ) -> SWSpawnResult<T, P>
    where
        P: Send + 'static,
        T: Send + 'static,
    {
        let SubmitRequest {
            control,
            class,
            group,
            options,
            prerequisites,
            policy,
            allow_inline,
        } = request;
        let reject = |reason, operation| SWSpawnRejected {
            reason,
            operation,
            options,
        };
        let admission = match control.admit_owned() {
            Ok(admission) => admission,
            Err(crate::execution::SWExecutionError::Closed) => {
                return Err(reject(SWSpawnError::Closed, payload));
            }
            Err(crate::execution::SWExecutionError::InvalidContext) => {
                return Err(reject(SWSpawnError::InvalidContext, payload));
            }
        };
        let mut state = self.lock();
        if state.abandoned {
            return Err(reject(SWSpawnError::Closed, payload));
        }
        if prerequisites.len() > self.limits.edges {
            return Err(reject(SWSpawnError::TooLarge, payload));
        }
        if state.records.len() >= self.limits.records
            || state
                .edges
                .checked_add(prerequisites.len())
                .is_none_or(|edges| edges > self.limits.edges)
        {
            return Err(reject(SWSpawnError::Full, payload));
        }
        let group_inner = if let Some(group) = group {
            if group.runtime != control.identity()
                || group.inner.class != class
                || !Weak::ptr_eq(&group.scheduler, &Arc::downgrade(self))
            {
                return Err(reject(SWSpawnError::InvalidGroup, payload));
            }
            if !group.inner.add() {
                return Err(reject(SWSpawnError::InvalidGroup, payload));
            }
            Some(Arc::clone(&group.inner))
        } else {
            None
        };
        let saturated = prerequisites.is_empty()
            && state.ready.runnable[class.index()] >= self.limits.runnable_for(class);
        let inline =
            saturated && allow_inline && options.eligibility == SWCallerEligibility::CallerEligible;
        if inline
            && context::current().is_some_and(|current| {
                current.runtime != control.identity() || current.class != class
            })
        {
            if let Some(group) = &group_inner {
                group.finish();
            }
            return Err(reject(SWSpawnError::InvalidContext, payload));
        }
        if saturated && !inline {
            if let Some(group) = &group_inner {
                group.finish();
            }
            return Err(reject(SWSpawnError::Full, payload));
        }
        let id = state.next_id;
        state.next_id = id.checked_add(1).expect("owned record identity exhausted");
        let (task, sink) = SWTask::pending_pair();
        let envelope = make_envelope(payload, run, application_failed, sink);
        let job = Arc::new(Job {
            id,
            class,
            scheduler: Arc::downgrade(self),
            envelope: Mutex::new(Some(envelope)),
        });
        let stage = if prerequisites.is_empty() {
            Stage::Ready
        } else {
            Stage::Waiting
        };
        state.edges += prerequisites.len();
        state.records.insert(
            id,
            Record {
                job: Arc::clone(&job),
                class,
                group: group_inner,
                pending: prerequisites.len(),
                failed: false,
                policy,
                stage,
                eligibility: options.eligibility,
                edges: prerequisites.len(),
                subscriptions: Vec::with_capacity(prerequisites.len()),
                attaching: !prerequisites.is_empty(),
                deferred_finish: None,
                admission,
            },
        );
        if stage == Stage::Ready {
            state.ready.push(class, id);
        }
        drop(state);
        let weak = Arc::downgrade(self);
        let producer = SWProducerControl::new(Box::new(move || {
            if let Some(scheduler) = weak.upgrade() {
                scheduler.suppress(id, SWTaskStatus::Cancelled);
            }
        }));
        for prerequisite in prerequisites {
            let weak = Arc::downgrade(self);
            let subscription = prerequisite.subscribe_cancelable(Box::new(move |status| {
                if let Some(scheduler) = weak.upgrade() {
                    enqueue_activation(Activation::Prerequisite(scheduler, id, status));
                }
            }));
            let mut state = self.lock();
            if let Some(record) = state.records.get_mut(&id) {
                record.subscriptions.push(subscription);
            } else {
                drop(state);
                drop(subscription);
            }
        }
        let deferred_finish = {
            let mut state = self.lock();
            state.records.get_mut(&id).and_then(|record| {
                record.attaching = false;
                record.deferred_finish.take()
            })
        };
        if let Some(finish) = deferred_finish {
            self.finish_record(id, finish);
        }
        if inline {
            self.claim_and_run(id, true);
        } else {
            self.dispatch();
        }
        Ok((task, producer))
    }

    fn activate_prerequisite(self: &Arc<Self>, id: u64, status: SWTaskStatus) {
        let mut suppress = false;
        let mut group = None;
        {
            let mut state = self.lock();
            let activation = {
                let Some(record) = state.records.get_mut(&id) else {
                    return;
                };
                if record.stage != Stage::Waiting || record.pending == 0 {
                    return;
                }
                record.pending -= 1;
                record.failed |= !status.is_success();
                if record.pending != 0 {
                    None
                } else if record.failed && record.policy == SWDependencyPolicy::SuccessOnly {
                    suppress = true;
                    None
                } else {
                    Some((record.class, record.group.clone()))
                }
            };
            if let Some((class, ready_group)) = activation {
                let has_slot =
                    state.ready.runnable[class.index()] < self.limits.runnable_for(class);
                let record = state
                    .records
                    .get_mut(&id)
                    .expect("activation retains record");
                if has_slot {
                    record.stage = Stage::Ready;
                    state.ready.push(class, id);
                    group = ready_group;
                } else {
                    record.stage = Stage::DeferredReady;
                    state.deferred[class.index()].push_back(id);
                }
            }
        }
        if let Some(group) = group {
            group.notify_ready();
        }
        if suppress {
            self.suppress(id, SWTaskStatus::PrerequisiteFailed);
        }
        self.dispatch();
    }

    fn suppress(self: &Arc<Self>, id: u64, status: SWTaskStatus) {
        let job = {
            let mut state = self.lock();
            let Some(record) = state.records.get_mut(&id) else {
                return;
            };
            let stage = record.stage;
            let decrement = match stage {
                Stage::Waiting | Stage::DeferredReady => false,
                Stage::Ready | Stage::Handed => true,
                Stage::Running | Stage::Finalizing => return,
            };
            record.stage = Stage::Finalizing;
            let class = record.class;
            let job = Arc::clone(&record.job);
            match stage {
                Stage::Ready => state.ready.remove(class, id),
                Stage::DeferredReady => {
                    state.deferred[class.index()].retain(|queued| *queued != id)
                }
                _ => {}
            }
            if decrement {
                state.ready.runnable[class.index()] -= 1;
                self.promote_locked(&mut state, class);
            }
            job
        };
        self.settle(job, Decision::Suppress(status));
    }

    fn claim_and_run(self: &Arc<Self>, id: u64, caller: bool) -> bool {
        let Some(control) = self.control.upgrade() else {
            return false;
        };
        let class = {
            let state = self.lock();
            let Some(record) = state.records.get(&id) else {
                return false;
            };
            record.class
        };
        let Ok(_lease) = control.acquire_owned(class) else {
            return false;
        };
        let (job, class, group_id) = {
            let mut state = self.lock();
            // A lease obtained just before abandonment is not itself a claim.
            // Serialize the actual claim with the scheduler's abandonment cut.
            if state.abandoned {
                return false;
            }
            let Some(record) = state.records.get_mut(&id) else {
                return false;
            };
            if !matches!(record.stage, Stage::Ready | Stage::Handed) {
                return false;
            }
            if caller && record.eligibility != SWCallerEligibility::CallerEligible {
                return false;
            }
            let was_ready = record.stage == Stage::Ready;
            record.stage = Stage::Running;
            let class = record.class;
            let job = Arc::clone(&record.job);
            let group_id = record.group.as_ref().map(|group| group.id);
            // Dispatch already unlinked handed-off jobs. Only an inline or
            // helper claim of a still-ready job needs to scan/remove it here.
            if was_ready {
                state.ready.remove(class, id);
            }
            state.ready.runnable[class.index()] -= 1;
            self.promote_locked(&mut state, class);
            (job, class, group_id)
        };
        let _context = ContextGuard::enter_owned(control.identity(), class, group_id);
        self.settle(job, Decision::Run);
        true
    }

    fn run_job(self: &Arc<Self>, job: &Arc<Job>) {
        // The claim obtains a temporary backend lease before running. Queued
        // records retain only admission credit and never own a backend Arc.
        self.claim_and_run(job.id, false);
        self.release_handoff(job.class);
    }

    fn release_handoff(self: &Arc<Self>, class: SWExecutionClass) {
        self.decrement_handoff(class);
        self.dispatch();
    }

    fn decrement_handoff(&self, class: SWExecutionClass) {
        let mut state = self.lock();
        state.ready.handed_off[class.index()] -= 1;
    }

    fn promote_locked(&self, state: &mut State, class: SWExecutionClass) {
        while state.ready.runnable[class.index()] < self.limits.runnable_for(class) {
            let Some(id) = state.deferred[class.index()].pop_front() else {
                break;
            };
            let Some(record) = state.records.get_mut(&id) else {
                continue;
            };
            if record.stage != Stage::DeferredReady {
                continue;
            }
            record.stage = Stage::Ready;
            if let Some(group) = &record.group {
                group.notify_ready();
            }
            state.ready.push(class, id);
        }
    }

    fn cleanup_context(&self, id: u64) -> Option<ContextGuard> {
        let control = self.control.upgrade()?;
        let (class, group) = {
            let state = self.lock();
            let record = state.records.get(&id)?;
            (record.class, record.group.as_ref().map(|group| group.id))
        };
        match context::current() {
            None => Some(ContextGuard::enter_owned(control.identity(), class, group)),
            Some(current)
                if current.runtime == control.identity()
                    && current.class == class
                    && group.is_some() =>
            {
                Some(ContextGuard::enter_owned(control.identity(), class, group))
            }
            // Preserve a different execution route: nested scoped calls must
            // still reject cross-pool entry before touching backend TLS.
            Some(_) => None,
        }
    }

    fn settle(self: &Arc<Self>, job: Arc<Job>, decision: Decision) {
        let Some(envelope) = job.take_envelope() else {
            return;
        };
        // Cancellation and abandonment may be initiated by a host caller.
        // Destructors and terminal callbacks remain participating CPU work,
        // so a shutdown requested from either must not join this runtime.
        let _context = self.cleanup_context(job.id);
        {
            let mut state = self.lock();
            if let Some(record) = state.records.get_mut(&job.id) {
                record.stage = Stage::Finalizing;
            }
        }
        // Capture cleanup executes outside the control lock. The admission
        // token stays live through result publication and group completion.
        let finish = match catch_unwind(AssertUnwindSafe(|| envelope(decision))) {
            Ok(finish) => finish,
            Err(payload) => {
                let _ = catch_unwind(AssertUnwindSafe(|| drop(payload)));
                Box::new(|| {})
            }
        };
        self.finish_record(job.id, finish);
    }

    fn finish_record(self: &Arc<Self>, id: u64, finish: Finish) {
        let _context = self.cleanup_context(id);
        let subscriptions = {
            let mut state = self.lock();
            let Some(record) = state.records.get_mut(&id) else {
                return;
            };
            if record.attaching {
                record.deferred_finish = Some(finish);
                return;
            }
            std::mem::take(&mut record.subscriptions)
        };
        drop(subscriptions);
        if let Err(payload) = catch_unwind(AssertUnwindSafe(finish)) {
            let _ = catch_unwind(AssertUnwindSafe(|| drop(payload)));
        }
        // A terminal publication can activate another edge, which can itself
        // publish a terminal outcome. Drain those callbacks without recursing,
        // and retain this record's credits until its callbacks have run.
        enqueue_activation(Activation::Finalize(Arc::clone(self), id));
    }

    fn finalize_record(self: &Arc<Self>, id: u64) {
        let admission = {
            let mut state = self.lock();
            if let Some(group) = state
                .records
                .get(&id)
                .and_then(|record| record.group.as_ref())
            {
                // Group completion is bookkeeping only. Commit it before
                // releasing record/edge credit under this admission lock.
                group.finish();
            }
            let Some(record) = state.records.remove(&id) else {
                return;
            };
            state.edges -= record.edges;
            record.admission
        };
        drop(admission);
        self.dispatch();
    }

    pub(crate) fn help_group(self: &Arc<Self>, group_id: u64) -> bool {
        let id = {
            let state = self.lock();
            state
                .records
                .iter()
                .filter(|(_, record)| {
                    record
                        .group
                        .as_ref()
                        .is_some_and(|group| group.id == group_id)
                        && matches!(record.stage, Stage::Ready | Stage::Handed)
                        && record.eligibility == SWCallerEligibility::CallerEligible
                })
                .map(|(id, _)| *id)
                .min()
        };
        id.is_some_and(|id| self.claim_and_run(id, true))
    }

    pub(crate) fn abandon(self: &Arc<Self>) {
        let ids = {
            let mut state = self.lock();
            state.abandoned = true;
            state
                .records
                .iter()
                .filter(|(_, record)| record.stage != Stage::Running)
                .map(|(id, _)| *id)
                .collect::<Vec<_>>()
        };
        for id in ids {
            self.suppress(id, SWTaskStatus::Abandoned);
        }
    }

    fn dispatch(self: &Arc<Self>) {
        let Some(control) = self.control.upgrade() else {
            return;
        };
        for class in SWExecutionClass::ALL {
            loop {
                let job = {
                    let mut state = self.lock();
                    if state.abandoned
                        || state.ready.handed_off[class.index()] >= self.limits.handoff_for(class)
                    {
                        break;
                    }
                    let Some(id) = state.ready.pop(class) else {
                        break;
                    };
                    let Some(record) = state.records.get_mut(&id) else {
                        continue;
                    };
                    if record.stage != Stage::Ready {
                        continue;
                    }
                    record.stage = Stage::Handed;
                    let job = Arc::clone(&record.job);
                    state.ready.handed_off[class.index()] += 1;
                    job
                };
                let Ok(lease) = control.acquire_owned(class) else {
                    self.decrement_handoff(class);
                    self.abandon();
                    break;
                };
                let rejected_job = Arc::clone(&job);
                let offered = lease.pool().try_spawn_owned(move || job.run());
                if let Err(wrapper) = offered {
                    // Checked handoff returned the intact wrapper after stop.
                    drop(wrapper);
                    self.decrement_handoff(class);
                    self.abandon();
                    drop(rejected_job);
                    break;
                }
            }
        }
    }
}

pub(crate) fn call_once<F, T>(operation: F) -> T
where
    F: FnOnce() -> T,
{
    operation()
}

pub(crate) fn never_fail<T>(_: &T) -> bool {
    false
}

pub(crate) fn result_is_err<T, E>(result: &Result<T, E>) -> bool {
    result.is_err()
}

fn make_envelope<P, T>(
    payload: P,
    run: fn(P) -> T,
    application_failed: fn(&T) -> bool,
    sink: CompletionSink<T>,
) -> Envelope
where
    P: Send + 'static,
    T: Send + 'static,
{
    Box::new(move |decision| {
        let (outcome, failed) = match decision {
            Decision::Run => match catch_unwind(AssertUnwindSafe(|| run(payload))) {
                Ok(value) => {
                    let failed = application_failed(&value);
                    (SWOutcome::Success(value), failed)
                }
                Err(panic) => {
                    drop(panic);
                    (SWOutcome::Panicked, false)
                }
            },
            Decision::Suppress(status) => {
                drop(payload);
                (
                    match status {
                        SWTaskStatus::Cancelled => SWOutcome::Cancelled,
                        SWTaskStatus::PrerequisiteFailed | SWTaskStatus::ApplicationFailed => {
                            SWOutcome::PrerequisiteFailed
                        }
                        SWTaskStatus::Panicked => SWOutcome::Panicked,
                        SWTaskStatus::Abandoned | SWTaskStatus::Succeeded => SWOutcome::Abandoned,
                    },
                    false,
                )
            }
        };
        Box::new(move || sink.finish(outcome, failed))
    })
}
