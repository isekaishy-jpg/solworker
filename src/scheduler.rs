//! Coordination of accepted owned work.
//!
//! Own the shared admission, record identity, claiming, and completion transitions
//! that must commit together. Child modules define policies within those
//! transactions rather than independent schedulers or unrelated lock domains.
//! User code, destructors, provider hooks, and backend calls run outside locks.

mod admission;
mod demand;
mod ready;
pub(crate) mod reservation;
pub(crate) mod work_set;

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use crate::execution::ContextGuard;
use crate::execution::context;
use crate::execution::group::{GroupInner, SWGroup};
use crate::external::producer::{
    ExternalCore, SWExternalOptions, SWExternalRejected, SWExternalResult, SWProducer,
};
use crate::runtime::config::SWExecutionClass;
use crate::runtime::{ExternalAdmission, OwnedAdmission, RuntimeControl};
use crate::task::{
    CompletionSink, SWCompletion, SWOutcome, SWProducerControl, SWTask, SWTaskStatus, Subscription,
};

pub use admission::{
    SWCallerEligibility, SWDependencyPolicy, SWOwnedConfigError, SWOwnedLimits, SWSpawnError,
    SWSpawnOptions, SWSpawnRejected, SWSpawnResult,
};
use demand::DemandState;
pub use demand::{SWDemand, SWDemandError, SWDemandSnapshot, SWPriority};
use ready::ReadyQueues;
use reservation::SWReservationPool;
pub use reservation::{
    SWByteLease, SWCapacityUsage, SWCost, SWLimitError, SWLimits, SWReservation, SWReservationError,
};
use work_set::WorkSetLease;
pub use work_set::{SWDiscoveryError, SWDiscoveryPermit, SWWorkSet, SWWorkSetProgress};

#[cfg(test)]
#[path = "../tests/unit/demand.rs"]
mod demand_tests;

#[derive(Default)]
pub(crate) struct SubmitExtras {
    work_set: Option<WorkSetLease>,
    capacity: Option<SWReservation>,
    priority: Option<SWPriority>,
    reserved: bool,
}

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
    work_set: Option<WorkSetLease>,
    capacity: Option<SWReservation>,
    resource: bool,
}

struct ExternalRecord {
    settling: bool,
    settle: Arc<dyn Fn(SWTaskStatus) + Send + Sync>,
    admission: ExternalAdmission,
    work_set: Option<WorkSetLease>,
    capacity: Option<SWReservation>,
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
    external: HashMap<u64, ExternalRecord>,
    next_id: u64,
    next_group: u64,
    edges: usize,
    ready: ReadyQueues,
    deferred: [VecDeque<u64>; 3],
    abandoned: bool,
    demand: DemandState,
}

/// One control domain for admission, dependencies, claims and terminal cleanup.
pub(crate) struct OwnedScheduler {
    control: Weak<RuntimeControl>,
    limits: SWOwnedLimits,
    state: Mutex<State>,
    pub(crate) capacity: Option<SWReservationPool>,
}

impl OwnedScheduler {
    pub(crate) fn admit_external<'a, T: Send + 'static>(
        self: &Arc<Self>,
        control: &Arc<RuntimeControl>,
        mut options: SWExternalOptions<'a>,
    ) -> SWExternalResult<'a, T> {
        macro_rules! reject {
            ($reason:expr) => {
                return Err(SWExternalRejected {
                    reason: $reason,
                    options,
                })
            };
        }
        let admission = match control.admit_external() {
            Ok(admission) => admission,
            Err(crate::execution::SWExecutionError::Closed) => reject!(SWSpawnError::Closed),
            Err(crate::execution::SWExecutionError::InvalidContext) => {
                reject!(SWSpawnError::InvalidContext)
            }
        };
        if options.work_set.is_some() && options.discovery.is_some() {
            reject!(SWSpawnError::InvalidContext);
        }
        let work_set = match (options.work_set, options.discovery) {
            (Some(set), _) => Some(set.try_root_lease(control.identity())),
            (_, Some(permit)) => Some(permit.try_child_lease(control.identity())),
            _ => None,
        }
        .transpose();
        let work_set = match work_set {
            Ok(lease) => lease,
            Err(SWDiscoveryError::Full) => reject!(SWSpawnError::Full),
            Err(_) => reject!(SWSpawnError::Closed),
        };
        let unaccounted_delivery = options
            .delivery
            .as_ref()
            .is_some_and(|ticket| !ticket.is_accounted());
        let cost = SWCost::new(
            options.cost.records.max(1),
            options.cost.edges,
            options
                .cost
                .deliveries
                .max(usize::from(options.delivery.is_some()))
                - usize::from(
                    options
                        .delivery
                        .as_ref()
                        .is_some_and(|ticket| ticket.is_accounted()),
                ),
            options.cost.bytes,
        );
        if options.retained_bytes > cost.bytes {
            reject!(SWSpawnError::TooLarge);
        }
        let capacity = if let Some(reservation) = options.reservation {
            if reservation.runtime_identity() != control.identity() {
                reject!(SWSpawnError::InvalidReservation);
            }
            match reservation.stage(cost) {
                Ok(charge) => Some(charge),
                Err(error) => reject!(map_reservation_error(error)),
            }
        } else if let Some(pool) = &self.capacity {
            match pool.try_reserve_ordinary(cost) {
                Ok(charge) => Some(charge),
                Err(error) => reject!(map_reservation_error(error)),
            }
        } else {
            if cost.bytes != 0 {
                reject!(SWSpawnError::Disabled);
            }
            None
        };
        let bytes = if options.retained_bytes != 0 {
            match capacity
                .as_ref()
                .expect("retained bytes require capacity")
                .retain_bytes(options.retained_bytes)
            {
                Ok(bytes) => Some(bytes),
                Err(error) => reject!(map_reservation_error(error)),
            }
        } else {
            None
        };
        let mut state = self.lock();
        if state.abandoned {
            reject!(SWSpawnError::Closed);
        }
        if options
            .priority
            .is_some_and(|rank| !state.demand.contains(rank))
            || (options.provider_demand.is_some() && !state.demand.enabled())
        {
            reject!(SWSpawnError::InvalidPriority);
        }
        if state.records.len() + state.external.len() >= self.limits.records {
            reject!(SWSpawnError::Full);
        }
        let delivery_capacity = if unaccounted_delivery {
            capacity.as_ref().map(|capacity| {
                capacity
                    .stage(SWCost::new(0, 0, 1, 0))
                    .expect("external stage reserved promised delivery")
            })
        } else {
            None
        };
        if let Some(ticket) = options.delivery.as_mut() {
            let rejection = if ticket.runtime_identity() != control.identity() {
                Some(SWSpawnError::InvalidDelivery)
            } else {
                ticket.try_commit().err().map(|error| match error {
                    crate::owner::TicketCommitError::Closed => SWSpawnError::Closed,
                    crate::owner::TicketCommitError::AlreadyCommitted => {
                        SWSpawnError::InvalidDelivery
                    }
                })
            };
            if let Some(reason) = rejection {
                reject!(reason);
            }
        }
        let id = state.next_id;
        state.next_id = id.checked_add(1).expect("owned record identity exhausted");
        let (task, sink) = SWTask::pending_pair();
        task.set_producer(control.identity(), id, Arc::downgrade(self));
        let core = Arc::new(ExternalCore::new(sink, bytes));
        if state.demand.enabled() {
            state
                .demand
                .register(id, options.priority, &[], options.provider_demand.take())
                .expect("external rank validated before commitment");
        }
        let bound_ticket = options.delivery.take().inspect(|ticket| {
            if let Some(charge) = delivery_capacity {
                ticket.attach_capacity(charge);
            }
        });
        let set_registration = work_set.clone();
        let finalizer_core = Arc::clone(&core);
        state.external.insert(
            id,
            ExternalRecord {
                settling: false,
                settle: Arc::new(move |status| finalizer_core.publish_status(status)),
                admission,
                work_set,
                capacity,
            },
        );
        drop(state);
        if let Some(ticket) = bound_ticket {
            ticket.bind(task.completion());
        }
        let weak = Arc::downgrade(self);
        let producer_control = SWProducerControl::new(Box::new(move || {
            if let Some(scheduler) = weak.upgrade() {
                scheduler.settle_external(id, SWTaskStatus::Cancelled);
            }
        }));
        if let Some(registration) = set_registration {
            registration.register_producer(producer_control.clone());
        }
        let producer = SWProducer {
            scheduler: Arc::downgrade(self),
            id,
            core,
        };
        self.service_demand(32);
        Ok((producer, task, producer_control))
    }

    pub(crate) fn claim_external(&self, id: u64, abandonment: bool) -> bool {
        let mut state = self.lock();
        if state.abandoned && !abandonment {
            return false;
        }
        let Some(record) = state.external.get_mut(&id) else {
            return false;
        };
        if record.settling {
            return false;
        }
        record.settling = true;
        true
    }

    pub(crate) fn finish_external(&self, id: u64, publish: impl FnOnce()) {
        let publication = catch_unwind(AssertUnwindSafe(publish));
        let (record, provider) = {
            let mut state = self.lock();
            let record = state.external.remove(&id);
            let provider = state.demand.remove(id);
            (record, provider)
        };
        drop(provider);
        if let Some(record) = record {
            let ExternalRecord {
                admission,
                work_set,
                capacity,
                settle,
                ..
            } = record;
            drop((settle, capacity, work_set, admission));
        }
        if let Err(payload) = publication {
            let _ = catch_unwind(AssertUnwindSafe(|| drop(payload)));
        }
    }

    pub(crate) fn settle_external(&self, id: u64, status: SWTaskStatus) {
        if !self.claim_external(id, status == SWTaskStatus::Abandoned) {
            return;
        }
        let settle = {
            let state = self.lock();
            state
                .external
                .get(&id)
                .map(|record| Arc::clone(&record.settle))
        };
        if let Some(settle) = settle {
            self.finish_external(id, move || settle(status));
        }
    }

    fn push_ready(state: &mut State, class: SWExecutionClass, id: u64) {
        if state.records.get(&id).is_some_and(|record| record.resource) {
            let selection = state
                .demand
                .selection(id)
                .expect("admitted resource demand");
            state.ready.push_resource(class, id, selection);
        } else {
            state.ready.push(class, id);
        }
    }

    pub(crate) fn prepare_stage(
        &self,
        runtime: u64,
        options: &crate::execution::SWStageOptions<'_>,
    ) -> Result<(SubmitExtras, Option<SWByteLease>), SWSpawnError> {
        if options.work_set.is_some() && options.discovery.is_some() {
            return Err(SWSpawnError::InvalidContext);
        }
        let work_set = match (options.work_set, options.discovery) {
            (Some(set), _) => Some(set.try_root_lease(runtime)),
            (_, Some(permit)) => Some(permit.try_child_lease(runtime)),
            _ => None,
        }
        .transpose()
        .map_err(|error| match error {
            SWDiscoveryError::Full => SWSpawnError::Full,
            _ => SWSpawnError::Closed,
        })?;
        let cost = SWCost::new(
            options.cost.records.max(1),
            options.cost.edges.max(options.prerequisites.len()),
            options
                .cost
                .deliveries
                .max(usize::from(options.delivery.is_some()))
                - usize::from(
                    options
                        .delivery
                        .as_ref()
                        .is_some_and(|ticket| ticket.is_accounted()),
                ),
            options.cost.bytes,
        );
        if options.retained_bytes > cost.bytes {
            return Err(SWSpawnError::TooLarge);
        }
        let capacity = if let Some(reservation) = options.reservation {
            if reservation.runtime_identity() != runtime {
                return Err(SWSpawnError::InvalidReservation);
            }
            Some(reservation.stage(cost).map_err(map_reservation_error)?)
        } else if let Some(pool) = &self.capacity {
            Some(
                pool.try_reserve_ordinary(cost)
                    .map_err(map_reservation_error)?,
            )
        } else {
            if cost.bytes != 0 {
                return Err(SWSpawnError::Disabled);
            }
            None
        };
        let bytes = if options.retained_bytes != 0 {
            Some(
                capacity
                    .as_ref()
                    .expect("retained bytes require capacity")
                    .retain_bytes(options.retained_bytes)
                    .map_err(map_reservation_error)?,
            )
        } else {
            None
        };
        Ok((
            SubmitExtras {
                work_set,
                capacity,
                priority: options.priority,
                reserved: options.reservation.is_some(),
            },
            bytes,
        ))
    }

    pub(crate) fn attach_demand(
        self: &Arc<Self>,
        id: u64,
        priority: SWPriority,
    ) -> Result<SWDemand, SWDemandError> {
        let lease = {
            let mut state = self.lock();
            if state.abandoned {
                return Err(SWDemandError::Closed);
            }
            state.demand.attach(id, priority)?
        };
        let scheduler = Arc::downgrade(self);
        let demand = SWDemand::new(
            lease,
            Arc::new(move |id, command| {
                let scheduler = scheduler.upgrade().ok_or(SWDemandError::Closed)?;
                scheduler.lock().demand.change(id, command)?;
                scheduler.service_demand(32);
                Ok(())
            }),
        );
        self.service_demand(32);
        Ok(demand)
    }

    fn service_demand_chunk(&self, budget: usize) {
        let providers = {
            let mut state = self.lock();
            let changes = state.demand.service(budget);
            let mut providers = Vec::new();
            for change in changes {
                if let Some(record) = state.records.get(&change.id)
                    && record.resource
                    && record.stage == Stage::Ready
                {
                    let class = record.class;
                    state
                        .ready
                        .update_resource(class, change.id, change.selection);
                }
                if let Some(provider) = change.provider {
                    providers.push(provider);
                }
            }
            providers
        };
        for (provider, snapshot) in providers {
            // Provider demand is advisory. A panicking hook cannot unwind an
            // admission or worker dispatch after the record was committed.
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| provider(snapshot))) {
                let _ = catch_unwind(AssertUnwindSafe(|| drop(payload)));
            }
        }
    }

    pub(crate) fn service_demand(self: &Arc<Self>, budget: usize) -> bool {
        self.service_demand_chunk(budget);
        self.dispatch_ready();
        self.lock().demand.pending_updates()
    }

    pub(crate) fn new(
        control: Weak<RuntimeControl>,
        limits: SWOwnedLimits,
        capacity: Option<SWReservationPool>,
        priorities: Vec<SWPriority>,
        demand_leases: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            control,
            limits,
            capacity,
            state: Mutex::new(State {
                records: HashMap::new(),
                external: HashMap::new(),
                next_id: 1,
                next_group: 1,
                edges: 0,
                ready: ReadyQueues::with_priorities(&priorities),
                deferred: std::array::from_fn(|_| VecDeque::new()),
                abandoned: false,
                demand: DemandState::new(priorities, demand_leases),
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
        self.submit_payload_delivering(request, payload, run, application_failed, &mut None)
    }

    pub(crate) fn submit_payload_delivering<P, T>(
        self: &Arc<Self>,
        request: SubmitRequest<'_>,
        payload: P,
        run: fn(P) -> T,
        application_failed: fn(&T) -> bool,
        delivery: &mut Option<crate::owner::SWDeliveryTicket>,
    ) -> SWSpawnResult<T, P>
    where
        P: Send + 'static,
        T: Send + 'static,
    {
        self.submit_payload_accounted(
            request,
            payload,
            run,
            application_failed,
            delivery,
            SubmitExtras::default(),
        )
    }

    pub(crate) fn submit_payload_in_set<P: Send + 'static, T: Send + 'static>(
        self: &Arc<Self>,
        request: SubmitRequest<'_>,
        payload: P,
        run: fn(P) -> T,
        application_failed: fn(&T) -> bool,
        lease: WorkSetLease,
    ) -> SWSpawnResult<T, P> {
        self.submit_payload_accounted(
            request,
            payload,
            run,
            application_failed,
            &mut None,
            SubmitExtras {
                work_set: Some(lease),
                ..SubmitExtras::default()
            },
        )
    }

    pub(crate) fn submit_payload_accounted<P: Send + 'static, T: Send + 'static>(
        self: &Arc<Self>,
        request: SubmitRequest<'_>,
        payload: P,
        run: fn(P) -> T,
        application_failed: fn(&T) -> bool,
        delivery: &mut Option<crate::owner::SWDeliveryTicket>,
        mut extras: SubmitExtras,
    ) -> SWSpawnResult<T, P> {
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
        if extras.capacity.is_none()
            && let Some(capacity) = &self.capacity
        {
            match capacity.try_reserve_ordinary(SWCost::new(
                1,
                prerequisites.len(),
                usize::from(
                    delivery
                        .as_ref()
                        .is_some_and(|ticket| !ticket.is_accounted()),
                ),
                0,
            )) {
                Ok(charge) => extras.capacity = Some(charge),
                Err(error) => return Err(reject(map_reservation_error(error), payload)),
            }
        }
        let mut state = self.lock();
        if state.abandoned {
            return Err(reject(SWSpawnError::Closed, payload));
        }
        if extras
            .priority
            .is_some_and(|priority| !state.demand.contains(priority))
        {
            return Err(reject(SWSpawnError::InvalidPriority, payload));
        }
        if prerequisites.len() > self.limits.edges {
            return Err(reject(SWSpawnError::TooLarge, payload));
        }
        if state.records.len() + state.external.len() >= self.limits.records
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
        if saturated && !inline && !extras.reserved {
            if let Some(group) = &group_inner {
                group.finish();
            }
            return Err(reject(SWSpawnError::Full, payload));
        }
        let delivery_capacity = if delivery
            .as_ref()
            .is_some_and(|ticket| !ticket.is_accounted())
        {
            extras.capacity.as_ref().map(|capacity| {
                capacity
                    .stage(SWCost::new(0, 0, 1, 0))
                    .expect("stage reserved promised delivery")
            })
        } else {
            None
        };
        if let Some(ticket) = delivery.as_mut() {
            let rejection = if ticket.runtime_identity() != control.identity() {
                Some(SWSpawnError::InvalidDelivery)
            } else {
                ticket.try_commit().err().map(|error| match error {
                    crate::owner::TicketCommitError::Closed => SWSpawnError::Closed,
                    crate::owner::TicketCommitError::AlreadyCommitted => {
                        SWSpawnError::InvalidDelivery
                    }
                })
            };
            if let Some(reason) = rejection {
                if let Some(group) = &group_inner {
                    group.finish();
                }
                return Err(reject(reason, payload));
            }
        }
        let id = state.next_id;
        state.next_id = id.checked_add(1).expect("owned record identity exhausted");
        let (task, sink) = SWTask::pending_pair();
        task.set_producer(control.identity(), id, Arc::downgrade(self));
        if state.demand.enabled() {
            let parents = prerequisites
                .iter()
                .filter_map(|completion| completion.producer_identity())
                .filter_map(|(runtime, record)| (runtime == control.identity()).then_some(record))
                .collect::<Vec<_>>();
            state
                .demand
                .register(id, extras.priority, &parents, None)
                .expect("resource rank validated before commitment");
        }
        if let Some(ticket) = delivery.take() {
            if let Some(capacity) = delivery_capacity {
                ticket.attach_capacity(capacity);
            }
            // This new result cannot complete before the record is published.
            // Binding registers an internal notifier, never a user callback.
            // The delivery entitlement was committed before exposing CPU work.
            ticket.bind(task.completion());
        }
        let envelope = make_envelope(payload, run, application_failed, sink);
        let job = Arc::new(Job {
            id,
            class,
            scheduler: Arc::downgrade(self),
            envelope: Mutex::new(Some(envelope)),
        });
        let stage = if !prerequisites.is_empty() {
            Stage::Waiting
        } else if saturated && !inline {
            Stage::DeferredReady
        } else {
            Stage::Ready
        };
        state.edges += prerequisites.len();
        let set_registration = extras.work_set.clone();
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
                work_set: extras.work_set,
                capacity: extras.capacity,
                resource: extras.priority.is_some(),
            },
        );
        if stage == Stage::Ready {
            Self::push_ready(&mut state, class, id);
        } else if stage == Stage::DeferredReady {
            state.deferred[class.index()].push_back(id);
        }
        drop(state);
        let weak = Arc::downgrade(self);
        let producer = SWProducerControl::new(Box::new(move || {
            if let Some(scheduler) = weak.upgrade() {
                scheduler.suppress(id, SWTaskStatus::Cancelled);
            }
        }));
        if let Some(registration) = set_registration {
            registration.register_producer(producer.clone());
        }
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
                    Self::push_ready(&mut state, class, id);
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

    fn suppress(self: &Arc<Self>, id: u64, mut status: SWTaskStatus) {
        let job = {
            let mut state = self.lock();
            if state.abandoned {
                status = SWTaskStatus::Abandoned;
            }
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
        if !state.deferred[class.index()].is_empty() {
            // Ready and deferred-ready are both unhanded work. Refill the
            // bounded runnable window from both so capacity pressure cannot
            // freeze a background resource ahead of newly urgent work.
            // Keep previously-ready ordinary entries before deferred ordinary
            // entries, preserving their FIFO activation order.
            let mut pending = VecDeque::new();
            while let Some(id) = state.ready.pop(class) {
                state
                    .records
                    .get_mut(&id)
                    .expect("ready record exists")
                    .stage = Stage::DeferredReady;
                state.ready.runnable[class.index()] -= 1;
                pending.push_back(id);
            }
            pending.append(&mut state.deferred[class.index()]);
            state.deferred[class.index()] = pending;
        }
        while state.ready.runnable[class.index()] < self.limits.runnable_for(class) {
            let queue = &state.deferred[class.index()];
            let ordinary = queue
                .iter()
                .enumerate()
                .find(|(_, id)| state.records.get(id).is_some_and(|record| !record.resource))
                .map(|(index, id)| (index, *id));
            let resource = queue
                .iter()
                .enumerate()
                .filter_map(|(index, id)| {
                    let record = state.records.get(id)?;
                    if !record.resource {
                        return None;
                    }
                    let selection = state.demand.selection(*id)?;
                    Some((
                        index,
                        *id,
                        (!selection.active, selection.priority, selection.tie),
                    ))
                })
                .min_by_key(|(_, _, key)| *key);
            let index = match (ordinary, resource) {
                (Some((index, id)), Some((_, resource, _))) if id < resource => Some(index),
                (_, Some((index, _, _))) | (Some((index, _)), None) => Some(index),
                (None, None) => None,
            };
            let Some(id) = index.and_then(|index| state.deferred[class.index()].remove(index))
            else {
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
            Self::push_ready(state, class, id);
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
        let (record, provider) = {
            let mut state = self.lock();
            let Some(record) = state.records.remove(&id) else {
                return;
            };
            state.edges -= record.edges;
            let provider = state.demand.remove(id);
            (record, provider)
        };
        drop(provider);
        // Strong settlement follows capacity return, not just result readiness.
        drop(record.capacity);
        if let Some(group) = record.group {
            group.finish();
        }
        drop((record.work_set, record.admission));
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
        let (ids, external) = {
            let mut state = self.lock();
            state.abandoned = true;
            let ids = state
                .records
                .iter()
                .filter(|(_, record)| record.stage != Stage::Running)
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            let external = state.external.keys().copied().collect::<Vec<_>>();
            (ids, external)
        };
        for id in ids {
            self.suppress(id, SWTaskStatus::Abandoned);
        }
        for id in external {
            self.settle_external(id, SWTaskStatus::Abandoned);
        }
    }

    fn dispatch(self: &Arc<Self>) {
        self.service_demand_chunk(32);
        self.dispatch_ready();
    }

    fn dispatch_ready(self: &Arc<Self>) {
        let Some(control) = self.control.upgrade() else {
            return;
        };
        for class in SWExecutionClass::ALL {
            loop {
                let job = {
                    let mut state = self.lock();
                    if state.abandoned {
                        break;
                    }
                    self.promote_locked(&mut state, class);
                    if state.ready.handed_off[class.index()] >= self.limits.handoff_for(class) {
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

pub(crate) fn map_reservation_error(error: SWReservationError) -> SWSpawnError {
    match error {
        SWReservationError::Full | SWReservationError::InsufficientCredits => SWSpawnError::Full,
        SWReservationError::TooLarge => SWSpawnError::TooLarge,
        SWReservationError::Closed => SWSpawnError::Closed,
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
