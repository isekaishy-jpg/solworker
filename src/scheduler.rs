//! Coordination of accepted owned work.
//!
//! Class gates serialize acceptance, ready selection and claiming. Stable job
//! controls retain dependency and settlement state. Demand, global charges and
//! recyclers have separate synchronization domains.
//! User code, destructors, provider hooks, and backend calls run outside locks.

mod admission;
mod demand;
mod ready;
pub(crate) mod reservation;
pub(crate) mod storage;
pub(crate) mod work_set;

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
use ready::ReadyQueue;
use reservation::SWReservationPool;
pub use reservation::{
    SWByteLease, SWCapacityUsage, SWCost, SWLimitError, SWLimits, SWReservation, SWReservationError,
};
use storage::{BufferPool, GroupPool, JobHandle, JobPool, SignalPool};
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

#[cfg(test)]
type AttachmentHook = Arc<dyn Fn(JobHandle) + Send + Sync>;

#[cfg(test)]
#[path = "../tests/unit/scheduler_domains.rs"]
mod domain_tests;

enum Activation {
    Prerequisite(Arc<OwnedScheduler>, JobHandle, SWTaskStatus),
    Finalize(Arc<OwnedScheduler>, JobHandle),
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
                scheduler.activate_prerequisite(&id, status)
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

/// A backend wrapper occupies its handoff slot even if its logical job has
/// already settled. Discard during stop must release the slot without running
/// scheduler callbacks under the backend's queue lock.
struct Handoff {
    scheduler: Weak<OwnedScheduler>,
    class: SWExecutionClass,
    completed: bool,
}

impl Handoff {
    fn run(mut self, job: JobHandle) {
        job.run();
        self.completed = true;
    }
}

impl Drop for Handoff {
    fn drop(&mut self) {
        if let Some(scheduler) = self.scheduler.upgrade() {
            scheduler.decrement_handoff(self.class);
            let abandoned = scheduler.abandoned.load(Ordering::Acquire);
            if self.completed && !abandoned {
                scheduler.dispatch_class(self.class);
            }
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
    completion: Option<SWCompletion>,
    group: Option<Arc<GroupInner>>,
    pending: usize,
    failed: bool,
    policy: SWDependencyPolicy,
    stage: Stage,
    eligibility: SWCallerEligibility,
    subscriptions: Vec<Subscription>,
    attaching: bool,
    deferred_finish: Option<Finish>,
    admission: OwnedAdmission,
    work_set: Option<WorkSetLease>,
    capacity: Option<SWReservation>,
    resource: bool,
    selection: Option<demand::DemandSelection>,
    charge: AccountingLease,
}

struct ExternalRecord {
    charge: AccountingLease,
    attaching: bool,
    deferred_status: Option<SWTaskStatus>,
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

/// A charge is acquired before acceptance and retained through strong settlement.
/// CAS rollback keeps the record/edge limits global across all classes/providers.
struct Accounting {
    records: AtomicUsize,
    edges: AtomicUsize,
    limits: SWOwnedLimits,
}

struct AccountingLease {
    accounting: Arc<Accounting>,
    edges: usize,
}

impl Accounting {
    fn acquire(self: &Arc<Self>, edges: usize) -> Result<AccountingLease, SWSpawnError> {
        if edges > self.limits.edges {
            return Err(SWSpawnError::TooLarge);
        }
        self.records
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.limits.records).then_some(count + 1)
            })
            .map_err(|_| SWSpawnError::Full)?;
        if edges != 0
            && self
                .edges
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count
                        .checked_add(edges)
                        .filter(|count| *count <= self.limits.edges)
                })
                .is_err()
        {
            self.records.fetch_sub(1, Ordering::AcqRel);
            return Err(SWSpawnError::Full);
        }
        Ok(AccountingLease {
            accounting: Arc::clone(self),
            edges,
        })
    }
}

impl Drop for AccountingLease {
    fn drop(&mut self) {
        if self.edges != 0 {
            self.accounting
                .edges
                .fetch_sub(self.edges, Ordering::AcqRel);
        }
        self.accounting.records.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Ordinary operations enter exactly one class gate, then a record guard.
/// No record-only operation enters a class while retaining its record guard.
struct ClassState {
    records: HashMap<u64, JobHandle>,
    ready: ReadyQueue,
    deferred: VecDeque<u64>,
}

struct DemandDomain {
    graph: DemandState,
    targets: HashMap<u64, SWExecutionClass>,
}

#[cfg(feature = "diagnostics")]
#[derive(Default)]
struct DomainMetrics {
    acquisitions: AtomicU64,
    wait_ns: AtomicU64,
    hold_ns: AtomicU64,
}

/// Notifications and diagnostic buffering follow the release of every guard.
struct DomainGuard<'a, T> {
    guard: Option<MutexGuard<'a, T>>,
    wake: &'a crate::progress::SWWake,
    _notification: crate::notification::NotificationScope,
    #[cfg(feature = "diagnostics")]
    timing: Option<(std::time::Instant, std::time::Instant)>,
    #[cfg(feature = "diagnostics")]
    scheduler: u64,
    #[cfg(feature = "diagnostics")]
    runtime: u64,
    #[cfg(feature = "diagnostics")]
    domain: u64,
    #[cfg(feature = "diagnostics")]
    site: u32,
    #[cfg(feature = "diagnostics")]
    cycles: Option<u64>,
    #[cfg(feature = "diagnostics")]
    metrics: &'a DomainMetrics,
}

impl<T> std::ops::Deref for DomainGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.as_deref().expect("live domain guard")
    }
}
impl<T> std::ops::DerefMut for DomainGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.guard.as_deref_mut().expect("live domain guard")
    }
}
impl<T> Drop for DomainGuard<'_, T> {
    fn drop(&mut self) {
        #[cfg(feature = "diagnostics")]
        let cycles = self
            .cycles
            .and_then(|start| crate::diagnostics::cycles().map(|end| end.saturating_sub(start)));
        #[cfg(feature = "diagnostics")]
        let released = self.timing.map(|_| std::time::Instant::now());
        drop(self.guard.take());
        #[cfg(feature = "diagnostics")]
        if let (Some((started, acquired)), Some(released)) = (self.timing, released) {
            let waiting = acquired
                .duration_since(started)
                .as_nanos()
                .min(u64::MAX as u128) as u64;
            let holding = released
                .duration_since(acquired)
                .as_nanos()
                .min(u64::MAX as u128) as u64;
            self.metrics.acquisitions.fetch_add(1, Ordering::Relaxed);
            self.metrics.wait_ns.fetch_add(waiting, Ordering::Relaxed);
            self.metrics.hold_ns.fetch_add(holding, Ordering::Relaxed);
            if waiting >= 50_000 {
                crate::diagnostics::record_at(
                    "scheduler.lock.slow",
                    self.runtime,
                    self.scheduler,
                    waiting,
                    acquired,
                );
                crate::diagnostics::record_at(
                    "scheduler.domain.lock.slow",
                    self.runtime,
                    self.domain,
                    waiting,
                    acquired,
                );
            }
            if holding >= 50_000 {
                crate::diagnostics::record_at(
                    "scheduler.hold.slow",
                    self.runtime,
                    self.scheduler,
                    holding,
                    released,
                );
                crate::diagnostics::record_at(
                    "scheduler.domain.hold.slow",
                    self.runtime,
                    self.domain,
                    holding,
                    released,
                );
                crate::diagnostics::record_at(
                    "scheduler.hold.site",
                    self.runtime,
                    self.scheduler,
                    u64::from(self.site),
                    released,
                );
                if let Some(cycles) = cycles {
                    crate::diagnostics::record_at(
                        "scheduler.hold.cycles",
                        self.runtime,
                        self.scheduler,
                        cycles,
                        released,
                    );
                }
            }
        }
        self.wake.notify();
    }
}

struct ProviderCallbacks<'a> {
    scheduler: &'a OwnedScheduler,
    count: usize,
}
impl Drop for ProviderCallbacks<'_> {
    fn drop(&mut self) {
        self.scheduler
            .provider_callbacks
            .fetch_sub(self.count, Ordering::AcqRel);
        self.scheduler.wake.notify();
    }
}

/// Stable controls and independently coordinated execution classes.
pub(crate) struct OwnedScheduler {
    control: Weak<RuntimeControl>,
    #[cfg(feature = "diagnostics")]
    trace_runtime: u64,
    #[cfg(feature = "diagnostics")]
    metrics: [DomainMetrics; 5],
    limits: SWOwnedLimits,
    classes: [Mutex<ClassState>; 3],
    external: Mutex<HashMap<u64, ExternalRecord>>,
    abandoned: AtomicBool,
    next_id: AtomicU64,
    next_group: AtomicU64,
    accounting: Arc<Accounting>,
    demand: Mutex<DemandDomain>,
    // Snapshot publication serializes only graph and bounded class mailboxes,
    // never queue/record gates. Each class applies only its own mailbox.
    demand_transfer: Mutex<()>,
    demand_pending: AtomicBool,
    demand_updates: [Mutex<HashMap<u64, demand::DemandSelection>>; 3],
    demand_updates_pending: [AtomicBool; 3],
    demand_enabled: bool,
    provider_callbacks: AtomicUsize,
    jobs: JobPool,
    signals: SignalPool,
    subscriptions: Mutex<BufferPool<Subscription>>,
    groups: GroupPool,
    pub(crate) capacity: Option<SWReservationPool>,
    wake: Arc<crate::progress::SWWake>,
    #[cfg(test)]
    attachment_hook: Mutex<Option<AttachmentHook>>,
}

impl OwnedScheduler {
    #[cfg(feature = "diagnostics")]
    fn trace(&self, event: &'static str, id: u64, related: u64) {
        crate::diagnostics::record(event, self.trace_runtime, id, related);
    }

    #[cfg(feature = "diagnostics")]
    fn trace_job(&self, job: &JobHandle) -> crate::diagnostics::JobGuard {
        crate::diagnostics::JobGuard::enter(crate::diagnostics::SWTraceJob {
            runtime: self.trace_runtime,
            id: job.id(),
            group: job.group_id(),
        })
    }

    #[track_caller]
    fn domain_lock<'a, T>(&'a self, mutex: &'a Mutex<T>, _domain: u64) -> DomainGuard<'a, T> {
        let notification = self.wake.notification_scope();
        #[cfg(feature = "diagnostics")]
        let started = crate::diagnostics::clock();
        let guard = mutex.lock().unwrap_or_else(|error| error.into_inner());
        DomainGuard {
            guard: Some(guard),
            wake: &self.wake,
            _notification: notification,
            #[cfg(feature = "diagnostics")]
            timing: started.map(|start| (start, std::time::Instant::now())),
            #[cfg(feature = "diagnostics")]
            scheduler: self as *const Self as usize as u64,
            #[cfg(feature = "diagnostics")]
            runtime: self.trace_runtime,
            #[cfg(feature = "diagnostics")]
            domain: _domain,
            #[cfg(feature = "diagnostics")]
            site: std::panic::Location::caller().line(),
            #[cfg(feature = "diagnostics")]
            cycles: started.and_then(|_| crate::diagnostics::cycles()),
            #[cfg(feature = "diagnostics")]
            metrics: &self.metrics[_domain as usize],
        }
    }

    #[track_caller]
    fn class_lock(&self, class: SWExecutionClass) -> DomainGuard<'_, ClassState> {
        self.domain_lock(&self.classes[class.index()], class.index() as u64)
    }

    fn allocate_id(counter: &AtomicU64) -> u64 {
        counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("scheduler identity exhausted")
    }

    pub(crate) fn new(
        control: Weak<RuntimeControl>,
        limits: SWOwnedLimits,
        capacity: Option<SWReservationPool>,
        priorities: Vec<SWPriority>,
        demand_leases: usize,
    ) -> Arc<Self> {
        let wake = control.upgrade().expect("building live runtime").wake();
        if let Some(capacity) = &capacity {
            capacity.set_wake(Arc::clone(&wake));
        }
        Arc::new(Self {
            #[cfg(feature = "diagnostics")]
            trace_runtime: control.upgrade().expect("building live runtime").identity(),
            #[cfg(feature = "diagnostics")]
            metrics: std::array::from_fn(|_| DomainMetrics::default()),
            control,
            limits,
            capacity,
            wake,
            classes: std::array::from_fn(|_| {
                Mutex::new(ClassState {
                    records: HashMap::new(),
                    ready: ReadyQueue::with_priorities(&priorities),
                    deferred: VecDeque::new(),
                })
            }),
            external: Mutex::new(HashMap::new()),
            abandoned: AtomicBool::new(false),
            next_id: AtomicU64::new(1),
            next_group: AtomicU64::new(1),
            accounting: Arc::new(Accounting {
                records: AtomicUsize::new(0),
                edges: AtomicUsize::new(0),
                limits,
            }),
            demand_enabled: !priorities.is_empty(),
            demand: Mutex::new(DemandDomain {
                graph: DemandState::new(priorities, demand_leases),
                targets: HashMap::new(),
            }),
            demand_transfer: Mutex::new(()),
            demand_pending: AtomicBool::new(false),
            demand_updates: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            demand_updates_pending: std::array::from_fn(|_| AtomicBool::new(false)),
            provider_callbacks: AtomicUsize::new(0),
            jobs: JobPool::new(limits.records),
            signals: SignalPool::new(limits.records),
            subscriptions: Mutex::new(BufferPool::new(limits.edges)),
            groups: GroupPool::new(limits.records),
            #[cfg(test)]
            attachment_hook: Mutex::new(None),
        })
    }

    pub(crate) fn quiescent(&self) -> bool {
        self.accounting.records.load(Ordering::Acquire) == 0
            && self.classes.iter().all(|class| {
                class
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .ready
                    .handed_off
                    == 0
            })
            && !self.demand_pending.load(Ordering::Acquire)
            && self
                .demand_updates_pending
                .iter()
                .all(|pending| !pending.load(Ordering::Acquire))
            && self.provider_callbacks.load(Ordering::Acquire) == 0
    }

    pub(crate) fn progress(&self) -> crate::progress::SWSchedulerProgress {
        #[cfg(feature = "diagnostics")]
        for (domain, metrics) in self.metrics.iter().enumerate() {
            self.trace(
                "scheduler.domain.acquisitions",
                domain as u64,
                metrics.acquisitions.load(Ordering::Relaxed),
            );
            self.trace(
                "scheduler.domain.wait_ns",
                domain as u64,
                metrics.wait_ns.load(Ordering::Relaxed),
            );
            self.trace(
                "scheduler.domain.hold_ns",
                domain as u64,
                metrics.hold_ns.load(Ordering::Relaxed),
            );
        }
        let mut snapshot = crate::progress::SWSchedulerProgress {
            demand_pending: self.demand_pending.load(Ordering::Acquire)
                || self
                    .demand_updates_pending
                    .iter()
                    .any(|pending| pending.load(Ordering::Acquire)),
            provider_callbacks: self.provider_callbacks.load(Ordering::Acquire),
            records_full: self.accounting.records.load(Ordering::Acquire) >= self.limits.records,
            edges_full: self.limits.edges != 0
                && self.accounting.edges.load(Ordering::Acquire) >= self.limits.edges,
            ..Default::default()
        };
        for class in SWExecutionClass::ALL {
            let state = self.classes[class.index()]
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let index = class.index();
            snapshot.handoff_wrappers[index] = state.ready.handed_off;
            snapshot.runnable_full[index] = state.ready.runnable >= self.limits.runnable[index];
            snapshot.handoff_full[index] = state.ready.handed_off >= self.limits.handoff[index];
            for job in state.records.values() {
                if let Some(record) = job.record_lock().as_ref() {
                    match record.stage {
                        Stage::Waiting => snapshot.waiting += 1,
                        Stage::DeferredReady => snapshot.deferred += 1,
                        Stage::Ready => snapshot.ready += 1,
                        Stage::Handed => snapshot.handed += 1,
                        Stage::Running => snapshot.running += 1,
                        Stage::Finalizing => snapshot.finalizing += 1,
                    }
                }
            }
        }
        snapshot
    }

    fn reserve_group_id(&self, class: SWExecutionClass) -> Result<u64, SWSpawnError> {
        // Identity reservation uses the group's own acceptance gate.
        let _gate = self.class_lock(class);
        if self.abandoned.load(Ordering::Acquire) {
            return Err(SWSpawnError::Closed);
        }
        Ok(Self::allocate_id(&self.next_group))
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

    pub(crate) fn group(
        self: &Arc<Self>,
        class: SWExecutionClass,
    ) -> Result<SWGroup, SWSpawnError> {
        let control = self.control.upgrade().ok_or(SWSpawnError::Closed)?;
        let admission = control.admit_owned(false).map_err(|error| match error {
            crate::execution::SWExecutionError::Closed => SWSpawnError::Closed,
            crate::execution::SWExecutionError::InvalidContext => SWSpawnError::InvalidContext,
        })?;
        let id = self.reserve_group_id(class)?;
        let inner = self.groups.acquire(id, class);
        inner.set_notification_runtime(control.identity());
        if let Some(domain) = control.notification_domain() {
            inner.set_notification_source(domain);
        }
        // The reserved ID is the admission point. Keep the runtime admission
        // alive through checkout even when graceful closure races this call.
        drop(admission);
        Ok(SWGroup::new(
            inner,
            Arc::downgrade(self),
            control.identity(),
        ))
    }

    pub(crate) fn renew_group(self: &Arc<Self>, group: &mut SWGroup) -> Result<(), SWSpawnError> {
        let control = self.control.upgrade().ok_or(SWSpawnError::Closed)?;
        if group.runtime != control.identity()
            || !Weak::ptr_eq(&group.scheduler, &Arc::downgrade(self))
        {
            return Err(SWSpawnError::InvalidGroup);
        }
        let admission = control.admit_owned(false).map_err(|error| match error {
            crate::execution::SWExecutionError::Closed => SWSpawnError::Closed,
            crate::execution::SWExecutionError::InvalidContext => SWSpawnError::InvalidContext,
        })?;
        let id = self.reserve_group_id(group.class())?;
        if !group.try_reset(id) {
            let inner = self.groups.acquire(id, group.class());
            *group = SWGroup::new(inner, Arc::downgrade(self), control.identity());
        }
        group.inner.set_notification_runtime(control.identity());
        if let Some(domain) = control.notification_domain() {
            group.inner.set_notification_source(domain);
        }
        drop(admission);
        Ok(())
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
        let admission = match control.admit_external(work_set.is_some()) {
            Ok(admission) => admission,
            Err(crate::execution::SWExecutionError::Closed) => reject!(SWSpawnError::Closed),
            Err(crate::execution::SWExecutionError::InvalidContext) => {
                reject!(SWSpawnError::InvalidContext)
            }
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
        if options.priority.is_some_and(|rank| {
            !self
                .demand
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .graph
                .contains(rank)
        }) || (options.provider_demand.is_some() && !self.demand_enabled)
        {
            reject!(SWSpawnError::InvalidPriority);
        }
        let charge = match self.accounting.acquire(0) {
            Ok(charge) => charge,
            Err(reason) => reject!(reason),
        };
        let id = Self::allocate_id(&self.next_id);
        let (task, sink) = SWTask::pending_with_signal(self.signals.acquire());
        task.set_producer(control.identity(), id, Arc::downgrade(self));
        if let Some(domain) = control.notification_domain() {
            task.set_notification_source(domain);
        }
        let core = Arc::new(ExternalCore::new(sink, bytes));
        let mut state = self.domain_lock(&self.external, 4);
        if self.abandoned.load(Ordering::Acquire) {
            reject!(SWSpawnError::Closed);
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
                drop(state);
                reject!(reason);
            }
        }
        let set_registration = work_set.clone();
        let finalizer_core = Arc::clone(&core);
        // Attachment retains the accepted external record until graph and
        // delivery binding finish. Abandonment stages suppression during this
        // phase, and provider/cancellation controls are exposed afterward.
        state.insert(
            id,
            ExternalRecord {
                settling: false,
                attaching: true,
                deferred_status: None,
                settle: Arc::new(move |status| finalizer_core.publish_status(status)),
                admission,
                work_set,
                capacity,
                charge,
            },
        );
        drop(state);
        if self.demand_enabled {
            let mut demand = self.domain_lock(&self.demand, 3);
            demand
                .graph
                .register(id, options.priority, &[], options.provider_demand.take())
                .expect("external rank validated");
            self.demand_pending.store(true, Ordering::Release);
        }
        let bound_ticket = options.delivery.take();
        if let Some(ticket) = bound_ticket {
            if let Some(charge) = delivery_capacity {
                ticket.attach_capacity(charge);
            }
            ticket.bind(task.completion());
        }
        let deferred_status = {
            let mut state = self.domain_lock(&self.external, 4);
            let record = state
                .get_mut(&id)
                .expect("attachment retains external acceptance");
            record.attaching = false;
            record.deferred_status.take()
        };
        if let Some(status) = deferred_status {
            self.settle_external(id, status);
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
        self.service_demand_automatic();
        Ok((producer, task, producer_control))
    }

    pub(crate) fn claim_external(&self, id: u64, abandonment: bool) -> bool {
        let mut state = self.domain_lock(&self.external, 4);
        if self.abandoned.load(Ordering::Acquire) && !abandonment {
            return false;
        }
        let Some(record) = state.get_mut(&id) else {
            return false;
        };
        if record.settling {
            return false;
        }
        record.settling = true;
        true
    }

    pub(crate) fn finish_external(&self, id: u64, publish: impl FnOnce()) {
        let _context = context::ControlCallbackGuard::enter();
        let publication = catch_unwind(AssertUnwindSafe(publish));
        let record = self.domain_lock(&self.external, 4).remove(&id);
        self.remove_demand(id);
        if let Some(record) = record {
            let ExternalRecord {
                admission,
                work_set,
                capacity,
                settle,
                charge,
                ..
            } = record;
            drop((settle, capacity, work_set, charge, admission));
        }
        if let Err(payload) = publication {
            crate::cleanup::discard_panic(payload);
        }
        // Completing an external or ordinary CPU graph node can dirty its
        // ancestors even when no further CPU arrivals occur. Producers retain
        // an automatic graph route rather than relying on an application pump.
        self.service_demand_chunk(32);
        if let Some(control) = self.control.upgrade()
            && let Ok(scheduler) = control.owned_scheduler()
        {
            scheduler.dispatch_ready();
        }
        self.wake.notify();
    }

    pub(crate) fn settle_external(&self, id: u64, status: SWTaskStatus) {
        {
            let mut state = self.domain_lock(&self.external, 4);
            let Some(record) = state.get_mut(&id) else {
                return;
            };
            if record.attaching {
                record.deferred_status = Some(status);
                return;
            }
        }
        if !self.claim_external(id, status == SWTaskStatus::Abandoned) {
            return;
        }
        let settle = self
            .external
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&id)
            .map(|record| Arc::clone(&record.settle));
        if let Some(settle) = settle {
            self.finish_external(id, move || settle(status));
        }
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
        let admission = match control.admit_owned(extras.work_set.is_some()) {
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
        if extras.priority.is_some_and(|priority| {
            !self
                .demand
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .graph
                .contains(priority)
        }) {
            return Err(reject(SWSpawnError::InvalidPriority, payload));
        }
        let charge = match self.accounting.acquire(prerequisites.len()) {
            Ok(charge) => charge,
            Err(reason) => return Err(reject(reason, payload)),
        };
        let id = Self::allocate_id(&self.next_id);
        let (task, sink) = SWTask::pending_with_signal(self.signals.acquire());
        task.set_producer(control.identity(), id, Arc::downgrade(self));
        if let Some(domain) = control.notification_domain() {
            task.set_notification_source(domain);
        }
        // Prepare recycler storage without capturing the rejectable payload.
        let mut job = self.jobs.prepare(id, Arc::downgrade(self));
        let subscriptions = self
            .subscriptions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .acquire(prerequisites.len());
        let set_registration = extras.work_set.clone();
        let mut state = self.class_lock(class);
        if self.abandoned.load(Ordering::Acquire) {
            return Err(reject(SWSpawnError::Closed, payload));
        }
        let group_inner = if let Some(group) = group {
            if group.runtime != control.identity()
                || group.inner.class != class
                || !Weak::ptr_eq(&group.scheduler, &Arc::downgrade(self))
                || prerequisites
                    .iter()
                    .any(|input| input.same_signal(&group.completion()))
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
        let saturated =
            prerequisites.is_empty() && state.ready.runnable >= self.limits.runnable_for(class);
        let inline =
            saturated && allow_inline && options.eligibility == SWCallerEligibility::CallerEligible;
        let reason = if inline
            && context::current().is_some_and(|current| {
                current.runtime != control.identity() || current.class != class
            }) {
            Some(SWSpawnError::InvalidContext)
        } else if saturated && !inline && !extras.reserved {
            Some(SWSpawnError::Full)
        } else {
            None
        };
        if let Some(reason) = reason {
            drop(state);
            if let Some(group) = &group_inner {
                group.finish(None);
            }
            return Err(reject(reason, payload));
        }
        // Acceptance retains the existing class -> group/owner-route/capacity
        // order. Their notification scopes defer callbacks beyond the class
        // guard; rejection unwinds provisional charges after releasing it.
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
                drop(state);
                if let Some(group) = &group_inner {
                    group.finish(None);
                }
                return Err(reject(reason, payload));
            }
        }
        let group_id = group_inner.as_ref().map(|group| group.id);
        let priority = extras.priority;
        job.initialize(
            class,
            group_id,
            make_envelope(payload, run, application_failed, sink),
            Record {
                completion: group_inner.as_ref().map(|_| task.completion()),
                group: group_inner.clone(),
                pending: prerequisites.len(),
                failed: false,
                policy,
                stage: Stage::Waiting,
                eligibility: options.eligibility,
                subscriptions,
                // Every accepted record is visible to abandonment before dependency
                // and graph attachment. Closure, not a callback, ends this phase.
                attaching: true,
                deferred_finish: None,
                admission,
                work_set: extras.work_set,
                capacity: extras.capacity,
                resource: priority.is_some(),
                selection: None,
                charge,
            },
        );
        // This insertion is the acceptance point; all later failures settle the
        // accepted responsibility and never return the consumed payload.
        state.records.insert(id, job.clone());
        drop(state);
        if let Some(ticket) = delivery.take() {
            if let Some(capacity) = delivery_capacity {
                ticket.attach_capacity(capacity);
            }
            ticket.bind(task.completion());
        }
        if let Some(group) = &group_inner {
            group.register_member(job.downgrade());
        }
        if self.demand_enabled {
            let parents = prerequisites
                .iter()
                .filter_map(|completion| completion.producer_identity())
                .filter_map(|(runtime, id)| (runtime == control.identity()).then_some(id))
                .collect::<Vec<_>>();
            let mut demand = self.domain_lock(&self.demand, 3);
            demand
                .graph
                .register(id, priority, &parents, None)
                .expect("rank checked before acceptance");
            let selection = demand.graph.selection(id);
            if priority.is_some() {
                demand.targets.insert(id, class);
            }
            self.demand_pending.store(true, Ordering::Release);
            drop(demand);
            if let (Some(record), Some(selection)) = (job.record_lock().as_mut(), selection)
                && record
                    .selection
                    .is_none_or(|previous| previous.version < selection.version)
            {
                record.selection = Some(selection);
            }
        }
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
        let weak = Arc::downgrade(self);
        let producer = SWProducerControl::new(Box::new(move || {
            if let Some(scheduler) = weak.upgrade() {
                scheduler.suppress_id(class, id, SWTaskStatus::Cancelled);
            }
        }));
        if let Some(registration) = set_registration {
            registration.register_producer(producer.clone());
        }
        for prerequisite in prerequisites {
            let weak = Arc::downgrade(self);
            let target = job.downgrade();
            let subscription = prerequisite.subscribe_cancelable(Box::new(move |status| {
                if let (Some(scheduler), Some(job)) = (weak.upgrade(), target.upgrade()) {
                    enqueue_activation(Activation::Prerequisite(scheduler, job, status));
                }
            }));
            let mut record = job.record_lock();
            if let Some(record) = record.as_mut() {
                record.subscriptions.push(subscription);
            } else {
                drop(record);
                drop(subscription);
            }
        }
        let deferred_finish = {
            let _notification = self.wake.notification_scope();
            let mut record = job.record_lock();
            record.as_mut().and_then(|record| {
                record.attaching = false;
                record.deferred_finish.take()
            })
        };
        if let Some(finish) = deferred_finish {
            self.finish_record(&job, finish);
        } else {
            self.activate_ready(&job, inline);
        }
        if inline {
            self.claim_and_run(&job, true);
        } else {
            self.dispatch_class(class);
        }
        Ok((task, producer))
    }
    fn push_ready(&self, state: &mut ClassState, job: &JobHandle, record: &Record) {
        #[cfg(feature = "diagnostics")]
        self.trace("job.ready", job.id(), job.class().index() as u64);
        if record.resource {
            state.ready.push_resource(
                job.id(),
                record
                    .selection
                    .expect("resource graph attached before activation"),
            );
        } else {
            state.ready.push(job.id());
        }
    }

    fn activate_prerequisite(self: &Arc<Self>, job: &JobHandle, status: SWTaskStatus) {
        {
            let _notification = self.wake.notification_scope();
            let mut record = job.record_lock();
            let Some(record) = record.as_mut() else {
                return;
            };
            if record.stage != Stage::Waiting || record.pending == 0 {
                return;
            }
            record.pending -= 1;
            record.failed |= !status.is_success();
        }
        self.activate_ready(job, false);
        self.dispatch_class(job.class());
    }

    fn activate_ready(self: &Arc<Self>, job: &JobHandle, inline: bool) {
        let class = job.class();
        let (suppress, group) = {
            let mut state = self.class_lock(class);
            let mut record = job.record_lock();
            let Some(record) = record.as_mut() else {
                return;
            };
            if record.stage != Stage::Waiting || record.pending != 0 || record.attaching {
                return;
            }
            if self.abandoned.load(Ordering::Acquire) {
                (Some(SWTaskStatus::Abandoned), None)
            } else if record.failed && record.policy == SWDependencyPolicy::SuccessOnly {
                (Some(SWTaskStatus::PrerequisiteFailed), None)
            } else if inline || state.ready.runnable < self.limits.runnable_for(class) {
                record.stage = Stage::Ready;
                self.push_ready(&mut state, job, record);
                (None, record.group.clone())
            } else {
                record.stage = Stage::DeferredReady;
                state.deferred.push_back(job.id());
                (None, None)
            }
        };
        if let Some(group) = group {
            group.notify_ready();
        }
        if let Some(status) = suppress {
            self.suppress(job, status);
        }
    }

    fn suppress_id(self: &Arc<Self>, class: SWExecutionClass, id: u64, status: SWTaskStatus) {
        let job = self.classes[class.index()]
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .get(&id)
            .cloned();
        if let Some(job) = job {
            self.suppress(&job, status);
        }
    }

    fn suppress(self: &Arc<Self>, job: &JobHandle, mut status: SWTaskStatus) {
        let class = job.class();
        let promoted = {
            let mut state = self.class_lock(class);
            if self.abandoned.load(Ordering::Acquire) {
                status = SWTaskStatus::Abandoned;
            }
            let mut guard = job.record_lock();
            let Some(record) = guard.as_mut() else {
                return;
            };
            let stage = record.stage;
            let decrement = match stage {
                Stage::Waiting | Stage::DeferredReady => false,
                Stage::Ready | Stage::Handed => true,
                Stage::Running | Stage::Finalizing => return,
            };
            record.stage = Stage::Finalizing;
            match stage {
                Stage::Ready => state.ready.remove(job.id()),
                Stage::DeferredReady => state.deferred.retain(|id| *id != job.id()),
                _ => {}
            }
            drop(guard);
            if decrement {
                state.ready.runnable -= 1;
                self.promote_locked(&mut state, class)
            } else {
                Vec::new()
            }
        };
        for group in promoted {
            group.notify_ready();
        }
        self.settle(job, Decision::Suppress(status));
    }

    fn claim_and_run(self: &Arc<Self>, job: &JobHandle, caller: bool) -> bool {
        let Some(control) = self.control.upgrade() else {
            return false;
        };
        let class = job.class();
        // Notification capture cleanup cannot obtain an execution lease. A
        // handoff lease is deliberately weaker and never substitutes for this.
        let Ok(_lease) = control.acquire_owned(class) else {
            return false;
        };
        let promoted = {
            let mut state = self.class_lock(class);
            if self.abandoned.load(Ordering::Acquire) {
                return false;
            }
            let mut guard = job.record_lock();
            let Some(record) = guard.as_mut() else {
                return false;
            };
            if !matches!(record.stage, Stage::Ready | Stage::Handed)
                || (caller && record.eligibility != SWCallerEligibility::CallerEligible)
            {
                return false;
            }
            let was_ready = record.stage == Stage::Ready;
            // Claim and suppression compete under this actual class gate.
            record.stage = Stage::Running;
            drop(guard);
            if was_ready {
                state.ready.remove(job.id());
            }
            state.ready.runnable -= 1;
            self.promote_locked(&mut state, class)
        };
        for group in promoted {
            group.notify_ready();
        }
        #[cfg(feature = "diagnostics")]
        self.trace("job.claimed", job.id(), job.group_id().unwrap_or(0));
        let _context = ContextGuard::enter_owned(control.identity(), class, job.group_id());
        self.settle(job, Decision::Run);
        true
    }

    fn run_job(self: &Arc<Self>, job: &JobHandle) {
        #[cfg(feature = "diagnostics")]
        self.trace("job.backend_entry", job.id(), 0);
        self.claim_and_run(job, false);
    }

    fn decrement_handoff(&self, class: SWExecutionClass) {
        self.class_lock(class).ready.handed_off -= 1;
    }

    fn promote_locked(
        &self,
        state: &mut ClassState,
        class: SWExecutionClass,
    ) -> Vec<Arc<GroupInner>> {
        self.apply_demand_updates(state, class);
        let mut notifications = Vec::new();
        if !state.deferred.is_empty() {
            let mut pending = VecDeque::new();
            while let Some(id) = state.ready.pop() {
                let job = state.records.get(&id).expect("ready record exists");
                if let Some(record) = job.record_lock().as_mut() {
                    record.stage = Stage::DeferredReady;
                }
                state.ready.runnable -= 1;
                pending.push_back(id);
            }
            pending.append(&mut state.deferred);
            state.deferred = pending;
        }
        while state.ready.runnable < self.limits.runnable_for(class) {
            let ordinary = state.deferred.iter().enumerate().find_map(|(index, id)| {
                let record = state.records.get(id)?.record_lock();
                (!record.as_ref()?.resource).then_some((index, *id))
            });
            let resource = state
                .deferred
                .iter()
                .enumerate()
                .filter_map(|(index, id)| {
                    let record = state.records.get(id)?.record_lock();
                    let record = record.as_ref()?;
                    if !record.resource {
                        return None;
                    }
                    let selection = record.selection?;
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
            let Some(id) = index.and_then(|index| state.deferred.remove(index)) else {
                break;
            };
            let job = state.records.get(&id).expect("deferred record exists");
            let mut record = job.record_lock();
            let record = record.as_mut().expect("deferred record remains live");
            if record.stage != Stage::DeferredReady {
                continue;
            }
            record.stage = Stage::Ready;
            if let Some(group) = &record.group {
                notifications.push(Arc::clone(group));
            }
            // Borrow fields directly; no owning job handle is dropped under gate.
            if record.resource {
                state
                    .ready
                    .push_resource(id, record.selection.expect("attached resource"));
            } else {
                state.ready.push(id);
            }
        }
        notifications
    }

    fn cleanup_context(&self, job: &JobHandle) -> Option<ContextGuard> {
        let control = self.control.upgrade()?;
        let (class, group) = (job.class(), job.group_id());
        match context::current() {
            None => Some(ContextGuard::enter_owned(control.identity(), class, group)),
            Some(current)
                if current.runtime == control.identity()
                    && current.class == class
                    && group.is_some() =>
            {
                Some(ContextGuard::enter_owned(control.identity(), class, group))
            }
            Some(_) => None,
        }
    }

    fn settle(self: &Arc<Self>, job: &JobHandle, decision: Decision) {
        let Some(envelope) = job.take_envelope() else {
            return;
        };
        #[cfg(feature = "diagnostics")]
        let _trace_job = self.trace_job(job);
        let _context = self.cleanup_context(job);
        #[cfg(feature = "diagnostics")]
        self.trace(
            if matches!(&decision, Decision::Run) {
                "job.invoke"
            } else {
                "job.suppress"
            },
            job.id(),
            0,
        );
        let finish = match catch_unwind(AssertUnwindSafe(|| envelope(decision))) {
            Ok(finish) => finish,
            Err(payload) => {
                crate::cleanup::discard_panic(payload);
                Box::new(|| {})
            }
        };
        #[cfg(feature = "diagnostics")]
        self.trace("job.body_returned", job.id(), 0);
        {
            let _notification = self.wake.notification_scope();
            if let Some(record) = job.record_lock().as_mut() {
                record.stage = Stage::Finalizing;
            }
        }
        self.finish_record(job, finish);
    }

    fn finish_record(self: &Arc<Self>, job: &JobHandle, finish: Finish) {
        #[cfg(feature = "diagnostics")]
        let _trace_job = match crate::diagnostics::SWTrace::current_job() {
            Some(current) if current.runtime == self.trace_runtime && current.id == job.id() => {
                None
            }
            _ => Some(self.trace_job(job)),
        };
        let _context = self.cleanup_context(job);
        let mut subscriptions = {
            let _notification = self.wake.notification_scope();
            let mut record = job.record_lock();
            let Some(record) = record.as_mut() else {
                return;
            };
            if record.attaching {
                record.deferred_finish = Some(finish);
                return;
            }
            std::mem::take(&mut record.subscriptions)
        };
        subscriptions.clear();
        self.subscriptions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .release(subscriptions);
        #[cfg(feature = "diagnostics")]
        self.trace("job.result_publish.begin", job.id(), 0);
        if let Err(payload) = catch_unwind(AssertUnwindSafe(finish)) {
            crate::cleanup::discard_panic(payload);
        }
        #[cfg(feature = "diagnostics")]
        self.trace("job.result_publish.end", job.id(), 0);
        enqueue_activation(Activation::Finalize(Arc::clone(self), job.clone()));
    }

    fn finalize_record(self: &Arc<Self>, job: JobHandle) {
        let record = {
            let _notification = self.wake.notification_scope();
            job.record_lock().take()
        };
        let Some(record) = record else {
            return;
        };
        let removed = self.class_lock(job.class()).records.remove(&job.id());
        drop(removed);
        self.remove_demand(job.id());
        if let Some(group) = &record.group {
            group.retire_member(job.id());
        }
        // Strong settlement remains capacity -> work set -> group -> runtime.
        drop(record.capacity);
        drop(record.work_set);
        drop(record.charge);
        if let Some(group) = record.group {
            let status = record
                .completion
                .expect("group member retains its completion")
                .status()
                .expect("settled member outcome");
            #[cfg(feature = "diagnostics")]
            self.trace("job.group_finish", job.id(), group.id);
            group.finish(Some(status));
        }
        drop(record.admission);
        self.dispatch_class(job.class());
    }

    pub(crate) fn help_group(self: &Arc<Self>, group: &Arc<GroupInner>) -> bool {
        let mut after = 0;
        while let Some(job) = group.member_after(after) {
            after = job.id();
            if self.claim_and_run(&job, true) {
                return true;
            }
        }
        false
    }

    pub(crate) fn abandon(self: &Arc<Self>) {
        // Size the snapshot from live charges, never the configured ceiling. No
        // allocation, backend operation, callback or final handle drop occurs
        // under the rare fixed Low/Mid/High/external cut.
        let mut jobs = Vec::new();
        let mut external_ids = Vec::new();
        let mut required = self.accounting.records.load(Ordering::Acquire);
        loop {
            jobs.reserve(required);
            external_ids.reserve(required);
            let _notification = self.wake.notification_scope();
            let low = self.classes[0]
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let mid = self.classes[1]
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let high = self.classes[2]
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let external = self
                .external
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let owned_count = low.records.len() + mid.records.len() + high.records.len();
            if owned_count > jobs.capacity() || external.len() > external_ids.capacity() {
                required = owned_count.max(external.len());
                // No claim cut has been published. Releasing all guards before
                // retrying permits the next allocation and admission callbacks.
                continue;
            }
            self.abandoned.store(true, Ordering::Release);
            for state in [&*low, &*mid, &*high] {
                jobs.extend(state.records.values().cloned());
            }
            external_ids.extend(external.keys().copied());
            break;
        }
        self.wake.notify();
        for job in jobs {
            self.suppress(&job, SWTaskStatus::Abandoned);
        }
        for id in external_ids {
            self.settle_external(id, SWTaskStatus::Abandoned);
        }
    }

    fn dispatch_class(self: &Arc<Self>, class: SWExecutionClass) {
        self.service_demand_chunk(32);
        self.dispatch_class_ready(class);
    }

    fn dispatch_class_ready(self: &Arc<Self>, class: SWExecutionClass) {
        let Some(control) = self.control.upgrade() else {
            return;
        };
        loop {
            let (job, promoted) = {
                let mut state = self.class_lock(class);
                if self.abandoned.load(Ordering::Acquire) {
                    break;
                }
                let promoted = self.promote_locked(&mut state, class);
                if state.ready.handed_off >= self.limits.handoff_for(class) {
                    drop(state);
                    for group in promoted {
                        group.notify_ready();
                    }
                    break;
                }
                let Some(id) = state.ready.pop() else {
                    drop(state);
                    for group in promoted {
                        group.notify_ready();
                    }
                    break;
                };
                let job = state
                    .records
                    .get(&id)
                    .expect("ready record remains indexed");
                {
                    let mut record = job.record_lock();
                    let record = record.as_mut().expect("ready record is live");
                    debug_assert!(record.stage == Stage::Ready);
                    record.stage = Stage::Handed;
                }
                #[cfg(feature = "diagnostics")]
                self.trace("job.handed", id, job.group_id().unwrap_or(0));
                let job = job.clone();
                state.ready.handed_off += 1;
                (job, promoted)
            };
            for group in promoted {
                group.notify_ready();
            }
            let handoff = Handoff {
                scheduler: Arc::downgrade(self),
                class,
                completed: false,
            };
            let Ok(lease) = control.acquire_handoff(class) else {
                drop(handoff);
                self.abandon();
                break;
            };
            let offered = lease.pool().try_spawn_owned(move || handoff.run(job));
            if let Err(wrapper) = offered {
                drop(wrapper);
                self.abandon();
                break;
            }
        }
    }

    fn dispatch_ready(self: &Arc<Self>) {
        for class in SWExecutionClass::ALL {
            self.dispatch_class_ready(class);
        }
    }
    pub(crate) fn attach_demand(
        self: &Arc<Self>,
        id: u64,
        priority: SWPriority,
    ) -> Result<SWDemand, SWDemandError> {
        let lease = {
            let mut demand = self.domain_lock(&self.demand, 3);
            if self.abandoned.load(Ordering::Acquire) {
                return Err(SWDemandError::Closed);
            }
            let lease = demand.graph.attach(id, priority)?;
            self.demand_pending.store(true, Ordering::Release);
            lease
        };
        let weak = Arc::downgrade(self);
        let demand = SWDemand::new(
            lease,
            Arc::new(move |id, command| {
                let scheduler = weak.upgrade().ok_or(SWDemandError::Closed)?;
                {
                    let mut demand = scheduler.domain_lock(&scheduler.demand, 3);
                    demand.graph.change(id, command)?;
                    scheduler.demand_pending.store(true, Ordering::Release);
                }
                scheduler.service_demand_automatic();
                Ok(())
            }),
        );
        self.service_demand_automatic();
        Ok(demand)
    }

    fn remove_demand(&self, id: u64) {
        if !self.demand_enabled {
            return;
        }
        let provider = {
            let _notification = self.wake.notification_scope();
            let transfer = self
                .demand_transfer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let (provider, class) = {
                let mut demand = self.domain_lock(&self.demand, 3);
                let class = demand.targets.remove(&id);
                let provider = demand.graph.remove(id);
                self.demand_pending
                    .store(demand.graph.pending_updates(), Ordering::Release);
                (provider, class)
            };
            if let Some(class) = class {
                let mut updates = self.demand_updates[class.index()]
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                updates.remove(&id);
                self.demand_updates_pending[class.index()]
                    .store(!updates.is_empty(), Ordering::Release);
            }
            drop(transfer);
            provider
        };
        crate::cleanup::discard_value(provider);
    }

    fn apply_demand_updates(&self, state: &mut ClassState, class: SWExecutionClass) {
        if !self.demand_updates_pending[class.index()].load(Ordering::Acquire) {
            return;
        }
        let updates = {
            let mut mailbox = self.demand_updates[class.index()]
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let updates = mailbox.drain().collect::<Vec<_>>();
            // Publish the clear before releasing the mailbox, then release it
            // before any record wait. Graph publication cannot wait on records
            // indirectly through a mailbox retained by this class consumer.
            self.demand_updates_pending[class.index()].store(false, Ordering::Release);
            updates
        };
        for (id, selection) in updates {
            let Some(job) = state.records.get(&id) else {
                continue;
            };
            let mut guard = job.record_lock();
            if let Some(record) = guard.as_mut()
                && record
                    .selection
                    .is_none_or(|previous| previous.version < selection.version)
            {
                record.selection = Some(selection);
                if record.stage == Stage::Ready {
                    state.ready.update_resource(id, selection);
                }
            }
        }
    }

    fn service_demand_chunk(&self, budget: usize) {
        if !self.demand_pending.load(Ordering::Acquire) {
            return;
        }
        let mut providers = Vec::new();
        {
            let _notification = self.wake.notification_scope();
            let transfer = self
                .demand_transfer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let changes = {
                let mut demand = self.domain_lock(&self.demand, 3);
                let changes = demand.graph.service(budget);
                let mut updates = Vec::with_capacity(changes.len());
                for change in changes {
                    if let Some(class) = demand.targets.get(&change.id) {
                        updates.push((*class, change.id, change.selection));
                    }
                    if let Some(provider) = change.provider {
                        providers.push(provider);
                    }
                }
                self.provider_callbacks
                    .fetch_add(providers.len(), Ordering::AcqRel);
                updates
            };
            // One coalesced entry per live resource node. Removal serializes
            // with this publication and erases its mailbox entry before credits
            // return. IDs never repeat even when control storage is reused.
            for (class, id, selection) in changes {
                let mut updates = self.demand_updates[class.index()]
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let entry = updates.entry(id).or_insert(selection);
                if entry.version < selection.version {
                    *entry = selection;
                }
                self.demand_updates_pending[class.index()].store(true, Ordering::Release);
            }
            {
                let demand = self.domain_lock(&self.demand, 3);
                self.demand_pending
                    .store(demand.graph.pending_updates(), Ordering::Release);
            }
            drop(transfer);
        }
        let _callbacks = ProviderCallbacks {
            scheduler: self,
            count: providers.len(),
        };
        let _context = context::ControlCallbackGuard::enter();
        for (provider, snapshot) in providers {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| provider(snapshot))) {
                crate::cleanup::discard_panic(payload);
            }
            crate::cleanup::discard_value(provider);
        }
    }

    pub(crate) fn service_demand(self: &Arc<Self>, budget: usize) -> bool {
        self.service_demand_chunk(budget);
        self.dispatch_ready();
        self.demand_pending.load(Ordering::Acquire)
            || self
                .demand_updates_pending
                .iter()
                .any(|pending| pending.load(Ordering::Acquire))
    }

    fn service_demand_automatic(self: &Arc<Self>) {
        self.service_demand_chunk(32);
        self.dispatch_ready();
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
                    crate::cleanup::discard_panic(panic);
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
