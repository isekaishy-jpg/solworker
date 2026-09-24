//! Runtime ownership, worker setup, and construction.

pub(crate) mod config;
mod lifecycle;

use std::error::Error;
use std::fmt;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::backend::MicropoolBackend;
pub(crate) use crate::execution::context::OwnerCallbackGuard;
use crate::execution::{SWLane, context};
use crate::platform::{SWWorkerSetupError, apply_worker_priority};
use crate::scheduler::{OwnedScheduler, SWOwnedLimits};
use config::{SWExecutionClass, SWRuntimeConfig};
use lifecycle::{BackendOwner, Startup, StartupGuard};
pub(crate) use lifecycle::{
    ExternalAdmission, OwnedAdmission, OwnerRegistration, OwnerRootAdmission, RuntimeControl,
};

type WorkerSetup = dyn Fn(SWExecutionClass, usize) -> io::Result<()> + Send + Sync;

/// A worker could not be created or prepared. Construction joins all workers
/// already created before returning the error.
#[derive(Debug)]
pub enum SWBuildError {
    Capacity(crate::scheduler::reservation::SWLimitError),
    OwnedWorkDisabled,
    Spawn {
        class: SWExecutionClass,
        worker: usize,
        source: io::Error,
    },
    Setup {
        class: SWExecutionClass,
        worker: usize,
        source: SWWorkerSetupError,
    },
    Hook {
        class: SWExecutionClass,
        worker: usize,
        source: io::Error,
    },
    SetupPanicked {
        class: SWExecutionClass,
        worker: usize,
    },
}

impl fmt::Display for SWBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity(error) => write!(f, "invalid capacity policy: {error}"),
            Self::OwnedWorkDisabled => {
                f.write_str("capacity and demand configuration require owned work limits")
            }
            Self::Spawn {
                class,
                worker,
                source,
            } => write!(f, "could not spawn {class:?} worker {worker}: {source}"),
            Self::Setup {
                class,
                worker,
                source,
            } => write!(f, "could not configure {class:?} worker {worker}: {source}"),
            Self::Hook {
                class,
                worker,
                source,
            } => write!(
                f,
                "setup hook failed for {class:?} worker {worker}: {source}"
            ),
            Self::SetupPanicked { class, worker } => {
                write!(f, "setup hook panicked for {class:?} worker {worker}")
            }
        }
    }
}

impl Error for SWBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Capacity(error) => Some(error),
            Self::OwnedWorkDisabled => None,
            Self::Spawn { source, .. } | Self::Hook { source, .. } => Some(source),
            Self::Setup { source, .. } => Some(source),
            Self::SetupPanicked { .. } => None,
        }
    }
}

/// Builder for three dedicated worker pools under a validated host budget.
pub struct SWRuntimeBuilder {
    config: SWRuntimeConfig,
    setup: Option<Arc<WorkerSetup>>,
    owned_limits: Option<SWOwnedLimits>,
    capacity_limits: Option<crate::scheduler::SWLimits>,
    demand_priorities: Vec<crate::scheduler::SWPriority>,
    demand_leases: usize,
    external_capacity: Option<std::num::NonZeroUsize>,
    notification_limits: Option<crate::notification::SWNotifyLimits>,
}

impl SWRuntimeBuilder {
    pub fn new(config: SWRuntimeConfig) -> Self {
        Self {
            config,
            setup: None,
            owned_limits: None,
            capacity_limits: None,
            demand_priorities: Vec::new(),
            demand_leases: 0,
            external_capacity: None,
            notification_limits: None,
        }
    }

    /// Enables owned jobs with explicit host-supplied admission capacities.
    /// Without this option the runtime provides scoped execution only.
    pub fn with_owned_limits(mut self, limits: SWOwnedLimits) -> Self {
        self.owned_limits = Some(limits);
        self
    }

    /// Enables bounded, optional host notification without creating threads.
    /// Internal worker and group wakes remain independent of host adapters.
    pub fn with_notification_limits(mut self, limits: crate::notification::SWNotifyLimits) -> Self {
        self.notification_limits = Some(limits);
        self
    }

    /// Enables physical provider-access registration. This bounds ordinary
    /// accesses independently of CPU jobs. With capacity policy enabled,
    /// required accesses use additional protected slots bounded by that policy's
    /// required record allowance, as well as their reserved record credits.
    pub fn with_external_capacity(mut self, capacity: std::num::NonZeroUsize) -> Self {
        self.external_capacity = Some(capacity);
        self
    }

    /// Enables declared-cost admission with a protected required-work allowance.
    pub fn with_capacity_limits(mut self, limits: crate::scheduler::SWLimits) -> Self {
        self.capacity_limits = Some(limits);
        self
    }

    /// Fixes resource demand bands and bounds independent consumer leases for
    /// this runtime. Ordinary CPU work retains its FIFO route.
    pub fn with_demand_limits(
        mut self,
        priorities: Vec<crate::scheduler::SWPriority>,
        max_leases: usize,
    ) -> Self {
        self.demand_priorities = priorities;
        self.demand_leases = max_leases;
        self
    }

    /// Installs a hook called once on each worker, after platform setup and
    /// before any worker enters its executor. Indices are local to each class.
    ///
    /// Hooks can run concurrently. They must return without depending on this
    /// runtime or on another worker's hook being called. Build waits for every
    /// hook, including during rollback; setup panics become build errors when
    /// unwinding is enabled. Thread-local state may be installed by the hook.
    pub fn with_worker_setup<F>(mut self, setup: F) -> Self
    where
        F: Fn(SWExecutionClass, usize) -> io::Result<()> + Send + Sync + 'static,
    {
        self.setup = Some(Arc::new(setup));
        self
    }

    /// Starts all classes atomically with respect to executor entry. Returns
    /// only after every worker has completed setup. Failure releases the startup
    /// gate and joins every created worker; no partial runtime is returned.
    pub fn build(self) -> Result<SWRuntime, SWBuildError> {
        self.build_with(Arc::new(Startup::new()), |_, _, builder, worker| {
            builder.spawn(worker)
        })
    }

    // Keep the OS launch boundary injectable without changing the public API.
    // Both real and injected spawn errors pass through the same gate/rollback path.
    fn build_with(
        self,
        startup: Arc<Startup>,
        mut spawn: impl FnMut(
            SWExecutionClass,
            usize,
            thread::Builder,
            Box<dyn FnOnce() + Send>,
        ) -> io::Result<thread::JoinHandle<()>>,
    ) -> Result<SWRuntime, SWBuildError> {
        if self.owned_limits.is_none()
            && (self.capacity_limits.is_some() || !self.demand_priorities.is_empty())
        {
            return Err(SWBuildError::OwnedWorkDisabled);
        }
        if let Some(limits) = self.capacity_limits {
            crate::scheduler::SWLimits::new(
                limits.ordinary_target,
                limits.required_allowance,
                limits.required_pipelines,
                limits.hard_byte_ceiling,
            )
            .map_err(SWBuildError::Capacity)?;
        }
        let mut guard = StartupGuard::new(Arc::clone(&startup));
        let mut expected = 0;
        for class in SWExecutionClass::ALL {
            let config = self.config.workers_for(class);
            expected += config.worker_count(); // Validated checked sum.
            let pool = MicropoolBackend::try_build_with(config.worker_count(), |worker, run| {
                if startup.is_aborted() {
                    return Err(());
                }
                let worker_startup = Arc::clone(&startup);
                let setup = self.setup.clone();
                let builder = thread::Builder::new().name(format!("sw-{class:?}-{worker}"));
                let spawned = spawn(
                    class,
                    worker,
                    builder,
                    Box::new(move || {
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            apply_worker_priority(config.requested_priority()).map_err(
                                |source| SWBuildError::Setup {
                                    class,
                                    worker,
                                    source,
                                },
                            )?;
                            if let Some(setup) = setup.as_ref() {
                                setup(class, worker).map_err(|source| SWBuildError::Hook {
                                    class,
                                    worker,
                                    source,
                                })?;
                            }
                            Ok(())
                        }));
                        // Setup captures belong to construction, not the pool
                        // lifetime. Thread-local state installed by the hook
                        // remains owned by the worker itself.
                        drop(setup);
                        match result {
                            Ok(Ok(())) => {
                                if worker_startup.ready_and_wait() {
                                    run();
                                }
                            }
                            Ok(Err(error)) => worker_startup.fail(error),
                            Err(payload) => {
                                // Release other workers even if a panic payload
                                // itself has a panicking destructor.
                                worker_startup.fail(SWBuildError::SetupPanicked { class, worker });
                                crate::cleanup::discard_panic(payload);
                            }
                        }
                    }),
                );
                spawned.map_err(|source| {
                    // The backend joins partial workers on launcher failure:
                    // open the abort gate BEFORE returning its error.
                    startup.fail(SWBuildError::Spawn {
                        class,
                        worker,
                        source,
                    });
                })
            });
            match pool {
                Ok(pool) => guard.pools.push(pool),
                Err(()) => return Err(startup.take_error()),
            }
        }
        if !startup.start_when_ready(expected) {
            return Err(startup.take_error());
        }
        let backend = Arc::new(BackendOwner::new(guard.commit()));
        let physical = crate::external::PhysicalRegistry::new(
            self.external_capacity,
            self.capacity_limits
                .map_or(0, |limits| limits.required_allowance.records),
        );
        let control = Arc::new(RuntimeControl::new_with_notifications(
            &backend,
            Arc::clone(&physical),
            self.notification_limits,
        ));
        let owned = self.owned_limits.map(|mut limits| {
            let capacity = self.capacity_limits.map(|policy| {
                limits.records = limits
                    .records
                    .max(policy.ordinary_target.records + policy.required_allowance.records);
                limits.edges = limits
                    .edges
                    .max(policy.ordinary_target.edges + policy.required_allowance.edges);
                crate::scheduler::reservation::SWReservationPool::new(control.identity(), policy)
            });
            let scheduler = OwnedScheduler::new(
                Arc::downgrade(&control),
                limits,
                capacity,
                self.demand_priorities,
                self.demand_leases,
            );
            control.install_owned(&scheduler);
            scheduler
        });
        Ok(SWRuntime {
            config: self.config,
            backend: Some(backend),
            control,
            owned,
            physical,
        })
    }
}

/// Observable state of the worker foundation. Terminal states cannot reopen.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWRuntimeState {
    Running,
    Closing,
    Stopped,
    Abandoned,
}

/// Owns dedicated Low, Mid, and High pools. Dropping a runtime closes new
/// invocations and requests worker stop without joining. Active borrowed calls
/// retain the pools and settle before returning. [`Self::begin_shutdown`] and
/// [`Self::try_shutdown`] let a host service owners and providers while draining.
pub struct SWRuntime {
    config: SWRuntimeConfig,
    backend: Option<Arc<BackendOwner>>,
    control: Arc<RuntimeControl>,
    owned: Option<Arc<OwnedScheduler>>,
    physical: Arc<crate::external::PhysicalRegistry>,
}

/// Terminal shutdown cannot block from within a participating invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWShutdownError {
    ExecutionContext,
    /// Close and clean registered owners on their own threads before joining.
    LiveOwners,
    /// Seal or cancel work sets and finish discovery/local cleanup before joining.
    LiveWorkSets,
    /// Finish logical producers and acknowledge physical release before joining.
    LiveExternal,
    /// Service bounded demand/control updates before attempting the join.
    PendingControl,
}

impl fmt::Display for SWShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionContext => f.write_str("shutdown cannot join from an execution context"),
            Self::LiveOwners => f.write_str("close and clean live owners before runtime shutdown"),
            Self::LiveWorkSets => f.write_str("seal and drain work sets before runtime shutdown"),
            Self::LiveExternal => f.write_str(
                "settle external outcomes and physical accesses before runtime shutdown",
            ),
            Self::PendingControl => {
                f.write_str("service pending demand updates before runtime shutdown")
            }
        }
    }
}

impl Error for SWShutdownError {}

impl SWRuntime {
    /// Binds a reusable host signal destination. The adapter runs synchronously
    /// on publishers, outside bookkeeping locks; it must only signal the host.
    /// Rejection returns the uninvoked closure and its captures.
    pub fn notification_route<F>(
        &self,
        signal: F,
    ) -> Result<crate::notification::SWNotifyRoute, crate::notification::SWNotifyRejected<F>>
    where
        F: Fn() -> io::Result<()> + Send + Sync + 'static,
    {
        match self.control.notification_domain() {
            Some(domain) => domain.create_route(signal),
            None => Err(crate::notification::SWNotifyRejected {
                reason: crate::notification::SWNotifyError::Disabled,
                signal,
            }),
        }
    }

    /// Admits a provider-owned logical result without occupying a CPU worker.
    /// Provider submission occurs only after successful admission. Dropping the
    /// producer abandons its logical result, never a separate physical access.
    pub fn external<'a, T: Send + 'static>(
        &self,
        options: crate::external::SWExternalOptions<'a>,
    ) -> crate::external::SWExternalResult<'a, T> {
        match self.control.owned_scheduler() {
            Ok(scheduler) => scheduler.admit_external(&self.control, options),
            Err(reason) => Err(crate::external::SWExternalRejected { reason, options }),
        }
    }

    /// Reserves storage before foreign access starts. The prepared value is safe
    /// to reclaim; activation separately commits access against closure and set
    /// cancellation. No I/O, GPU submission or foreign code runs in admission.
    pub fn prepare_external<'a, T: Send + 'static>(
        &self,
        resource: T,
        options: crate::external::SWExternalAccessOptions<'a>,
    ) -> Result<
        crate::external::SWExternalPrepared<T>,
        crate::external::SWExternalAccessRejected<'a, T>,
    > {
        let prepared = (|| {
            use crate::scheduler::{SWCost, SWSpawnError};
            if (options.work_set.is_some() && options.discovery.is_some())
                || options.cost.edges != 0
                || options.cost.deliveries != 0
            {
                return Err(SWSpawnError::InvalidContext);
            }
            let work_set = match (options.work_set, options.discovery) {
                (Some(set), _) => Some(set.try_root_lease(self.control.identity())),
                (_, Some(permit)) => Some(permit.try_child_lease(self.control.identity())),
                _ => None,
            }
            .transpose()
            .map_err(|_| SWSpawnError::Closed)?;
            let cost = SWCost::new(options.cost.records.max(1), 0, 0, options.cost.bytes);
            let capacity = if let Some(reserve) = options.reservation {
                if reserve.runtime_identity() != self.control.identity() {
                    return Err(SWSpawnError::InvalidReservation);
                }
                Some(
                    reserve
                        .stage(cost)
                        .map_err(crate::scheduler::map_reservation_error)?,
                )
            } else if let Some(pool) = self
                .owned
                .as_ref()
                .and_then(|owned| owned.capacity.as_ref())
            {
                Some(
                    pool.try_reserve_ordinary(cost)
                        .map_err(crate::scheduler::map_reservation_error)?,
                )
            } else {
                if cost.bytes != 0 {
                    return Err(SWSpawnError::Disabled);
                }
                None
            };
            let bytes = if cost.bytes == 0 {
                None
            } else {
                Some(
                    capacity
                        .as_ref()
                        .expect("charged bytes require capacity")
                        .retain_bytes(cost.bytes)
                        .map_err(crate::scheduler::map_reservation_error)?,
                )
            };
            let required = options
                .reservation
                .is_some_and(|reserve| reserve.is_required());
            let id = self
                .control
                .reserve_physical(required, work_set.is_some())?;
            let retention = crate::external::PhysicalRetention {
                control: Arc::clone(&self.control),
                registry: Arc::clone(&self.physical),
                id,
                work_set,
                capacity,
            };
            Ok((bytes, retention))
        })();
        match prepared {
            Ok((bytes, retention)) => Ok(crate::external::SWExternalPrepared::new(
                resource, bytes, retention,
            )),
            Err(reason) => Err(crate::external::SWExternalAccessRejected {
                reason,
                resource,
                options,
            }),
        }
    }

    /// Snapshot only; does not poll providers or infer device completion.
    pub fn external_progress(&self) -> crate::external::SWExternalProgress {
        self.control.external_progress()
    }

    /// Reserves a required pipeline without borrowing the ordinary allowance.
    pub fn reserve_required(
        &self,
        cost: crate::scheduler::SWCost,
    ) -> Result<crate::scheduler::SWReservation, crate::scheduler::SWReservationError> {
        if self.state() != SWRuntimeState::Running {
            return Err(crate::scheduler::SWReservationError::Closed);
        }
        self.owned
            .as_ref()
            .and_then(|owned| owned.capacity.as_ref())
            .ok_or(crate::scheduler::SWReservationError::Closed)?
            .try_reserve_required(cost)
    }

    pub fn reserve_ordinary(
        &self,
        cost: crate::scheduler::SWCost,
    ) -> Result<crate::scheduler::SWReservation, crate::scheduler::SWReservationError> {
        if self.state() != SWRuntimeState::Running {
            return Err(crate::scheduler::SWReservationError::Closed);
        }
        self.owned
            .as_ref()
            .and_then(|owned| owned.capacity.as_ref())
            .ok_or(crate::scheduler::SWReservationError::Closed)?
            .try_reserve_ordinary(cost)
    }

    pub fn capacity_usage(&self) -> Option<crate::scheduler::SWCapacityUsage> {
        self.owned
            .as_ref()?
            .capacity
            .as_ref()
            .map(|capacity| capacity.snapshot())
    }

    /// Processes a bounded number of demand nodes without running user jobs.
    /// Returns true while more control propagation remains serviceable.
    pub fn service_demand(&self, budget: usize) -> bool {
        self.owned
            .as_ref()
            .is_some_and(|owned| owned.service_demand(budget))
    }
    /// Creates a lifetime boundary with a bounded number of discovery permits.
    /// Retained results may outlive this set after its producers and consumers settle.
    pub fn work_set(
        &self,
        capacity: std::num::NonZeroUsize,
    ) -> Result<crate::scheduler::SWWorkSet, crate::scheduler::SWSpawnError> {
        self.control.work_set(capacity)
    }
    /// Registers thread-bound state and bounded deferred delivery on this host
    /// thread. Close/clean owners before joining the runtime. O need not be Send.
    pub fn owner<O>(
        &self,
        state: O,
        capacity: std::num::NonZeroUsize,
    ) -> Result<crate::owner::SWOwner<O>, crate::owner::SWOwnerError> {
        crate::owner::SWOwner::new(Arc::clone(&self.control), state, capacity)
    }

    pub fn builder(config: SWRuntimeConfig) -> SWRuntimeBuilder {
        SWRuntimeBuilder::new(config)
    }

    pub fn config(&self) -> &SWRuntimeConfig {
        &self.config
    }

    pub fn state(&self) -> SWRuntimeState {
        self.control.phase()
    }

    /// Passive snapshot of CPU, owner, work-set and physical blockers. The
    /// generation can be used to await a change and then sample again.
    pub fn progress(&self) -> crate::progress::SWProgress {
        let wake = self.control.wake();
        let generation = wake.generation();
        crate::progress::SWProgress::new(
            self.state(),
            self.owned
                .as_ref()
                .map_or_else(Default::default, |owned| owned.progress()),
            self.capacity_usage(),
            self.control.owner_progress(),
            self.control.work_sets_progress(),
            self.external_progress(),
            self.control.active_leases(),
            wake,
            generation,
        )
    }

    /// Returns a reusable class handle. A handle remains closed after this
    /// runtime shuts down or is abandoned; it does not retain worker ownership.
    pub fn lane(&self, class: SWExecutionClass) -> SWLane {
        SWLane::new(Arc::clone(&self.control), class)
    }

    /// Closes root admission and seals every work set. Accounted descendants,
    /// owner callbacks and provider release may continue. This never waits for
    /// worker work, owner phases or foreign completion, and may be called from
    /// a participating callback to request closure.
    pub fn begin_shutdown(&self) {
        self.control.begin_shutdown();
    }

    /// Joins the workers once accepted work and host services have settled.
    /// Returns `Ok(false)` while the host must continue owner/provider service.
    /// A successful join returns `Ok(true)`. Abandonment cannot be upgraded
    /// to a join and returns `Ok(false)`.
    /// Worker termination and thread-local cleanup can still block after the
    /// quiescence check, so a participating execution context cannot call this.
    pub fn try_shutdown(&mut self) -> Result<bool, SWShutdownError> {
        if context::current().is_some()
            || context::owner_callback_active()
            || context::control_callback_active()
        {
            return Err(SWShutdownError::ExecutionContext);
        }
        if self.state() == SWRuntimeState::Stopped {
            return Ok(true);
        }
        if self.state() == SWRuntimeState::Abandoned {
            return Ok(false);
        }
        self.begin_shutdown();
        // Establish that no counted ancestor can admit a new descendant before
        // checking scheduler tails, then recheck runtime accounting. Sampling
        // the scheduler first could miss a child created by a retiring scope.
        if !self.control.is_quiescent()
            || !self
                .owned
                .as_ref()
                .is_none_or(|scheduler| scheduler.quiescent())
            || !self.control.is_quiescent()
        {
            return Ok(false);
        }
        if let Some(capacity) = self
            .owned
            .as_ref()
            .and_then(|owned| owned.capacity.as_ref())
        {
            capacity.close();
        }
        if let Some(backend) = self.backend.take() {
            let backend = Arc::try_unwrap(backend).unwrap_or_else(|_| {
                panic!("quiescent runtime still has a backend execution owner")
            });
            backend.join();
        }
        self.control.set_phase(SWRuntimeState::Stopped);
        Ok(true)
    }

    /// Convenience join for workloads that need no ongoing host service. May
    /// block on scopes, owned jobs and worker thread-local destructors. Live
    /// owners, work sets, providers, physical accesses and demand updates return a blocker so
    /// the host can service them; if one appears during closing, roots stay
    /// closed and a later call can finish the join. Participating callers and
    /// callbacks receive an error before closure. For a host-driven drain,
    /// call [`Self::begin_shutdown`] and poll [`Self::try_shutdown`].
    pub fn shutdown(&mut self) -> Result<(), SWShutdownError> {
        if context::current().is_some()
            || context::owner_callback_active()
            || context::control_callback_active()
        {
            return Err(SWShutdownError::ExecutionContext);
        }
        if matches!(
            self.state(),
            SWRuntimeState::Stopped | SWRuntimeState::Abandoned
        ) {
            return Ok(());
        }
        if self.state() == SWRuntimeState::Running {
            self.control.prepare_shutdown()?;
        }
        loop {
            let wake = self.control.wake();
            let generation = wake.generation();
            if self.try_shutdown()? {
                return Ok(());
            }
            if let Some(blocker) = self.control.shutdown_blocker() {
                return Err(blocker);
            }
            if self.owned.as_ref().is_some_and(|owned| {
                let control = owned.progress();
                control.demand_pending || control.provider_callbacks != 0
            }) {
                return Err(SWShutdownError::PendingControl);
            }
            // A completion can publish its result before its final scheduler
            // bookkeeping and dispatch lease settle. Wait for that counted
            // work to leave without busy spinning or servicing host callbacks.
            wake.wait_since(generation, Instant::now() + Duration::from_secs(1));
        }
    }

    /// Suppresses unclaimed owned jobs and requests stop without joining.
    /// Their transferable captures are cleaned up on the abandoning caller;
    /// cleanup can take time. Claimed jobs and worker termination may finish
    /// after this returns. This cannot be upgraded to joining shutdown.
    pub fn abandon(&mut self) {
        if matches!(
            self.state(),
            SWRuntimeState::Stopped | SWRuntimeState::Abandoned
        ) {
            return;
        }
        self.control.set_phase(SWRuntimeState::Abandoned);
        if let Some(capacity) = self
            .owned
            .as_ref()
            .and_then(|owned| owned.capacity.as_ref())
        {
            capacity.close();
        }
        self.control.close_owner_routes();
        if let Some(owned) = &self.owned {
            owned.abandon();
        }
        // Preserve the runtime terminal reason before set cancellation invokes
        // producer controls. Discovery and consumer delivery still close below.
        self.control.cancel_work_sets();
        if let Some(backend) = self.backend.take() {
            backend.begin_stop();
            drop(backend);
        }
    }
}

impl Drop for SWRuntime {
    fn drop(&mut self) {
        self.abandon();
    }
}
