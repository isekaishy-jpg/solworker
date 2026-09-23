//! Runtime ownership, worker setup, and construction.

pub(crate) mod config;
mod lifecycle;

use std::error::Error;
use std::fmt;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::thread;

use crate::backend::MicropoolBackend;
pub(crate) use crate::execution::context::OwnerCallbackGuard;
use crate::execution::{SWLane, context};
use crate::platform::{SWWorkerSetupError, apply_worker_priority};
use crate::scheduler::{OwnedScheduler, SWOwnedLimits};
use config::{SWExecutionClass, SWRuntimeConfig};
use lifecycle::{BackendOwner, Startup, StartupGuard};
pub(crate) use lifecycle::{OwnedAdmission, OwnerRegistration, RuntimeControl};

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
        }
    }

    /// Enables owned jobs with explicit host-supplied admission capacities.
    /// Without this option the runtime provides scoped execution only.
    pub fn with_owned_limits(mut self, limits: SWOwnedLimits) -> Self {
        self.owned_limits = Some(limits);
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
                                drop(payload);
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
        let control = Arc::new(RuntimeControl::new(&backend));
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
/// retain the pools and settle before returning; use [`Self::shutdown`] to wait.
pub struct SWRuntime {
    config: SWRuntimeConfig,
    backend: Option<Arc<BackendOwner>>,
    control: Arc<RuntimeControl>,
    owned: Option<Arc<OwnedScheduler>>,
}

/// Terminal shutdown cannot block from within a participating invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWShutdownError {
    ExecutionContext,
    /// Close and clean registered owners on their own threads before joining.
    LiveOwners,
    /// Seal or cancel work sets and finish discovery/local cleanup before joining.
    LiveWorkSets,
}

impl fmt::Display for SWShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionContext => f.write_str("shutdown cannot join from an execution context"),
            Self::LiveOwners => f.write_str("close and clean live owners before runtime shutdown"),
            Self::LiveWorkSets => f.write_str("seal and drain work sets before runtime shutdown"),
        }
    }
}

impl Error for SWShutdownError {}

impl SWRuntime {
    /// Reserves a required pipeline without borrowing the ordinary allowance.
    pub fn reserve_required(
        &self,
        cost: crate::scheduler::SWCost,
    ) -> Result<crate::scheduler::SWReservation, crate::scheduler::SWReservationError> {
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

    /// Returns a reusable class handle. A handle remains closed after this
    /// runtime shuts down or is abandoned; it does not retain worker ownership.
    pub fn lane(&self, class: SWExecutionClass) -> SWLane {
        SWLane::new(Arc::clone(&self.control), class)
    }

    /// Stops and joins all workers. May block, including on thread-local
    /// destructors. Closes new root invocations and waits for existing scopes
    /// and owned jobs, including their accepted dependencies and descendants.
    /// Scoped nesting stays on one lane; owned descendants may target another.
    /// A participating caller or worker
    /// receives an error before closure, avoiding self-wait and cross-pool cycles.
    /// Live owners return `LiveOwners` before root closure; the host must pump
    /// or suppress their callbacks and close them on their creation threads.
    /// Repeating this after either terminal operation has no effect.
    pub fn shutdown(&mut self) -> Result<(), SWShutdownError> {
        if context::current().is_some() || context::owner_callback_active() {
            return Err(SWShutdownError::ExecutionContext);
        }
        if self.state() != SWRuntimeState::Running {
            return Ok(());
        }
        self.control.close_and_wait()?;
        if let Some(capacity) = self
            .owned
            .as_ref()
            .and_then(|owned| owned.capacity.as_ref())
        {
            capacity.close();
        }
        if let Some(backend) = self.backend.take() {
            // Lanes hold weak backend references; each active lease releases
            // its strong reference before publishing its departure.
            let backend = Arc::try_unwrap(backend).unwrap_or_else(|_| {
                panic!("quiescent runtime still has a backend execution owner")
            });
            backend.join();
        }
        self.control.set_phase(SWRuntimeState::Stopped);
        Ok(())
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
