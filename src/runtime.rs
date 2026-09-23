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
use crate::platform::{SWWorkerSetupError, apply_worker_priority};
use config::{SWExecutionClass, SWRuntimeConfig};
use lifecycle::{Startup, StartupGuard};

type WorkerSetup = dyn Fn(SWExecutionClass, usize) -> io::Result<()> + Send + Sync;

/// A worker could not be created or prepared. Construction joins all workers
/// already created before returning the error.
#[derive(Debug)]
pub enum SWBuildError {
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
}

impl SWRuntimeBuilder {
    pub fn new(config: SWRuntimeConfig) -> Self {
        Self {
            config,
            setup: None,
        }
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
        Ok(SWRuntime {
            config: self.config,
            pools: guard.commit(),
            state: SWRuntimeState::Running,
        })
    }
}

/// Observable state of the worker foundation. Terminal states cannot reopen.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWRuntimeState {
    Running,
    Stopped,
    Abandoned,
}

/// Owns dedicated Low, Mid, and High pools. No work-submission API is exposed
/// by this foundation yet. Dropping a runtime requests stop and detaches its
/// workers; use [`Self::shutdown`] to wait for worker termination.
pub struct SWRuntime {
    config: SWRuntimeConfig,
    pools: Vec<MicropoolBackend>,
    state: SWRuntimeState,
}

impl SWRuntime {
    pub fn builder(config: SWRuntimeConfig) -> SWRuntimeBuilder {
        SWRuntimeBuilder::new(config)
    }

    pub fn config(&self) -> &SWRuntimeConfig {
        &self.config
    }

    pub fn state(&self) -> SWRuntimeState {
        self.state
    }

    /// Stops and joins all workers. May block, including on thread-local
    /// destructors. Call from the host outside worker execution. The current
    /// foundation exposes no submissions, so its executors are quiescent.
    /// Repeating this after either terminal operation has no effect.
    pub fn shutdown(&mut self) {
        if self.state != SWRuntimeState::Running {
            return;
        }
        // Worker-local cleanup may depend on another class's worker exiting.
        // Wake every pool before any join can block on that cleanup.
        for pool in &self.pools {
            pool.begin_stop();
        }
        for pool in self.pools.drain(..) {
            pool.stop_and_join();
        }
        self.state = SWRuntimeState::Stopped;
    }

    /// Requests stop without joining. Worker termination may occur after this
    /// returns. This cannot subsequently be upgraded to joining shutdown.
    pub fn abandon(&mut self) {
        if self.state != SWRuntimeState::Running {
            return;
        }
        self.state = SWRuntimeState::Abandoned;
        for pool in self.pools.drain(..) {
            pool.stop_and_detach();
        }
    }
}

impl Drop for SWRuntime {
    fn drop(&mut self) {
        self.abandon();
    }
}
