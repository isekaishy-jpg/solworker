//! Private adapter for the pinned micropool executor.

use std::thread::JoinHandle;

use micropool::{ThreadPool, ThreadPoolBuilder};

/// One independently owned worker pool. The runtime controls admission and
/// establishes global quiescence before invoking the joining terminal path.
pub(crate) struct MicropoolBackend {
    pool: ThreadPool,
}

impl MicropoolBackend {
    /// Builds workers through a launcher supplied by the runtime. A launcher
    /// that waits on a shared startup gate must release that gate before it
    /// returns an error, since partial-construction rollback joins workers.
    pub(crate) fn try_build_with<E>(
        num_threads: usize,
        spawn: impl FnMut(usize, Box<dyn FnOnce() + Send>) -> Result<JoinHandle<()>, E>,
    ) -> Result<Self, E> {
        ThreadPoolBuilder::default()
            .num_threads(num_threads)
            .try_build_with(spawn)
            .map(|pool| Self { pool })
    }

    /// Requests stop and wakes workers without joining. Signal every runtime
    /// pool before joining any, so worker-local cleanup can settle across pools.
    pub(crate) fn begin_stop(&self) {
        self.pool.begin_stop();
    }

    /// Stops and joins workers after runtime-wide quiescence has been proven.
    /// Must be called from outside worker and owner callback execution.
    pub(crate) fn stop_and_join(self) {
        self.pool.stop_and_join();
    }

    /// Stops and detaches workers, discarding only queued owned callables.
    /// Already-claimed owned work may finish. Future borrowed-work integration
    /// must finish its scope before releasing the pool lifetime.
    pub(crate) fn stop_and_detach(self) {
        self.pool.stop_and_detach();
    }
}
