//! Private adapter for the pinned micropool executor.

use std::thread::JoinHandle;

use micropool::{ThreadPool, ThreadPoolBuilder};

/// One independently owned worker pool. The runtime controls admission and
/// establishes global quiescence before invoking the joining terminal path.
pub(crate) struct MicropoolBackend {
    pool: ThreadPool,
}

impl MicropoolBackend {
    /// Checked handoff for a scheduler-owned wrapper. Rejection returns the
    /// untouched closure so the scheduler can settle the accepted record.
    pub(crate) fn try_spawn_owned<F>(&self, f: F) -> Result<(), F>
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.try_spawn_owned(f).map(|task| {
            // The private queue retains the callable. SW owns its completion
            // and does not expose micropool's eager-helping task handle.
            drop(task);
        })
    }

    /// Builds workers through a launcher supplied by the runtime. A launcher
    /// that waits on a shared startup gate must release that gate before it
    /// returns an error, since partial-construction rollback joins workers.
    pub(crate) fn try_build_with<E>(
        num_threads: usize,
        spawn: impl FnMut(usize, Box<dyn FnOnce() + Send>) -> Result<JoinHandle<()>, E>,
    ) -> Result<Self, E> {
        ThreadPoolBuilder::default()
            .num_threads(num_threads)
            .idle_spin_cycles(0)
            .try_build_with(spawn)
            .map(|pool| Self { pool })
    }

    /// Installs this lane on an external caller, or reuses its context when a
    /// branch enters another operation on the same lane.
    pub(crate) fn with_context<R>(&self, f: impl FnOnce() -> R) -> R {
        self.pool.with_pool_context(f)
    }

    /// Runs two transferable borrowed branches to completion.
    pub(crate) fn join<L, R, LO, RO>(&self, left: L, right: R) -> (LO, RO)
    where
        L: FnOnce() -> LO + Send,
        R: FnOnce() -> RO + Send,
        LO: Send,
        RO: Send,
    {
        self.pool.join(left, right)
    }

    /// Runs indexed borrowed work. Each index is invoked exactly once, and all
    /// invocations finish before return.
    pub(crate) fn invoke_indexed(&self, count: usize, f: impl Fn(usize) + Sync) {
        self.pool.invoke_indexed(count, f);
    }

    /// Publishes one worker branch before running the owner branch on this
    /// caller. The caller may help related work after its branch finishes.
    pub(crate) fn join_with_owner<W, O, WO, OO>(&self, worker: W, owner: O) -> (WO, OO)
    where
        W: FnOnce() -> WO + Send,
        O: FnOnce() -> OO,
        WO: Send,
    {
        self.pool.join_with_owner(worker, owner)
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
