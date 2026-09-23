//! Shared startup gate and rollback ownership.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use super::SWBuildError;
use crate::backend::MicropoolBackend;

#[cfg(test)]
#[path = "../../tests/unit/startup.rs"]
mod tests;

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Preparing,
    Started,
    Aborted,
}

struct State {
    phase: Phase,
    ready: usize,
    error: Option<SWBuildError>,
}

pub(super) struct Startup {
    state: Mutex<State>,
    changed: Condvar,
}

impl Startup {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(State {
                phase: Phase::Preparing,
                ready: 0,
                error: None,
            }),
            changed: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // No user code executes while locked. Recover poisoning so rollback
        // can still release the gate during internal unwinding.
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn wait<'a>(&self, state: MutexGuard<'a, State>) -> MutexGuard<'a, State> {
        self.changed
            .wait(state)
            .unwrap_or_else(|error| error.into_inner())
    }

    pub(super) fn is_aborted(&self) -> bool {
        self.lock().phase == Phase::Aborted
    }

    pub(super) fn fail(&self, error: SWBuildError) {
        let mut state = self.lock();
        if state.error.is_none() {
            state.error = Some(error);
        }
        state.phase = Phase::Aborted;
        self.changed.notify_all();
    }

    pub(super) fn abort(&self) {
        self.lock().phase = Phase::Aborted;
        self.changed.notify_all();
    }

    pub(super) fn ready_and_wait(&self) -> bool {
        let mut state = self.lock();
        state.ready += 1;
        self.changed.notify_all();
        while state.phase == Phase::Preparing {
            state = self.wait(state);
        }
        state.phase == Phase::Started
    }

    pub(super) fn start_when_ready(&self, expected: usize) -> bool {
        let mut state = self.lock();
        while state.phase == Phase::Preparing && state.ready != expected {
            state = self.wait(state);
        }
        if state.phase == Phase::Aborted {
            return false;
        }
        state.phase = Phase::Started;
        self.changed.notify_all();
        true
    }

    pub(super) fn take_error(&self) -> SWBuildError {
        // Only the builder takes the error, once, after a recorded failure.
        self.lock()
            .error
            .take()
            .expect("aborted startup has a worker error")
    }
}

pub(super) struct StartupGuard {
    startup: Arc<Startup>,
    pub(super) pools: Vec<MicropoolBackend>,
    committed: bool,
}

impl StartupGuard {
    pub(super) fn new(startup: Arc<Startup>) -> Self {
        Self {
            startup,
            pools: Vec::with_capacity(3),
            committed: false,
        }
    }

    pub(super) fn commit(&mut self) -> Vec<MicropoolBackend> {
        self.committed = true;
        std::mem::take(&mut self.pools)
    }
}

impl Drop for StartupGuard {
    fn drop(&mut self) {
        if !self.committed {
            // Release every gate before any join, including during unwinding.
            self.startup.abort();
            for pool in self.pools.drain(..) {
                pool.stop_and_join();
            }
        }
    }
}
