//! Shared startup gate and rollback ownership.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};

use super::{SWBuildError, SWRuntimeState, SWShutdownError};
use crate::SWExecutionClass;
use crate::backend::MicropoolBackend;
use crate::execution::context::{self, SWExecutionError, current};
use crate::scheduler::{OwnedScheduler, SWSpawnError};

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

/// Pool ownership is separate from the bookkeeping retained by lane handles.
/// The last owner always detaches unless the host explicitly joined it.
pub(super) struct BackendOwner {
    pools: Vec<MicropoolBackend>,
}

impl BackendOwner {
    pub(super) fn new(pools: Vec<MicropoolBackend>) -> Self {
        Self { pools }
    }

    pub(super) fn begin_stop(&self) {
        for pool in &self.pools {
            pool.begin_stop();
        }
    }

    pub(super) fn join(mut self) {
        self.begin_stop();
        for pool in self.pools.drain(..) {
            pool.stop_and_join();
        }
    }
}

impl Drop for BackendOwner {
    fn drop(&mut self) {
        self.begin_stop();
        for pool in self.pools.drain(..) {
            pool.stop_and_detach();
        }
    }
}

struct ControlState {
    phase: SWRuntimeState,
    active: usize,
    external: usize,
    backend: Weak<BackendOwner>,
    owner_closers: HashMap<u64, Weak<dyn Fn() + Send + Sync>>,
    next_owner: u64,
    work_sets: Vec<Weak<crate::scheduler::work_set::WorkSetInner>>,
}

/// Lanes retain admission state, but cannot keep an idle executor alive.
pub(crate) struct RuntimeControl {
    id: u64,
    state: Mutex<ControlState>,
    changed: Condvar,
    owned: OnceLock<Weak<OwnedScheduler>>,
    physical: Arc<crate::external::PhysicalRegistry>,
}

impl RuntimeControl {
    pub(super) fn new(
        backend: &Arc<BackendOwner>,
        physical: Arc<crate::external::PhysicalRegistry>,
    ) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        // IDs have no publication role; the mutex publishes runtime state.
        let id = NEXT_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("runtime identity space exhausted");
        Self {
            id,
            state: Mutex::new(ControlState {
                phase: SWRuntimeState::Running,
                active: 0,
                external: 0,
                backend: Arc::downgrade(backend),
                owner_closers: HashMap::new(),
                next_owner: 1,
                work_sets: Vec::new(),
            }),
            changed: Condvar::new(),
            owned: OnceLock::new(),
            physical,
        }
    }

    fn lock(&self) -> MutexGuard<'_, ControlState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn identity(&self) -> u64 {
        self.id
    }

    pub(crate) fn admit_external(self: &Arc<Self>) -> Result<ExternalAdmission, SWExecutionError> {
        let mut state = self.lock();
        if state.phase != SWRuntimeState::Running {
            return Err(SWExecutionError::Closed);
        }
        state.external = state
            .external
            .checked_add(1)
            .expect("external count exhausted");
        Ok(ExternalAdmission {
            control: Arc::clone(self),
        })
    }

    pub(crate) fn reserve_physical(&self, required: bool) -> Result<u64, SWSpawnError> {
        let state = self.lock();
        if state.phase != SWRuntimeState::Running {
            return Err(SWSpawnError::Closed);
        }
        self.physical.reserve(required)
    }

    pub(crate) fn activate_physical(
        &self,
        registry: &crate::external::PhysicalRegistry,
        id: u64,
        work_set: Option<&crate::scheduler::work_set::WorkSetLease>,
    ) -> Result<(), SWSpawnError> {
        let state = self.lock();
        if state.phase != SWRuntimeState::Running {
            return Err(SWSpawnError::Closed);
        }
        if let Some(work_set) = work_set {
            work_set.activate_physical(registry, id)
        } else {
            registry.activate(id);
            Ok(())
        }
    }

    pub(crate) fn external_progress(&self) -> crate::external::SWExternalProgress {
        let state = self.lock();
        let mut progress = self.physical.progress();
        progress.logical = state.external;
        progress
    }

    pub(crate) fn work_set(
        self: &Arc<Self>,
        capacity: std::num::NonZeroUsize,
    ) -> Result<crate::scheduler::SWWorkSet, SWSpawnError> {
        let mut state = self.lock();
        if state.phase != SWRuntimeState::Running {
            return Err(SWSpawnError::Closed);
        }
        let set = crate::scheduler::SWWorkSet::new(self, capacity);
        state.work_sets.retain(|set| set.strong_count() != 0);
        state.work_sets.push(Arc::downgrade(&set.inner));
        Ok(set)
    }

    pub(super) fn cancel_work_sets(&self) {
        let sets = self
            .lock()
            .work_sets
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        for set in sets {
            set.cancel();
        }
    }

    /// The closer contains only transferable transport bookkeeping, never O
    /// or local callbacks. Registration and runtime closure share this lock.
    pub(crate) fn register_owner(
        self: &Arc<Self>,
        closer: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<OwnerRegistration, SWExecutionError> {
        if current().is_some() || context::owner_callback_active() {
            return Err(SWExecutionError::InvalidContext);
        }
        let mut state = self.lock();
        if state.phase != SWRuntimeState::Running {
            return Err(SWExecutionError::Closed);
        }
        let id = state.next_owner;
        state.next_owner = id.checked_add(1).expect("owner identity exhausted");
        state.owner_closers.insert(id, Arc::downgrade(&closer));
        drop(state);
        context::register_live_owner();
        Ok(OwnerRegistration {
            control: Arc::clone(self),
            id,
            _closer: closer,
            not_send: PhantomData,
        })
    }

    pub(super) fn close_owner_routes(&self) {
        let closers = self
            .lock()
            .owner_closers
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        for close in closers {
            close();
        }
    }

    pub(super) fn install_owned(&self, scheduler: &Arc<OwnedScheduler>) {
        assert!(
            self.owned.set(Arc::downgrade(scheduler)).is_ok(),
            "owned scheduler installed once"
        );
    }

    pub(crate) fn owned_scheduler(&self) -> Result<Arc<OwnedScheduler>, SWSpawnError> {
        match self.owned.get() {
            Some(scheduler) => scheduler.upgrade().ok_or(SWSpawnError::Closed),
            None if self.phase() == SWRuntimeState::Running => Err(SWSpawnError::Disabled),
            None => Err(SWSpawnError::Closed),
        }
    }

    /// Commit a root or an accounted execution descendant against runtime
    /// closure. This token retains bookkeeping, never the backend queues.
    pub(crate) fn admit_owned(self: &Arc<Self>) -> Result<OwnedAdmission, SWExecutionError> {
        let descendant = current().is_some_and(|context| context.runtime == self.id);
        let mut state = self.lock();
        if state.phase != SWRuntimeState::Running
            && !(state.phase == SWRuntimeState::Closing && descendant)
        {
            return Err(SWExecutionError::Closed);
        }
        state.active = state
            .active
            .checked_add(1)
            .expect("owned admission count overflow");
        Ok(OwnedAdmission {
            control: Arc::clone(self),
        })
    }

    /// Temporary backend ownership for an accepted handoff or execution.
    /// Ready successors remain dispatchable during graceful closure. The
    /// scheduler must acquire this before claiming user work, not retain it
    /// inside a queued closure or dependency-waiting record.
    pub(crate) fn acquire_owned(
        self: &Arc<Self>,
        class: SWExecutionClass,
    ) -> Result<ExecutionLease, SWExecutionError> {
        let mut state = self.lock();
        if !matches!(
            state.phase,
            SWRuntimeState::Running | SWRuntimeState::Closing
        ) {
            return Err(SWExecutionError::Closed);
        }
        let backend = state.backend.upgrade().ok_or(SWExecutionError::Closed)?;
        state.active = state
            .active
            .checked_add(1)
            .expect("execution lease count overflow");
        Ok(ExecutionLease {
            control: Arc::clone(self),
            backend: Some(backend),
            class,
        })
    }

    pub(crate) fn phase(&self) -> SWRuntimeState {
        self.lock().phase
    }

    pub(crate) fn acquire(
        self: &Arc<Self>,
        class: SWExecutionClass,
    ) -> Result<ExecutionLease, SWExecutionError> {
        let nested = match current() {
            Some(context) if context.runtime == self.id && context.class == class => true,
            Some(_) => return Err(SWExecutionError::InvalidContext),
            None => false,
        };
        let mut state = self.lock();
        if state.phase != SWRuntimeState::Running && !nested {
            return Err(SWExecutionError::Closed);
        }
        // Same-lane nesting is authorized by the outer invocation, including
        // after root closure. Its live lease guarantees this upgrade succeeds.
        let backend = state.backend.upgrade().ok_or(SWExecutionError::Closed)?;
        state.active = state
            .active
            .checked_add(1)
            .expect("execution lease count overflow");
        Ok(ExecutionLease {
            control: Arc::clone(self),
            backend: Some(backend),
            class,
        })
    }

    pub(super) fn close_and_wait(&self) -> Result<(), SWShutdownError> {
        let mut state = self.lock();
        if !state.owner_closers.is_empty() {
            return Err(SWShutdownError::LiveOwners);
        }
        if state.external != 0 || !self.physical.is_empty() {
            return Err(SWShutdownError::LiveExternal);
        }
        if state
            .work_sets
            .iter()
            .filter_map(Weak::upgrade)
            .any(|set| !set.progress().is_drained())
        {
            return Err(SWShutdownError::LiveWorkSets);
        }
        state.phase = SWRuntimeState::Closing;
        while state.active != 0 {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        Ok(())
    }

    pub(super) fn set_phase(&self, phase: SWRuntimeState) {
        self.lock().phase = phase;
    }
}

/// Owner-local registration survives until every local callback is cleaned.
/// It retains control state but never backend ownership.
pub(crate) struct OwnerRegistration {
    control: Arc<RuntimeControl>,
    id: u64,
    _closer: Arc<dyn Fn() + Send + Sync>,
    not_send: PhantomData<Rc<()>>,
}

impl Drop for OwnerRegistration {
    fn drop(&mut self) {
        self.control.lock().owner_closers.remove(&self.id);
        context::unregister_live_owner();
    }
}

/// Retains all pools until a lexical invocation has settled its borrowed data.
/// There is one lease per invocation, not per batch item.
pub(crate) struct ExecutionLease {
    control: Arc<RuntimeControl>,
    backend: Option<Arc<BackendOwner>>,
    class: SWExecutionClass,
}

impl ExecutionLease {
    pub(crate) fn pool(&self) -> &MicropoolBackend {
        &self
            .backend
            .as_ref()
            .expect("live lease owns backend")
            .pools[self.class.index()]
    }
}

impl Drop for ExecutionLease {
    fn drop(&mut self) {
        // Zero active leases must imply no lease still owns a backend Arc.
        // Drop outside the control lock: a last-owner drop can run destructors.
        drop(self.backend.take());
        let mut state = self.control.lock();
        state.active -= 1;
        if state.active == 0 {
            self.control.changed.notify_all();
        }
    }
}

pub(crate) struct OwnedAdmission {
    control: Arc<RuntimeControl>,
}

pub(crate) struct ExternalAdmission {
    control: Arc<RuntimeControl>,
}

impl Drop for ExternalAdmission {
    fn drop(&mut self) {
        let mut state = self.control.lock();
        state.external -= 1;
        self.control.changed.notify_all();
    }
}

impl Drop for OwnedAdmission {
    fn drop(&mut self) {
        let mut state = self.control.lock();
        state.active -= 1;
        if state.active == 0 {
            self.control.changed.notify_all();
        }
    }
}
