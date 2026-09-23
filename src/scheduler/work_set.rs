//! Lifetime accounting for accepted work and bounded descendant discovery.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};

use crate::execution::context;
use crate::runtime::{RuntimeControl, SWRuntimeState};
use crate::task::{SWProducerControl, SWWaitError};

/// Why a discovery capability could not be acquired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWDiscoveryError {
    Full,
    Closed,
    Cancelled,
}

impl std::fmt::Display for SWDiscoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => formatter.write_str("discovery permit capacity is full"),
            Self::Closed => formatter.write_str("work set or runtime is closed"),
            Self::Cancelled => formatter.write_str("work set is cancelled"),
        }
    }
}

impl std::error::Error for SWDiscoveryError {}

/// Set responsibilities that remain after a snapshot is taken.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWWorkSetProgress {
    pub sealed: bool,
    pub cancelled: bool,
    pub active_work: usize,
    pub discovery_permits: usize,
}

impl SWWorkSetProgress {
    pub fn is_drained(self) -> bool {
        self.sealed && self.active_work == 0 && self.discovery_permits == 0
    }
}

type CancelHook = Box<dyn Fn() + Send + Sync + 'static>;

struct State {
    open: bool,
    cancelled: bool,
    active_work: usize,
    permits: usize,
    next_id: u64,
    cancel_hooks: HashMap<u64, CancelHook>,
}

pub(crate) struct WorkSetInner {
    runtime_identity: u64,
    control: Weak<RuntimeControl>,
    permit_capacity: NonZeroUsize,
    state: Mutex<State>,
    changed: Condvar,
    wake: Arc<crate::progress::SWWake>,
}

impl Drop for WorkSetInner {
    fn drop(&mut self) {
        // Dropping the last empty, open set also removes a shutdown blocker.
        self.wake.notify();
    }
}

impl WorkSetInner {
    fn runtime_accepts(&self, root: bool) -> bool {
        self.control
            .upgrade()
            .is_some_and(|control| match control.phase() {
                SWRuntimeState::Running => true,
                SWRuntimeState::Closing => !root,
                SWRuntimeState::Stopped | SWRuntimeState::Abandoned => false,
            })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn progress(&self) -> SWWorkSetProgress {
        let state = self.lock();
        SWWorkSetProgress {
            sealed: !state.open,
            cancelled: state.cancelled,
            active_work: state.active_work,
            discovery_permits: state.permits,
        }
    }

    fn acquire_work(
        self: &Arc<Self>,
        runtime_identity: u64,
        root: bool,
    ) -> Result<WorkSetLease, SWDiscoveryError> {
        if self.runtime_identity != runtime_identity || !self.runtime_accepts(root) {
            return Err(SWDiscoveryError::Closed);
        }
        let mut state = self.lock();
        if state.cancelled {
            return Err(SWDiscoveryError::Cancelled);
        }
        if root && !state.open {
            return Err(SWDiscoveryError::Closed);
        }
        state.active_work = state
            .active_work
            .checked_add(1)
            .expect("work set count exhausted");
        let id = state.next_id;
        state.next_id = id.checked_add(1).expect("work set identity exhausted");
        drop(state);
        self.wake.notify();
        Ok(WorkSetLease(Arc::new(LeaseInner {
            inner: Arc::clone(self),
            id,
        })))
    }

    pub(crate) fn cancel(&self) {
        let hooks = {
            let mut state = self.lock();
            state.open = false;
            state.cancelled = true;
            self.changed.notify_all();
            std::mem::take(&mut state.cancel_hooks)
        };
        self.wake.notify();
        for (_, hook) in hooks {
            hook();
        }
    }

    pub(crate) fn seal(&self) {
        let mut state = self.lock();
        state.open = false;
        self.changed.notify_all();
        drop(state);
        self.wake.notify();
    }
}

/// A lifetime boundary for owned producers, consumer deliveries and discovery.
///
/// The capacity bounds simultaneously live discovery permits. Owned record and
/// delivery capacities are controlled by their own scheduler/owner limits.
/// Retained immutable results do not keep this set active.
#[derive(Clone)]
pub struct SWWorkSet {
    pub(crate) inner: Arc<WorkSetInner>,
}

impl SWWorkSet {
    pub(crate) fn new(control: &Arc<RuntimeControl>, permit_capacity: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(WorkSetInner {
                runtime_identity: control.identity(),
                control: Arc::downgrade(control),
                permit_capacity,
                state: Mutex::new(State {
                    open: true,
                    cancelled: false,
                    active_work: 0,
                    permits: 0,
                    next_id: 1,
                    cancel_hooks: HashMap::new(),
                }),
                changed: Condvar::new(),
                wake: control.wake(),
            }),
        }
    }

    /// Reserves a live discoverer that can admit descendants after sealing.
    pub fn discovery(&self) -> Result<SWDiscoveryPermit, SWDiscoveryError> {
        if !self.inner.runtime_accepts(true) {
            return Err(SWDiscoveryError::Closed);
        }
        let mut state = self.inner.lock();
        if state.cancelled {
            return Err(SWDiscoveryError::Cancelled);
        }
        if !state.open {
            return Err(SWDiscoveryError::Closed);
        }
        if state.permits >= self.inner.permit_capacity.get() {
            return Err(SWDiscoveryError::Full);
        }
        state.permits += 1;
        drop(state);
        self.inner.wake.notify();
        Ok(SWDiscoveryPermit {
            inner: Arc::clone(&self.inner),
        })
    }

    /// Rejects new roots while accepted work and discoverers can finish.
    pub fn seal(&self) {
        self.inner.seal();
    }

    /// Rejects descendants and requests cancellation of owned producers and
    /// unclaimed consumer deliveries. Hooks run without the accounting lock.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    pub fn progress(&self) -> SWWorkSetProgress {
        self.inner.progress()
    }

    pub fn is_drained(&self) -> bool {
        self.progress().is_drained()
    }

    /// Passively waits for a sealed set to drain. The host must still service
    /// owner phases and external providers. A CPU or owner context cannot wait.
    pub fn wait_drained(&self) -> Result<(), SWWaitError> {
        if context::passive_wait_forbidden() {
            return Err(SWWaitError::ExecutionContext);
        }
        let mut state = self.inner.lock();
        while state.open || state.active_work != 0 || state.permits != 0 {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        Ok(())
    }

    pub(crate) fn try_root_lease(
        &self,
        runtime_identity: u64,
    ) -> Result<WorkSetLease, SWDiscoveryError> {
        self.inner.acquire_work(runtime_identity, true)
    }

    pub(crate) fn try_consumer_lease(
        &self,
        runtime_identity: u64,
    ) -> Result<WorkSetLease, SWDiscoveryError> {
        self.inner.acquire_work(runtime_identity, true)
    }
}

/// A bounded capability for admitting children after root closure. Moving a
/// permit transfers its discovery count; it cannot be cloned without `fork`.
pub struct SWDiscoveryPermit {
    inner: Arc<WorkSetInner>,
}

impl SWDiscoveryPermit {
    pub fn fork(&self) -> Result<Self, SWDiscoveryError> {
        if !self.inner.runtime_accepts(false) {
            return Err(SWDiscoveryError::Closed);
        }
        let mut state = self.inner.lock();
        if state.cancelled {
            return Err(SWDiscoveryError::Cancelled);
        }
        if state.permits >= self.inner.permit_capacity.get() {
            return Err(SWDiscoveryError::Full);
        }
        state.permits += 1;
        drop(state);
        self.inner.wake.notify();
        Ok(Self {
            inner: Arc::clone(&self.inner),
        })
    }

    pub fn progress(&self) -> SWWorkSetProgress {
        self.inner.progress()
    }

    pub(crate) fn try_child_lease(
        &self,
        runtime_identity: u64,
    ) -> Result<WorkSetLease, SWDiscoveryError> {
        self.inner.acquire_work(runtime_identity, false)
    }
}

impl Drop for SWDiscoveryPermit {
    fn drop(&mut self) {
        let mut state = self.inner.lock();
        state.permits -= 1;
        self.inner.changed.notify_all();
        drop(state);
        self.inner.wake.notify();
    }
}

/// Provisional admission becomes the record's accounting once accepted. The
/// scheduler drops this after terminal capture, edge and group cleanup.
#[derive(Clone)]
pub(crate) struct WorkSetLease(Arc<LeaseInner>);

struct LeaseInner {
    inner: Arc<WorkSetInner>,
    id: u64,
}

impl WorkSetLease {
    pub(crate) fn activate_physical(
        &self,
        registry: &crate::external::PhysicalRegistry,
        id: u64,
    ) -> Result<(), crate::scheduler::SWSpawnError> {
        let state = self.0.inner.lock();
        if state.cancelled {
            return Err(crate::scheduler::SWSpawnError::Closed);
        }
        // The registry transition invokes no provider or user code. Activation
        // and set cancellation share this lock; existing active accesses retain
        // their independent lease even after cancellation.
        registry.activate(id);
        Ok(())
    }
    pub(crate) fn register_producer(&self, producer: SWProducerControl) {
        self.register_cancel(Box::new(move || producer.cancel()));
    }

    pub(crate) fn register_cancel(&self, hook: CancelHook) {
        let previous = {
            let mut state = self.0.inner.lock();
            if state.cancelled {
                drop(state);
                hook();
                return;
            }
            state.cancel_hooks.insert(self.0.id, hook)
        };
        drop(previous);
    }
}

impl Drop for LeaseInner {
    fn drop(&mut self) {
        let hook = {
            let mut state = self.inner.lock();
            state.active_work -= 1;
            let hook = state.cancel_hooks.remove(&self.id);
            self.inner.changed.notify_all();
            hook
        };
        drop(hook);
        self.inner.wake.notify();
    }
}
