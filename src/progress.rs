//! Passive host observation and a generation-based wake protocol.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Instant;

use crate::execution::context;
use crate::external::SWExternalProgress;
use crate::runtime::SWRuntimeState;
use crate::scheduler::SWCapacityUsage;

/// CPU and control work still owned by the scheduler. Counts describe current
/// state; full flags identify saturated admission routes, not queued applicants.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SWSchedulerProgress {
    pub waiting: usize,
    pub ready: usize,
    pub deferred: usize,
    pub handed: usize,
    pub running: usize,
    pub finalizing: usize,
    pub handoff_wrappers: [usize; 3],
    pub demand_pending: bool,
    pub provider_callbacks: usize,
    pub records_full: bool,
    pub edges_full: bool,
    pub runnable_full: [bool; 3],
    pub handoff_full: [bool; 3],
}

/// Transferable owner-route bookkeeping. Waiting deliveries still need their
/// producer; ready deliveries need a matching host phase; cleanup is owner-local.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SWOwnerProgress {
    pub routes: usize,
    pub waiting: usize,
    pub ready: usize,
    pub claimed: usize,
    pub cleanup: usize,
}

impl SWOwnerProgress {
    pub(crate) fn add(&mut self, other: Self) {
        self.routes += other.routes;
        self.waiting += other.waiting;
        self.ready += other.ready;
        self.claimed += other.claimed;
        self.cleanup += other.cleanup;
    }
}

/// Work-set responsibilities that can outlive an empty CPU queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SWWorkSetsProgress {
    pub sets: usize,
    pub open: usize,
    pub active_work: usize,
    pub discovery_permits: usize,
}

/// One passive observation assembled from independently locked components,
/// not an atomic global snapshot or proof of drain. Call `wait_for_change` with
/// an explicit host deadline, then take a fresh snapshot. Service pending demand
/// updates and owner/provider work before sleeping; observation does not do so.
#[derive(Clone, Debug)]
pub struct SWProgress {
    pub state: SWRuntimeState,
    pub scheduler: SWSchedulerProgress,
    pub capacity: Option<SWCapacityUsage>,
    pub owner: SWOwnerProgress,
    pub work_sets: SWWorkSetsProgress,
    pub external: SWExternalProgress,
    /// Runtime leases include admitted CPU work, active scoped/owned invocations
    /// and in-progress owner-root admissions; these overlap scheduler counts.
    pub active_leases: usize,
    pub wake_generation: u64,
    wake: Arc<SWWake>,
}

impl SWProgress {
    // Components are sampled independently by the runtime; keeping construction
    // here prevents callers from forging the private wake association.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        state: SWRuntimeState,
        scheduler: SWSchedulerProgress,
        capacity: Option<SWCapacityUsage>,
        owner: SWOwnerProgress,
        work_sets: SWWorkSetsProgress,
        external: SWExternalProgress,
        active_leases: usize,
        wake: Arc<SWWake>,
        wake_generation: u64,
    ) -> Self {
        Self {
            state,
            scheduler,
            capacity,
            owner,
            work_sets,
            external,
            active_leases,
            wake_generation,
            wake,
        }
    }

    /// Blocks only for a state-change signal or the caller's deadline. It
    /// never runs CPU work, provider hooks, or owner callbacks. Any registered
    /// live owner or CPU participant must use explicit progress instead.
    pub fn wait_for_change(
        &self,
        deadline: Instant,
    ) -> Result<SWProgressWait, SWProgressWaitError> {
        if context::passive_wait_forbidden() {
            return Err(SWProgressWaitError::InvalidContext);
        }
        let changed = self.wake.wait_since(self.wake_generation, deadline);
        let generation = self.wake.generation();
        Ok(if changed {
            SWProgressWait::Changed(generation)
        } else {
            SWProgressWait::TimedOut(generation)
        })
    }
}

/// A wake reports a change, not the new progress snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWProgressWait {
    Changed(u64),
    TimedOut(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWProgressWaitError {
    InvalidContext,
}

impl std::fmt::Display for SWProgressWaitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("passive progress wait from a CPU or live-owner context")
    }
}

impl std::error::Error for SWProgressWaitError {}

/// After the first host sample, generation advances for each notified
/// transition. The condvar is dormant before that sample. Mutators notify
/// after publishing state, and the host samples generation before reading it.
#[derive(Debug)]
pub(crate) struct SWWake {
    generation: AtomicU64,
    enabled: AtomicBool,
    lock: Mutex<()>,
    changed: Condvar,
    notification: OnceLock<crate::notification::NotifySource>,
}

impl SWWake {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            generation: AtomicU64::new(0),
            enabled: AtomicBool::new(false),
            lock: Mutex::new(()),
            changed: Condvar::new(),
            notification: OnceLock::new(),
        })
    }

    pub(crate) fn install_notification_source(&self, source: crate::notification::NotifySource) {
        assert!(
            self.notification.set(source).is_ok(),
            "progress source installed once"
        );
    }

    pub(crate) fn notification_scope(&self) -> crate::notification::NotificationScope {
        crate::notification::NotificationScope::enter_if(self.notification.get().is_some())
    }

    pub(crate) fn generation(&self) -> u64 {
        // Sequential consistency orders this enable/sample pair against a
        // notifier's increment/enabled check, including their first race.
        self.enabled.store(true, Ordering::SeqCst);
        self.generation.load(Ordering::SeqCst)
    }

    pub(crate) fn notify(&self) {
        // If this precedes the first enable in SeqCst order, the mutation
        // itself precedes the subsequent snapshot. Otherwise incrementing
        // the generation lets the host detect it before sleeping.
        if self.enabled.load(Ordering::SeqCst) {
            self.generation
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |generation| {
                    generation.checked_add(1)
                })
                .expect("progress wake generation exhausted");
            let _guard = self.lock.lock().unwrap_or_else(|error| error.into_inner());
            self.changed.notify_all();
        }
        // Preserve the fixed CV wake before an arbitrary host adapter. Host
        // interest does not activate that CV, and scopes defer adapters past
        // enclosing bookkeeping locks.
        if let Some(source) = self.notification.get() {
            source.publish_if_watched();
        }
    }

    pub(crate) fn wait_since(&self, observed: u64, deadline: Instant) -> bool {
        let mut guard = self.lock.lock().unwrap_or_else(|error| error.into_inner());
        loop {
            if self.generation.load(Ordering::SeqCst) != observed {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let result = self.changed.wait_timeout(guard, remaining);
            guard = result.unwrap_or_else(|error| error.into_inner()).0;
        }
    }
}
