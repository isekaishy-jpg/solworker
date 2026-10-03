//! One physical prerequisite registration for a committed bounded portion.
//!
//! Records and the registered callback retain this descriptor; its slots and
//! subscription retain only weak identities. The descriptor mutex never nests
//! with scheduler, record, signal or recycler synchronization.

use std::sync::{Arc, Mutex, Weak};

use super::storage::JobWeak;
use super::{Activation, JobHandle, OwnedScheduler, SWCompletion, SWTaskStatus, Subscription};
use super::{bulk::QUANTUM, enqueue_activation};

#[cfg(test)]
#[path = "../../tests/unit/prerequisite_range.rs"]
mod tests;

pub(super) type Members = [Option<JobWeak>; QUANTUM];

const SMALL_RANGE: usize = 8;

struct State<const N: usize> {
    members: [Option<JobWeak>; N],
    active: usize,
    delivered: bool,
    installing: bool,
    subscription: Option<Subscription>,
}

struct Descriptor<const N: usize> {
    scheduler: Weak<OwnedScheduler>,
    state: Mutex<State<N>>,
}

// The handle erases only descriptor capacity. Each descriptor still uses one
// Arc allocation; no boxed member array or second descriptor allocation exists.
#[derive(Clone)]
pub(super) struct PrerequisiteRange(Arc<dyn RangeOps>);

trait RangeOps: Send + Sync {
    fn subscribe(self: Arc<Self>, prerequisite: &SWCompletion);
    fn take_members(&self) -> Members;
    fn detach(&self, slot: usize);
}

pub(super) struct RangeMember {
    range: PrerequisiteRange,
    slot: usize,
}

impl PrerequisiteRange {
    pub(super) fn new(scheduler: &Arc<OwnedScheduler>, jobs: &[JobHandle]) -> Self {
        assert!((2..=QUANTUM).contains(&jobs.len()));
        if jobs.len() <= SMALL_RANGE {
            Self(Descriptor::<SMALL_RANGE>::new(scheduler, jobs))
        } else {
            Self(Descriptor::<QUANTUM>::new(scheduler, jobs))
        }
    }

    pub(super) fn member(&self, slot: usize) -> RangeMember {
        RangeMember {
            range: self.clone(),
            slot,
        }
    }

    pub(super) fn subscribe(&self, prerequisite: &SWCompletion) {
        Arc::clone(&self.0).subscribe(prerequisite);
    }

    pub(super) fn take_members(&self) -> Members {
        self.0.take_members()
    }
}

impl<const N: usize> Descriptor<N> {
    fn new(scheduler: &Arc<OwnedScheduler>, jobs: &[JobHandle]) -> Arc<Self> {
        debug_assert!(jobs.len() <= N);
        Arc::new(Self {
            scheduler: Arc::downgrade(scheduler),
            state: Mutex::new(State {
                members: std::array::from_fn(|slot| jobs.get(slot).map(JobHandle::downgrade)),
                active: jobs.len(),
                delivered: false,
                installing: true,
                subscription: None,
            }),
        })
    }

    fn deliver(self: &Arc<Self>, status: SWTaskStatus) {
        let (subscription, _members) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.delivered {
                return;
            }
            state.delivered = true;
            (state.subscription.take(), state.active)
        };
        // Drop the weak signal identity promptly, before any member work.
        drop(subscription);
        #[cfg(test)]
        if let Some(scheduler) = self.scheduler.upgrade() {
            let hook = scheduler.range_delivery_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        if let Some(scheduler) = self.scheduler.upgrade() {
            #[cfg(feature = "diagnostics")]
            scheduler.trace(
                "range.delivered",
                Arc::as_ptr(self) as usize as u64,
                _members as u64,
            );
            enqueue_activation(Activation::PrerequisiteRange(
                scheduler,
                PrerequisiteRange(Arc::clone(self) as Arc<dyn RangeOps>),
                status,
            ));
        }
    }
}

impl<const N: usize> RangeOps for Descriptor<N> {
    fn subscribe(self: Arc<Self>, prerequisite: &SWCompletion) {
        // All slots/tokens exist before subscribing. Already-terminal signals
        // can invoke deliver inline before the returned handle is installed.
        let range = Arc::clone(&self);
        let subscription = prerequisite.subscribe_cancelable(Box::new(move |status| {
            range.deliver(status);
        }));
        #[cfg(test)]
        if let Some(scheduler) = self.scheduler.upgrade() {
            let hook = scheduler.range_install_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        let unused = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            debug_assert!(state.installing);
            state.installing = false;
            if state.delivered || state.active == 0 {
                Some(subscription)
            } else {
                state.subscription = Some(subscription);
                None
            }
        };
        drop(unused);
    }

    fn take_members(&self) -> Members {
        let members = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            std::mem::replace(&mut state.members, std::array::from_fn(|_| None))
        };
        // Pad the bounded activation snapshot after unlocking. The queue keeps
        // only the handle; ordinary activation/finalization entries stay small.
        let mut members = members.into_iter();
        std::array::from_fn(|_| members.next().flatten())
    }

    fn detach(&self, slot: usize) {
        let (member, subscription) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            // Delivery can already have taken the slots; token ownership, not
            // slot occupancy, accounts for the remaining interested records.
            state.active -= 1;
            let member = state.members[slot].take();
            let subscription = (state.active == 0)
                .then(|| state.subscription.take())
                .flatten();
            (member, subscription)
        };
        drop(member);
        drop(subscription);
    }
}

impl Drop for RangeMember {
    fn drop(&mut self) {
        self.range.0.detach(self.slot);
    }
}
