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

struct State {
    members: Members,
    active: usize,
    delivered: bool,
    installing: bool,
    subscription: Option<Subscription>,
}

pub(super) struct PrerequisiteRange {
    scheduler: Weak<OwnedScheduler>,
    state: Mutex<State>,
}

pub(super) struct RangeMember {
    range: Arc<PrerequisiteRange>,
    slot: usize,
}

impl PrerequisiteRange {
    pub(super) fn new(scheduler: &Arc<OwnedScheduler>, jobs: &[JobHandle]) -> Arc<Self> {
        assert!((2..=QUANTUM).contains(&jobs.len()));
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

    pub(super) fn member(self: &Arc<Self>, slot: usize) -> RangeMember {
        RangeMember {
            range: Arc::clone(self),
            slot,
        }
    }

    pub(super) fn subscribe(self: &Arc<Self>, prerequisite: &SWCompletion) {
        // All slots/tokens exist before subscribing. Already-terminal signals
        // can invoke deliver inline before the returned handle is installed.
        let range = Arc::clone(self);
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
                Arc::clone(self),
                status,
            ));
        }
    }

    pub(super) fn take_members(&self) -> Members {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        std::mem::replace(&mut state.members, std::array::from_fn(|_| None))
    }
}

impl Drop for RangeMember {
    fn drop(&mut self) {
        let (member, subscription) = {
            let mut state = self
                .range
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            // Delivery can already have taken the slots; token ownership, not
            // slot occupancy, accounts for the remaining interested records.
            state.active -= 1;
            let member = state.members[self.slot].take();
            let subscription = (state.active == 0)
                .then(|| state.subscription.take())
                .flatten();
            (member, subscription)
        };
        drop(member);
        drop(subscription);
    }
}
