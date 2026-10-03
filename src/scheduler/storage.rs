//! Bounded retention of control allocations, never of reusable public identities.

use std::sync::{Arc, Mutex};

use crate::execution::group::GroupInner;
use crate::runtime::config::SWExecutionClass;
use crate::task::{Signal, SignalLease};

#[cfg(test)]
#[path = "../../tests/unit/storage.rs"]
mod tests;

mod jobs;
pub(super) use jobs::{JobCheckouts, JobPool};
pub(crate) use jobs::{JobHandle, JobWeak};
mod buffers;
pub(crate) use buffers::BufferPool;

/// Only sealed waves enter this list. A wave may still have running members;
/// checkout checks its terminal boundary and every retained accessor.
pub(crate) struct GroupRetirement {
    entries: Mutex<Vec<Arc<GroupInner>>>,
    limit: usize,
}

impl GroupRetirement {
    pub(crate) fn retire(&self, group: Arc<GroupInner>) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if entries.len() < self.limit {
            entries.push(group);
        }
    }
}

pub(crate) struct GroupPool {
    retired: Arc<GroupRetirement>,
}

impl GroupPool {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            retired: Arc::new(GroupRetirement {
                entries: Mutex::new(Vec::new()),
                limit: limit.min(32),
            }),
        }
    }

    pub(crate) fn acquire(&self, id: u64, class: SWExecutionClass) -> Arc<GroupInner> {
        let mut entries = self
            .retired
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for index in 0..entries.len() {
            if let Some(group) = Arc::get_mut(&mut entries[index])
                && group.is_complete()
            {
                group.reset(id, class);
                return entries.swap_remove(index);
            }
        }
        drop(entries);
        let mut group = Arc::new(GroupInner::new(id, class));
        Arc::get_mut(&mut group)
            .expect("new group allocation is exclusive")
            .set_retirement(Arc::downgrade(&self.retired));
        group
    }
}

/// Signals enter this cache only when their final strong owner is released.
/// No live result cell is examined during acquisition.
pub(crate) struct SignalRetirement {
    entries: Mutex<Vec<Arc<Signal>>>,
    limit: usize,
}

impl SignalRetirement {
    pub(crate) fn retire_if_last(&self, mut signal: Arc<Signal>) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Serializing the actual nonfinal decrement is necessary: two final
        // leases dropping concurrently must not both observe the other lease.
        if Arc::strong_count(&signal) > 1 {
            drop(signal);
            return;
        }
        if Arc::get_mut(&mut signal).is_some() && signal.is_reusable() && entries.len() < self.limit
        {
            entries.push(signal);
            return;
        }
        drop(entries);
        drop(signal);
    }
}

pub(crate) struct SignalPool {
    retired: Arc<SignalRetirement>,
}

#[derive(Default)]
pub(super) struct SignalCheckouts {
    entries: Vec<Arc<Signal>>,
}

impl SignalCheckouts {
    pub(super) fn clear(&mut self) {
        for entry in self.entries.drain(..) {
            crate::cleanup::discard_value(entry);
        }
    }

    pub(super) fn bytes(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<Arc<Signal>>()
    }
}

impl SignalPool {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            retired: Arc::new(SignalRetirement {
                entries: Mutex::new(Vec::new()),
                limit,
            }),
        }
    }

    pub(crate) fn acquire(&self) -> SignalLease {
        let recycled = {
            self.retired
                .entries
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .pop()
        };
        let mut signal = recycled.unwrap_or_else(|| Arc::new(Signal::new()));
        Arc::get_mut(&mut signal)
            .expect("retired signal is exclusive")
            .reset();
        SignalLease::pooled(signal, Arc::downgrade(&self.retired))
    }

    #[cfg(test)]
    pub(crate) fn acquire_many(&self, count: usize) -> Vec<SignalLease> {
        let mut signals = Vec::new();
        self.acquire_many_into(count, &mut signals, &mut SignalCheckouts::default());
        signals
    }

    pub(super) fn acquire_many_into(
        &self,
        count: usize,
        signals: &mut Vec<SignalLease>,
        checkouts: &mut SignalCheckouts,
    ) {
        assert!(signals.is_empty());
        assert!(checkouts.entries.is_empty());
        signals.reserve(count);
        checkouts.entries.reserve(count);
        {
            let mut entries = self
                .retired
                .entries
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            for _ in 0..count {
                let Some(signal) = entries.pop() else {
                    break;
                };
                checkouts.entries.push(signal);
            }
        }
        let mut returned = checkouts.entries.drain(..);
        for _ in 0..count {
            let mut signal = returned.next().unwrap_or_else(|| Arc::new(Signal::new()));
            Arc::get_mut(&mut signal)
                .expect("retired signal is exclusive")
                .reset();
            signals.push(SignalLease::pooled(signal, Arc::downgrade(&self.retired)));
        }
    }
}
