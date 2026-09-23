//! Bounded retention of control allocations, never of reusable public identities.

use std::sync::{Arc, Mutex};

use crate::execution::group::GroupInner;
use crate::runtime::config::SWExecutionClass;
use crate::task::{Signal, SignalLease};

#[cfg(test)]
#[path = "../../tests/unit/storage.rs"]
mod tests;

mod jobs;
pub(super) use jobs::{Job, JobHandle, JobPool};
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
                limit,
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

impl SignalPool {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            retired: Arc::new(SignalRetirement {
                entries: Mutex::new(Vec::new()),
                limit,
            }),
        }
    }

    pub(crate) fn acquire(&mut self) -> SignalLease {
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
}
