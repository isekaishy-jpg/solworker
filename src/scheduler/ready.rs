//! Indexed ordinary FIFO and separately ordered resource-stage routes.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::demand::{DemandSelection, SWPriority};

#[cfg(test)]
#[path = "../../tests/unit/ready_queue.rs"]
mod tests;

type ResourceKey = (bool, SWPriority, i128, i128, u64);

#[derive(Clone, Copy)]
enum Entry {
    Ordinary(i128),
    Resource(ResourceKey),
}

/// One entry per live queued record, with no stale copies after a rank update.
/// Sequence keys retain activation FIFO, including when job IDs arrive out of
/// order. The two routes cannot be combined into a single transitive ordering:
/// their heads interleave by ID, but each route has its own internal order.
#[derive(Default)]
pub(super) struct PendingQueue {
    ordinary: BTreeMap<i128, u64>,
    resource: BTreeSet<ResourceKey>,
    entries: HashMap<u64, Entry>,
    front: i128,
    back: i128,
}

impl PendingQueue {
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn has_resources(&self) -> bool {
        !self.resource.is_empty()
    }

    pub(super) fn push(&mut self, id: u64, selection: Option<DemandSelection>) {
        let sequence = self.back;
        self.back += 1;
        self.insert(id, selection, sequence);
    }

    pub(super) fn push_front(&mut self, id: u64, selection: Option<DemandSelection>) {
        self.front -= 1;
        self.insert(id, selection, self.front);
    }

    fn insert(&mut self, id: u64, selection: Option<DemandSelection>, sequence: i128) {
        debug_assert!(!self.entries.contains_key(&id));
        let entry = if let Some(selection) = selection {
            let key = (
                !selection.active,
                selection
                    .priority
                    .expect("resource work requires a configured rank"),
                selection.tie,
                sequence,
                id,
            );
            self.resource.insert(key);
            Entry::Resource(key)
        } else {
            self.ordinary.insert(sequence, id);
            Entry::Ordinary(sequence)
        };
        self.entries.insert(id, entry);
    }

    pub(super) fn update_resource(&mut self, id: u64, selection: DemandSelection) {
        self.remove(id);
        self.push(id, Some(selection));
    }

    pub(super) fn peek(&self) -> Option<u64> {
        let ordinary = self.ordinary.first_key_value().map(|(_, id)| *id);
        let resource = self.resource.first().map(|key| key.4);
        match (ordinary, resource) {
            (Some(ordinary), Some(resource)) => Some(ordinary.min(resource)),
            (Some(id), None) | (None, Some(id)) => Some(id),
            (None, None) => None,
        }
    }

    pub(super) fn pop(&mut self) -> Option<u64> {
        let id = self.peek()?;
        self.remove(id);
        Some(id)
    }

    pub(super) fn remove(&mut self, id: u64) {
        match self.entries.remove(&id) {
            Some(Entry::Ordinary(sequence)) => {
                self.ordinary.remove(&sequence);
            }
            Some(Entry::Resource(key)) => {
                self.resource.remove(&key);
            }
            None => {}
        }
        if self.entries.is_empty() {
            self.front = 0;
            self.back = 0;
        }
    }
}

/// Runnable accounting includes handed work, which no longer has an index
/// entry. Removing or selecting an entry therefore does not change the count.
pub(super) struct ReadyQueue {
    pub(super) pending: PendingQueue,
    pub(super) runnable: usize,
    pub(super) handed_off: usize,
}

impl ReadyQueue {
    pub(super) fn with_priorities(_priorities: &[SWPriority]) -> Self {
        Self {
            pending: PendingQueue::default(),
            runnable: 0,
            handed_off: 0,
        }
    }

    pub(super) fn push(&mut self, id: u64) {
        self.pending.push(id, None);
        self.runnable += 1;
    }

    pub(super) fn push_resource(&mut self, id: u64, selection: DemandSelection) {
        self.pending.push(id, Some(selection));
        self.runnable += 1;
    }

    pub(super) fn update_resource(&mut self, id: u64, selection: DemandSelection) {
        self.pending.update_resource(id, selection);
    }

    pub(super) fn pop(&mut self) -> Option<u64> {
        self.pending.pop()
    }

    pub(super) fn remove(&mut self, id: u64) {
        self.pending.remove(id);
    }
}
