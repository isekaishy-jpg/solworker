//! Ordinary FIFO and separately ordered resource-stage routes.

use std::collections::VecDeque;

use super::demand::{DemandSelection, SWPriority};

struct Bucket {
    priority: SWPriority,
    active: VecDeque<(i128, u64)>,
    deferred: VecDeque<(i128, u64)>,
}

/// Ready records remain within the scheduler's admitted record bound. The
/// resource buckets are allocated from the runtime's fixed rank list; relinking
/// removes the old entry, so no stale heap copies accumulate.
pub(super) struct ReadyQueue {
    ordinary: VecDeque<u64>,
    resource: Vec<Bucket>,
    pub(super) runnable: usize,
    pub(super) handed_off: usize,
}

impl ReadyQueue {
    pub(super) fn with_priorities(priorities: &[SWPriority]) -> Self {
        let mut ranks = priorities.to_vec();
        ranks.sort_unstable();
        ranks.dedup();
        Self {
            ordinary: VecDeque::new(),
            resource: {
                ranks
                    .iter()
                    .map(|priority| Bucket {
                        priority: *priority,
                        active: VecDeque::new(),
                        deferred: VecDeque::new(),
                    })
                    .collect()
            },
            runnable: 0,
            handed_off: 0,
        }
    }

    pub(super) fn push(&mut self, id: u64) {
        self.ordinary.push_back(id);
        self.runnable += 1;
    }

    pub(super) fn push_resource(&mut self, id: u64, selection: DemandSelection) {
        self.insert_resource(id, selection);
        self.runnable += 1;
    }

    pub(super) fn update_resource(&mut self, id: u64, selection: DemandSelection) {
        self.unlink_resource(id);
        self.insert_resource(id, selection);
    }

    fn insert_resource(&mut self, id: u64, selection: DemandSelection) {
        debug_assert!(
            selection.priority.is_some(),
            "resource work requires a rank"
        );
        let Some(priority) = selection.priority else {
            return;
        };
        let Some(bucket) = self
            .resource
            .iter_mut()
            .find(|bucket| bucket.priority == priority)
        else {
            return;
        };
        let queue = if selection.active {
            &mut bucket.active
        } else {
            &mut bucket.deferred
        };
        let position = queue
            .iter()
            .position(|(tie, _)| *tie > selection.tie)
            .unwrap_or(queue.len());
        queue.insert(position, (selection.tie, id));
    }

    fn unlink_resource(&mut self, id: u64) {
        for bucket in &mut self.resource {
            bucket.active.retain(|(_, queued)| *queued != id);
            bucket.deferred.retain(|(_, queued)| *queued != id);
        }
    }

    /// The two routes interleave by admission identity. Within the resource
    /// route active ranks precede deferred ranks, with lower ranks first; the
    /// ordinary route retains FIFO. Neither route imposes rank on the other.
    pub(super) fn pop(&mut self) -> Option<u64> {
        let resource = self
            .resource
            .iter()
            .find_map(|bucket| bucket.active.front().map(|(_, id)| *id))
            .or_else(|| {
                self.resource
                    .iter()
                    .find_map(|bucket| bucket.deferred.front().map(|(_, id)| *id))
            });
        match (self.ordinary.front().copied(), resource) {
            (Some(ordinary), Some(resource)) if ordinary < resource => self.ordinary.pop_front(),
            (Some(_), Some(resource)) | (None, Some(resource)) => {
                self.unlink_resource(resource);
                Some(resource)
            }
            (Some(_), None) => self.ordinary.pop_front(),
            (None, None) => None,
        }
    }

    pub(super) fn remove(&mut self, id: u64) {
        self.ordinary.retain(|queued| *queued != id);
        self.unlink_resource(id);
    }
}
