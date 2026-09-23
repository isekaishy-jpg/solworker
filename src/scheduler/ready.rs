//! Ordinary FIFO and separately ordered resource-stage routes.

use std::collections::VecDeque;

use crate::runtime::config::SWExecutionClass;

use super::demand::{DemandSelection, SWPriority};

struct Bucket {
    priority: SWPriority,
    active: VecDeque<(i128, u64)>,
    deferred: VecDeque<(i128, u64)>,
}

/// Ready records remain within the scheduler's admitted record bound. The
/// resource buckets are allocated from the runtime's fixed rank list; relinking
/// removes the old entry, so no stale heap copies accumulate.
pub(super) struct ReadyQueues {
    ordinary: [VecDeque<u64>; 3],
    resource: [Vec<Bucket>; 3],
    pub(super) runnable: [usize; 3],
    pub(super) handed_off: [usize; 3],
}

impl ReadyQueues {
    pub(super) fn with_priorities(priorities: &[SWPriority]) -> Self {
        let mut ranks = priorities.to_vec();
        ranks.sort_unstable();
        ranks.dedup();
        Self {
            ordinary: std::array::from_fn(|_| VecDeque::new()),
            resource: std::array::from_fn(|_| {
                ranks
                    .iter()
                    .map(|priority| Bucket {
                        priority: *priority,
                        active: VecDeque::new(),
                        deferred: VecDeque::new(),
                    })
                    .collect()
            }),
            runnable: [0; 3],
            handed_off: [0; 3],
        }
    }

    pub(super) fn push(&mut self, class: SWExecutionClass, id: u64) {
        self.ordinary[class.index()].push_back(id);
        self.runnable[class.index()] += 1;
    }

    pub(super) fn push_resource(
        &mut self,
        class: SWExecutionClass,
        id: u64,
        selection: DemandSelection,
    ) {
        self.insert_resource(class, id, selection);
        self.runnable[class.index()] += 1;
    }

    pub(super) fn update_resource(
        &mut self,
        class: SWExecutionClass,
        id: u64,
        selection: DemandSelection,
    ) {
        self.unlink_resource(class, id);
        self.insert_resource(class, id, selection);
    }

    fn insert_resource(&mut self, class: SWExecutionClass, id: u64, selection: DemandSelection) {
        debug_assert!(
            selection.priority.is_some(),
            "resource work requires a rank"
        );
        let Some(priority) = selection.priority else {
            return;
        };
        let Some(bucket) = self.resource[class.index()]
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

    fn unlink_resource(&mut self, class: SWExecutionClass, id: u64) {
        for bucket in &mut self.resource[class.index()] {
            bucket.active.retain(|(_, queued)| *queued != id);
            bucket.deferred.retain(|(_, queued)| *queued != id);
        }
    }

    /// The two routes interleave by admission identity. Within the resource
    /// route active ranks precede deferred ranks, with lower ranks first; the
    /// ordinary route retains FIFO. Neither route imposes rank on the other.
    pub(super) fn pop(&mut self, class: SWExecutionClass) -> Option<u64> {
        let index = class.index();
        let resource = self.resource[index]
            .iter()
            .find_map(|bucket| bucket.active.front().map(|(_, id)| *id))
            .or_else(|| {
                self.resource[index]
                    .iter()
                    .find_map(|bucket| bucket.deferred.front().map(|(_, id)| *id))
            });
        match (self.ordinary[index].front().copied(), resource) {
            (Some(ordinary), Some(resource)) if ordinary < resource => {
                self.ordinary[index].pop_front()
            }
            (Some(_), Some(resource)) | (None, Some(resource)) => {
                self.unlink_resource(class, resource);
                Some(resource)
            }
            (Some(_), None) => self.ordinary[index].pop_front(),
            (None, None) => None,
        }
    }

    pub(super) fn remove(&mut self, class: SWExecutionClass, id: u64) {
        self.ordinary[class.index()].retain(|queued| *queued != id);
        self.unlink_resource(class, id);
    }
}
