//! FIFO class queues and the bounded backend handoff window.

use std::collections::VecDeque;

use crate::runtime::config::SWExecutionClass;

pub(super) struct ReadyQueues {
    queues: [VecDeque<u64>; 3],
    pub(super) runnable: [usize; 3],
    pub(super) handed_off: [usize; 3],
}

impl ReadyQueues {
    pub(super) fn new() -> Self {
        Self {
            queues: std::array::from_fn(|_| VecDeque::new()),
            runnable: [0; 3],
            handed_off: [0; 3],
        }
    }

    pub(super) fn push(&mut self, class: SWExecutionClass, id: u64) {
        self.queues[class.index()].push_back(id);
        self.runnable[class.index()] += 1;
    }

    pub(super) fn pop(&mut self, class: SWExecutionClass) -> Option<u64> {
        self.queues[class.index()].pop_front()
    }

    pub(super) fn remove(&mut self, class: SWExecutionClass, id: u64) {
        self.queues[class.index()].retain(|queued| *queued != id);
    }
}
