//! Bounded retention of control allocations, never of reusable public identities.

use std::sync::Arc;

#[cfg(test)]
#[path = "../../tests/unit/storage.rs"]
mod tests;

/// Retains at most `limit` allocations. Only exclusive allocations can be reset:
/// even a weak accessor prevents reuse. Busy entries are inspected once per
/// checkout, with a rotating start to avoid repeatedly favoring one slot.
pub(crate) struct ArcPool<T> {
    entries: Vec<Arc<T>>,
    limit: usize,
    cursor: usize,
}

impl<T> ArcPool<T> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            entries: Vec::new(),
            limit,
            cursor: 0,
        }
    }

    pub(crate) fn acquire(
        &mut self,
        create: impl FnOnce() -> T,
        reset: impl FnOnce(&mut T),
    ) -> Arc<T> {
        self.acquire_where(create, reset, |_| true)
    }

    pub(crate) fn acquire_where(
        &mut self,
        create: impl FnOnce() -> T,
        reset: impl FnOnce(&mut T),
        reusable: impl Fn(&T) -> bool,
    ) -> Arc<T> {
        for _ in 0..self.entries.len() {
            let index = self.cursor;
            self.cursor = (index + 1) % self.entries.len();
            if let Some(value) = Arc::get_mut(&mut self.entries[index])
                && reusable(value)
            {
                reset(value);
                return Arc::clone(&self.entries[index]);
            }
        }
        let mut value = Arc::new(create());
        reset(Arc::get_mut(&mut value).expect("new control allocation is exclusive"));
        if self.entries.len() < self.limit {
            self.entries.push(Arc::clone(&value));
        }
        value
    }
}

/// Empty prerequisite buffers retain at most the configured edge capacity in
/// total. Entries are cleared outside scheduler locks before returning here.
pub(crate) struct BufferPool<T> {
    entries: Vec<Vec<T>>,
    capacity: usize,
    limit: usize,
}

impl<T> BufferPool<T> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            entries: Vec::new(),
            capacity: 0,
            limit,
        }
    }

    pub(crate) fn acquire(&mut self, minimum: usize) -> Vec<T> {
        if minimum == 0 {
            return Vec::new();
        }
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.capacity() >= minimum)
        {
            let entry = self.entries.swap_remove(index);
            self.capacity -= entry.capacity();
            entry
        } else {
            Vec::with_capacity(minimum)
        }
    }

    pub(crate) fn release(&mut self, entry: Vec<T>) {
        assert!(
            entry.is_empty(),
            "only detached subscription storage can recycle"
        );
        let capacity = entry.capacity();
        if capacity != 0 && capacity <= self.limit - self.capacity {
            self.capacity += capacity;
            self.entries.push(entry);
        }
    }
}
