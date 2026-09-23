//! Bounded retention of control allocations, never of reusable public identities.

use std::sync::Arc;

#[cfg(test)]
#[path = "../../tests/unit/storage.rs"]
mod tests;

// Reuse is opportunistic: bounded searches keep pinned handles and cold bursts
// from making every checkout walk the admission-sized cache under its lock.
const REUSE_PROBES: usize = 8;

/// Retains at most `limit` allocations. Only exclusive allocations can be reset:
/// even a weak accessor prevents reuse. Each checkout inspects at most
/// `REUSE_PROBES` entries, rotating across calls, then allocates on a miss.
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
        for _ in 0..self.entries.len().min(REUSE_PROBES) {
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
/// Checked-out reused buffers have at most twice the requested capacity, so
/// small live edge counts cannot accumulate arbitrarily oversized buffers.
pub(crate) struct BufferPool<T> {
    entries: Vec<Vec<T>>,
    capacity: usize,
    limit: usize,
    cursor: usize,
}

impl<T> BufferPool<T> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            entries: Vec::new(),
            capacity: 0,
            limit,
            cursor: 0,
        }
    }

    pub(crate) fn acquire(&mut self, minimum: usize) -> Vec<T> {
        if minimum == 0 {
            return Vec::new();
        }
        let maximum = minimum.saturating_mul(2);
        for _ in 0..self.entries.len().min(REUSE_PROBES) {
            let index = self.cursor;
            self.cursor = (index + 1) % self.entries.len();
            if (minimum..=maximum).contains(&self.entries[index].capacity()) {
                let entry = self.entries.swap_remove(index);
                self.capacity -= entry.capacity();
                // Revisit the swapped-in entry on the next checkout.
                self.cursor = if self.entries.is_empty() {
                    0
                } else {
                    index % self.entries.len()
                };
                return entry;
            }
        }
        Vec::with_capacity(minimum)
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
