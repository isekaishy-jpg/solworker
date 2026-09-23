//! Bounded reuse of empty subscription buffers by backing capacity.

#[cfg(test)]
#[path = "../../../tests/unit/buffers.rs"]
mod tests;

/// Each bucket holds buffers with one exact power-of-two capacity. A checkout
/// therefore takes a suitable buffer directly, without probing unrelated
/// capacities. Noncanonical returned capacities are dropped.
pub(crate) struct BufferPool<T> {
    buckets: Vec<Vec<Vec<T>>>,
    capacity: usize,
    limit: usize,
}

impl<T> BufferPool<T> {
    pub(crate) fn new(limit: usize) -> Self {
        let mut buckets = Vec::with_capacity(usize::BITS as usize);
        buckets.resize_with(usize::BITS as usize, Vec::new);
        Self {
            buckets,
            capacity: 0,
            limit,
        }
    }

    pub(crate) fn acquire(&mut self, minimum: usize) -> Vec<T> {
        if std::mem::size_of::<T>() == 0 || minimum == 0 {
            return Vec::new();
        }
        let Some(index) = bucket_index(minimum) else {
            // No representable power-of-two class exists for this request.
            return Vec::with_capacity(minimum);
        };
        if let Some(entry) = self.buckets[index].pop() {
            self.capacity -= entry.capacity();
            return entry;
        }
        Vec::with_capacity(1_usize << index)
    }

    pub(crate) fn release(&mut self, entry: Vec<T>) {
        assert!(
            entry.is_empty(),
            "only detached subscription storage can recycle"
        );
        if std::mem::size_of::<T>() == 0 {
            return;
        }
        let capacity = entry.capacity();
        if capacity == 0 || capacity > self.limit {
            return;
        }
        if let Some(index) = bucket_index(capacity)
            && 1_usize << index == capacity
        {
            if capacity > self.limit - self.capacity {
                if !self.buckets[index].is_empty() {
                    return;
                }
                // A cold, wider class must not monopolize the retention budget
                // when an absent narrower class starts returning buffers. One
                // larger entry always frees enough room for this entry.
                let Some(larger_index) = ((index + 1)..self.buckets.len())
                    .rev()
                    .find(|&larger_index| !self.buckets[larger_index].is_empty())
                else {
                    return;
                };
                let evicted = self.buckets[larger_index]
                    .pop()
                    .expect("selected bucket contains a buffer");
                self.capacity -= evicted.capacity();
            }
            self.capacity += capacity;
            self.buckets[index].push(entry);
        }
    }
}

fn bucket_index(minimum: usize) -> Option<usize> {
    if minimum == 0 {
        return None;
    }
    minimum
        .checked_next_power_of_two()
        .map(|capacity| capacity.trailing_zeros() as usize)
}
