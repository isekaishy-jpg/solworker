use super::{BufferPool, bucket_index};

#[test]
fn reuses_exact_size_class_and_keeps_checkouts_near_requested_capacity() {
    let mut buffers = BufferPool::<u64>::new(16);
    let first = buffers.acquire(3);
    assert_eq!(first.capacity(), 4);
    let address = first.as_ptr();
    buffers.release(first);
    let reused = buffers.acquire(3);
    assert_eq!(reused.as_ptr(), address);
    assert_eq!(buffers.capacity, 0);
    buffers.release(reused);

    // Capacity four must not back a one-slot request.
    let narrow = buffers.acquire(1);
    assert_eq!(narrow.capacity(), 1);
    assert_ne!(narrow.as_ptr(), address);
    assert_eq!(buffers.capacity, 4);
}

#[test]
fn descending_widths_keep_live_and_cached_capacity_bounded() {
    for edge_limit in [8, 1024] {
        let mut buffers = BufferPool::<u64>::new(edge_limit);
        let mut waiting = Vec::new();
        for width in (1..=edge_limit).rev() {
            assert!(waiting.len() + width <= edge_limit);
            let temporary = buffers.acquire(width);
            assert!(temporary.capacity() <= width * 2);
            buffers.release(temporary);
            assert!(buffers.capacity <= edge_limit);

            let narrow = buffers.acquire(1);
            assert!(narrow.capacity() <= 2);
            waiting.push(narrow);
        }
        assert!(waiting.iter().map(Vec::capacity).sum::<usize>() <= edge_limit * 2);
        for buffer in waiting {
            buffers.release(buffer);
            assert!(buffers.capacity <= edge_limit);
        }
    }
}

#[test]
fn retention_limit_and_noncanonical_returns_do_not_pollute_classes() {
    let mut buffers = BufferPool::<u64>::new(5);
    buffers.release(Vec::with_capacity(3)); // No exact size class.
    assert_eq!(buffers.capacity, 0);

    buffers.release(Vec::with_capacity(4));
    let two = Vec::with_capacity(2);
    let address = two.as_ptr();
    buffers.release(two); // Replaces the larger cached buffer.
    assert_eq!(buffers.capacity, 2);
    assert_eq!(buffers.buckets.iter().map(Vec::len).sum::<usize>(), 1);

    let reused = buffers.acquire(2);
    assert_eq!(reused.as_ptr(), address);
    assert_eq!(buffers.capacity, 0);
    buffers.release(reused);
    buffers.release(Vec::with_capacity(4)); // No larger buffer can be evicted.
    assert_eq!(buffers.capacity, 2);
}

#[test]
fn wide_cached_buffer_yields_to_repeated_small_reuse() {
    let mut buffers = BufferPool::<u64>::new(1024);
    let wide = buffers.acquire(1024);
    buffers.release(wide);
    assert_eq!(buffers.capacity, 1024);

    let small = buffers.acquire(1);
    let address = small.as_ptr();
    buffers.release(small);
    assert_eq!(buffers.capacity, 1);
    assert!(buffers.buckets[10].is_empty());

    for _ in 0..3 {
        let reused = buffers.acquire(1);
        assert_eq!(reused.as_ptr(), address);
        buffers.release(reused);
    }
}

#[test]
fn zero_and_overflow_class_boundaries_are_explicit() {
    assert_eq!(bucket_index(0), None);
    assert_eq!(bucket_index(1), Some(0));
    assert_eq!(bucket_index(3), Some(2));
    assert_eq!(
        bucket_index(1_usize << (usize::BITS - 1)),
        Some((usize::BITS - 1) as usize)
    );
    assert_eq!(bucket_index((1_usize << (usize::BITS - 1)) + 1), None);

    let mut buffers = BufferPool::<u64>::new(0);
    assert_eq!(buffers.acquire(0).capacity(), 0);
    buffers.release(Vec::new());
    assert_eq!(buffers.capacity, 0);
}

#[test]
#[should_panic(expected = "only detached subscription storage can recycle")]
fn nonempty_storage_cannot_be_returned() {
    let mut buffers = BufferPool::new(4);
    buffers.release(vec![1_u64]);
}
