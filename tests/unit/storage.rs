use super::{ArcPool, BufferPool, REUSE_PROBES};
use crate::execution::group::{GroupInner, SWGroup};
use crate::runtime::config::SWExecutionClass;
use crate::task::{SWOutcome, SWTask, SWTaskStatus, Signal};
use std::cell::Cell;
use std::sync::{Arc, Weak};

#[test]
fn control_storage_recycles_without_resetting_retained_results_or_weak_accessors() {
    let mut signals = ArcPool::new(1);
    let first = signals.acquire(Signal::new, Signal::reset);
    let address = Arc::as_ptr(&first);
    let weak = Arc::downgrade(&first);
    let (task, sink) = SWTask::pending_with_signal(first);
    let completion = task.completion();
    sink.finish(SWOutcome::Success(41_u32), false);
    let shared = task.into_shared();
    let result = shared.try_result().unwrap();
    let other = signals.acquire(Signal::new, Signal::reset);
    assert_ne!(Arc::as_ptr(&other), address);
    assert_eq!(signals.entries.len(), 1); // Busy capacity doesn't grow the cache.
    assert_eq!(completion.status(), Some(SWTaskStatus::Succeeded));
    drop((other, shared, completion));
    let other = signals.acquire(Signal::new, Signal::reset);
    assert_ne!(Arc::as_ptr(&other), address); // Weak callbacks also pin identity.
    drop((other, weak));
    let recycled = signals.acquire(Signal::new, Signal::reset);
    assert_eq!(Arc::as_ptr(&recycled), address);
    let (next, sink) = SWTask::<u32>::pending_with_signal(recycled);
    assert_eq!(next.status(), None);
    sink.finish(SWOutcome::Success(42), false);
    // Detached immutable payloads don't prevent control reuse or change value.
    assert!(matches!(&*result, SWOutcome::Success(41)));
    assert_eq!(next.status(), Some(SWTaskStatus::Succeeded));
}

#[test]
fn group_and_edge_storage_reuse_preserves_old_completion_and_bounds_retention() {
    let mut groups = ArcPool::new(1);
    let class = SWExecutionClass::High;
    let acquire = |pool: &mut ArcPool<GroupInner>, id| {
        pool.acquire_where(
            || GroupInner::new(id, class),
            |group| group.reset(id, class),
            GroupInner::is_complete,
        )
    };
    let group = acquire(&mut groups, 1);
    let address = Arc::as_ptr(&group);
    assert!(group.add());
    let old = group.completion();
    group.finish(Some(SWTaskStatus::Succeeded));
    assert_eq!(old.status(), None);
    // An unsealed wave must never be recycled (even if its public handle is gone).
    drop(group);
    let separate = acquire(&mut groups, 2);
    assert_ne!(Arc::as_ptr(&separate), address);
    let original = Arc::clone(&groups.entries[0]);
    SWGroup::new(Arc::clone(&original), Weak::new(), 0).seal();
    assert_eq!(old.status(), Some(SWTaskStatus::Succeeded));
    drop(original);
    let recycled = acquire(&mut groups, 3);
    assert_eq!(Arc::as_ptr(&recycled), address);
    assert_eq!(recycled.id, 3);
    assert_eq!(recycled.completion().status(), None);
    assert_eq!(old.status(), Some(SWTaskStatus::Succeeded));
    assert!(recycled.add());
    SWGroup::new(Arc::clone(&recycled), Weak::new(), 0).seal();
    recycled.finish(Some(SWTaskStatus::Abandoned));
    assert_eq!(
        recycled.completion().status(),
        Some(SWTaskStatus::PrerequisiteFailed)
    );

    let mut buffers = BufferPool::<u64>::new(4);
    let buffer = buffers.acquire(4);
    let address = buffer.as_ptr();
    buffers.release(buffer);
    let buffer = buffers.acquire(2);
    assert_eq!(buffer.as_ptr(), address);
    assert_eq!(buffers.capacity, 0);
    buffers.release(buffer);
    buffers.release(Vec::with_capacity(8));
    assert_eq!(buffers.capacity, 4);
    assert_eq!(buffers.entries.len(), 1);

    // A descending sequence of wide temporary jobs followed by waiting
    // one-edge jobs used to accumulate E * (E + 1) / 2 backing slots.
    for edge_limit in [8, 1024] {
        let mut buffers = BufferPool::<u64>::new(edge_limit);
        let mut waiting = Vec::new();
        for width in (1..=edge_limit).rev() {
            assert!(waiting.len() + width <= edge_limit);
            let temporary = buffers.acquire(width);
            assert!(temporary.capacity() <= 2 * width);
            buffers.release(temporary);
            assert!(buffers.capacity <= edge_limit);
            let narrow = buffers.acquire(1);
            assert!(narrow.capacity() <= 2);
            waiting.push(narrow);
        }
        assert!(waiting.iter().map(Vec::capacity).sum::<usize>() <= 2 * edge_limit);
        for buffer in waiting {
            buffers.release(buffer);
            assert!(buffers.capacity <= edge_limit);
        }
    }
}

#[test]
fn reuse_searches_are_bounded_and_rotate_past_unavailable_entries() {
    let count = REUSE_PROBES * 4;
    let mut pool = ArcPool::new(count);
    let retained: Vec<_> = (0..count).map(|id| pool.acquire(|| id, |_| {})).collect();
    // Full pinned cache: allocating on a miss must not inspect every entry.
    let cursor = pool.cursor;
    let extra = pool.acquire(|| usize::MAX, |_| {});
    assert_eq!(pool.cursor, (cursor + REUSE_PROBES) % count);
    assert_eq!(pool.entries.len(), count);
    assert!(retained.iter().all(|entry| !Arc::ptr_eq(entry, &extra)));
    drop(retained);

    // Exclusive but ineligible entries (e.g. unsealed groups) also have bounded
    // inspection. Rotation eventually finds a reusable entry beyond one probe.
    pool.cursor = 0;
    let wanted = REUSE_PROBES * 2 + 1;
    let checks = Cell::new(0);
    let mut found = false;
    for _ in 0..4 {
        let before = checks.get();
        let value = pool.acquire_where(
            || usize::MAX,
            |_| {},
            |value| {
                checks.set(checks.get() + 1);
                *value == wanted
            },
        );
        assert!(checks.get() - before <= REUSE_PROBES);
        if *value == wanted {
            found = true;
            break;
        }
    }
    assert!(found);
    assert!(checks.get() > REUSE_PROBES);

    // Buffer size filtering must not introduce a new full-cache scan.
    let mut buffers = BufferPool::<u64>::new(count * 4);
    let mut wanted_address = std::ptr::null();
    for index in 0..count {
        let buffer = Vec::with_capacity(if index == wanted { 2 } else { 4 });
        if index == wanted {
            wanted_address = buffer.as_ptr();
        }
        buffers.release(buffer);
    }
    let first = buffers.acquire(1);
    assert_eq!(buffers.cursor, REUSE_PROBES);
    assert_ne!(first.as_ptr(), wanted_address);
    let mut found = false;
    for _ in 0..4 {
        let buffer = buffers.acquire(1);
        if buffer.as_ptr() == wanted_address {
            found = true;
            break;
        }
    }
    assert!(found);
    // Empty/single-entry cache removal and subsequent insertion keep a valid cursor.
    let mut single = BufferPool::<u64>::new(4);
    single.release(Vec::with_capacity(4));
    let buffer = single.acquire(2);
    assert_eq!(single.cursor, 0);
    single.release(buffer);
    assert_eq!(single.acquire(2).capacity(), 4);

    // Removing the last slot of a nonempty cache must wrap to a valid entry,
    // including when the next release appends a different-sized buffer.
    let mut tail = BufferPool::<u64>::new(16);
    tail.release(Vec::with_capacity(4));
    tail.release(Vec::with_capacity(8));
    tail.cursor = 1;
    let large = tail.acquire(8);
    assert_eq!(large.capacity(), 8);
    assert_eq!(tail.capacity, 4);
    assert_eq!(tail.cursor, 0);
    tail.release(large);
    assert_eq!(tail.acquire(2).capacity(), 4);
    assert_eq!(tail.acquire(8).capacity(), 8);
    assert_eq!(tail.capacity, 0);
    assert!(tail.entries.is_empty());
}
