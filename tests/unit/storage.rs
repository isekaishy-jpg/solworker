use super::{ArcPool, BufferPool};
use crate::execution::group::{GroupInner, SWGroup};
use crate::runtime::config::SWExecutionClass;
use crate::task::{SWOutcome, SWTask, SWTaskStatus, Signal};
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
}
