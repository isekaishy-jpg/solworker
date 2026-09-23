use super::{GroupPool, SignalPool};
use crate::execution::group::SWGroup;
use crate::runtime::config::SWExecutionClass;
use crate::task::{SWOutcome, SWTask, SWTaskStatus};
use std::sync::{Arc, Barrier, Weak};

#[test]
fn retired_signal_reuses_only_after_result_and_observers_release() {
    let mut signals = SignalPool::new(2);
    let first = signals.acquire();
    let address = &*first as *const _;
    let (task, sink) = SWTask::pending_with_signal(first);
    let completion = task.completion();
    let subscription = completion.subscribe_cancelable(Box::new(|_| {}));
    sink.finish(SWOutcome::Success(41_u32), false);
    let shared = task.into_shared();
    let result = shared.try_result().unwrap();
    let other = signals.acquire();
    assert_ne!(&*other as *const _, address);
    assert_eq!(completion.status(), Some(SWTaskStatus::Succeeded));
    drop(other);
    drop(shared);
    drop(completion);
    // A detached subscription's weak reference still pins the old identity.
    let other = signals.acquire();
    assert_ne!(&*other as *const _, address);
    drop(other);
    drop(subscription);
    // The weak reference outlived the last strong owner, so that particular
    // allocation was discarded. Detached payload ownership remains valid.
    assert_eq!(signals.retired.entries.lock().unwrap().len(), 0);
    let next = signals.acquire();
    drop(next);
    assert!(matches!(&*result, SWOutcome::Success(41)));

    let reusable = signals.acquire();
    let reusable_address = &*reusable as *const _;
    let (task, sink) = SWTask::pending_with_signal(reusable);
    sink.finish(SWOutcome::Success(42_u32), false);
    let completion = task.completion();
    drop(task);
    assert_eq!(completion.status(), Some(SWTaskStatus::Succeeded));
    drop(completion);
    let recycled = signals.acquire();
    assert_eq!(&*recycled as *const _, reusable_address);
    let (next, sink) = SWTask::<u32>::pending_with_signal(recycled);
    assert_eq!(next.status(), None);
    sink.finish(SWOutcome::Success(43), false);
    assert_eq!(next.status(), Some(SWTaskStatus::Succeeded));
}

#[test]
fn retired_group_waits_for_seal_settlement_and_public_release() {
    let mut groups = GroupPool::new(2);
    let class = SWExecutionClass::High;
    let inner = groups.acquire(1, class);
    let address = Arc::as_ptr(&inner);
    let group = SWGroup::new(Arc::clone(&inner), Weak::new(), 0);
    let retained = group.clone();
    assert!(inner.add());
    let old = group.completion();
    group.seal();
    drop(group);
    let separate = groups.acquire(2, class);
    assert_ne!(Arc::as_ptr(&separate), address);
    drop(retained);
    inner.finish(Some(SWTaskStatus::Succeeded));
    assert_eq!(old.status(), Some(SWTaskStatus::Succeeded));
    let still_pinned = groups.acquire(3, class);
    assert_ne!(Arc::as_ptr(&still_pinned), address);
    drop(inner);
    let recycled = groups.acquire(4, class);
    assert_eq!(Arc::as_ptr(&recycled), address);
    assert_eq!(recycled.id, 4);
    assert_eq!(recycled.completion().status(), None);
    assert_eq!(old.status(), Some(SWTaskStatus::Succeeded));
}

#[test]
fn simultaneous_final_signal_observers_return_one_reusable_allocation() {
    let mut signals = SignalPool::new(1);
    let signal = signals.acquire();
    let address = &*signal as *const _;
    let (task, sink) = SWTask::pending_with_signal(signal);
    sink.finish(SWOutcome::Success(()), false);
    let first = task.completion();
    let second = first.clone();
    drop(task);

    let barrier = Arc::new(Barrier::new(3));
    std::thread::scope(|scope| {
        for completion in [first, second] {
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                drop(completion);
            });
        }
        barrier.wait();
    });
    let recycled = signals.acquire();
    assert_eq!(&*recycled as *const _, address);
}

#[test]
fn group_retirement_searches_entire_retired_list_and_caps_retention() {
    let mut groups = GroupPool::new(12);
    let class = SWExecutionClass::High;
    let mut pinned = Vec::new();
    for id in 0..9 {
        let inner = groups.acquire(id, class);
        let group = SWGroup::new(Arc::clone(&inner), Weak::new(), 0);
        group.seal();
        drop(group);
        pinned.push(inner);
    }
    let eligible = groups.acquire(9, class);
    let address = Arc::as_ptr(&eligible);
    let group = SWGroup::new(Arc::clone(&eligible), Weak::new(), 0);
    group.seal();
    drop(group);
    drop(eligible);
    assert_eq!(groups.retired.entries.lock().unwrap().len(), 10);
    let recycled = groups.acquire(10, class);
    assert_eq!(Arc::as_ptr(&recycled), address);

    let unsealed = groups.acquire(11, class);
    let group = SWGroup::new(Arc::clone(&unsealed), Weak::new(), 0);
    drop(group);
    drop(unsealed);
    assert_eq!(groups.retired.entries.lock().unwrap().len(), 9);

    for id in 12..30 {
        let inner = groups.acquire(id, class);
        let group = SWGroup::new(Arc::clone(&inner), Weak::new(), 0);
        group.seal();
        drop(group);
        pinned.push(inner);
    }
    assert!(groups.retired.entries.lock().unwrap().len() <= 12);
}
