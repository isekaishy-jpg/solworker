use super::{GroupPool, SignalPool};
use crate::execution::group::SWGroup;
use crate::runtime::config::SWExecutionClass;
use crate::task::{SWOutcome, SWTask, SWTaskStatus};
use std::sync::{Arc, Barrier, Weak};
use std::time::{Duration, Instant};

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
    let groups = GroupPool::new(2);
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
    let groups = GroupPool::new(12);
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

#[test]
fn group_checkout_does_not_hold_scheduler_state() {
    use crate::runtime::SWRuntime;
    use crate::runtime::config::{SWRuntimeConfig, SWWorkerConfig};
    use crate::scheduler::SWOwnedLimits;

    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(1, 0, [1; 3], [1; 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(limits)
        .build()
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::High);
    let seed = lane.group().unwrap();
    let scheduler = seed.scheduler.upgrade().unwrap();
    seed.seal();
    drop(seed);
    let previous_id = scheduler.lock().next_group;
    let retired_guard = scheduler.groups.retired.entries.lock().unwrap();
    let checkout = std::thread::spawn(move || lane.group());

    // Observe reservation through the actual public admission path. A checkout
    // holding scheduler state while blocked on the recycler fails this check.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut state_available = false;
    while Instant::now() < deadline {
        if let Ok(state) = scheduler.state.try_lock()
            && state.next_group > previous_id
        {
            state_available = true;
            break;
        }
        std::thread::yield_now();
    }
    let admission_retained = if state_available {
        runtime.begin_shutdown();
        !runtime.try_shutdown().unwrap()
    } else {
        false
    };
    // Release even on regression, so failure cannot strand the checkout thread.
    drop(retired_guard);
    let group = checkout.join().unwrap().unwrap();
    group.seal();
    drop(group);
    runtime.shutdown().unwrap();
    assert!(state_available);
    assert!(admission_retained);
}

#[test]
fn signal_publication_detachment_and_final_release_can_overlap_teardown() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    for teardown in [false, true] {
        let mut signals = SignalPool::new(1);
        let (task, sink) = SWTask::pending_with_signal(signals.acquire());
        let completion = task.completion();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = Arc::clone(&calls);
        let subscription = completion.subscribe_cancelable(Box::new(move |status| {
            assert_eq!(status, SWTaskStatus::Succeeded);
            observed_calls.fetch_add(1, Ordering::SeqCst);
        }));
        drop(task);
        let mut pool = Some(signals);
        let barrier = Arc::new(Barrier::new(4));
        std::thread::scope(|scope| {
            let publish_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                publish_barrier.wait();
                sink.finish(SWOutcome::Success(()), false);
            });
            let detach_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                detach_barrier.wait();
                drop(subscription);
            });
            let release_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                release_barrier.wait();
                drop(completion);
            });
            barrier.wait();
            if teardown {
                drop(pool.take());
            }
        });
        assert!(calls.load(Ordering::SeqCst) <= 1);
        if let Some(mut signals) = pool {
            let (next, sink) = SWTask::<()>::pending_with_signal(signals.acquire());
            assert_eq!(next.status(), None);
            let previous_calls = calls.load(Ordering::SeqCst);
            sink.finish(SWOutcome::Cancelled, false);
            assert_eq!(next.status(), Some(SWTaskStatus::Cancelled));
            assert_eq!(calls.load(Ordering::SeqCst), previous_calls);
        }
    }
}
