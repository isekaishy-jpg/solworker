use std::cell::Cell;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use solworker::{SWExecutionClass, SWExecutionError, SWRuntime, SWRuntimeConfig, SWWorkerConfig};

fn runtime() -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    SWRuntime::builder(config).build().unwrap()
}

#[test]
fn discarded_panic_payloads_cannot_escape_the_executor() {
    const CHILD_ENV: &str = "SOLWORKER_PANIC_DISPOSAL_CHILD";
    if std::env::var(CHILD_ENV).as_deref() == Ok("1") {
        struct HostilePanic;
        impl Drop for HostilePanic {
            fn drop(&mut self) {
                std::panic::panic_any(HostilePanic);
            }
        }
        let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
        let mut runtime = SWRuntime::builder(config)
            .with_owned_limits(solworker::SWOwnedLimits::new(4, 4, [4; 3], [2; 3]).unwrap())
            .build()
            .unwrap();
        let lane = runtime.lane(SWExecutionClass::Mid);
        let mut values = [0_u8; 2];
        for readonly in [false, true] {
            let result = if readonly {
                lane.for_each_read_chunk(&values, NonZeroUsize::new(1).unwrap(), |_, _| {
                    std::panic::panic_any(HostilePanic)
                })
                .unwrap()
            } else {
                lane.for_each_chunk(&mut values, NonZeroUsize::new(1).unwrap(), |_, _| {
                    std::panic::panic_any(HostilePanic)
                })
                .unwrap()
            };
            // The first payload belongs to the caller. Test only runtime disposal.
            std::mem::forget(result.unwrap_err());
        }
        let (owned, _) = lane
            .try_spawn(solworker::SWSpawnOptions::default(), || {
                std::panic::panic_any(HostilePanic)
            })
            .unwrap();
        assert_eq!(
            owned
                .completion()
                .wait_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            Some(solworker::SWTaskStatus::Panicked)
        );
        runtime.shutdown().unwrap();
        return;
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "discarded_panic_payloads_cannot_escape_the_executor",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "panic disposal child failed: {status}");
            break;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("panic disposal child timed out");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
fn join_borrows_and_settles_both_branches_after_panic() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::High);
    let input = [5, 8, 13];
    let completed = AtomicUsize::new(0);
    let (left, right) = lane
        .join(
            || -> usize {
                assert_eq!(input[0], 5);
                completed.fetch_add(1, Ordering::SeqCst);
                panic!("left");
            },
            || {
                completed.fetch_add(1, Ordering::SeqCst);
                input[1] + input[2]
            },
        )
        .unwrap();
    assert!(left.is_err());
    assert_eq!(right.unwrap(), 21);
    assert_eq!(completed.load(Ordering::SeqCst), 2);
    runtime.shutdown().unwrap();
}

#[test]
fn owner_branch_stays_on_caller_and_need_not_be_send() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::High);
    let caller = thread::current().id();
    let local = Rc::new(Cell::new(0usize));
    let (worker, owner) = lane
        .join_with_owner(
            || 17usize,
            || {
                assert_eq!(thread::current().id(), caller);
                local.set(local.get() + 1);
                Rc::clone(&local)
            },
        )
        .unwrap();
    assert_eq!(worker.unwrap(), 17);
    assert!(Rc::ptr_eq(&owner.unwrap(), &local));
    assert_eq!(local.get(), 1);
    runtime.shutdown().unwrap();
}

#[test]
fn owner_panic_still_settles_worker() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::Mid);
    let completed = AtomicUsize::new(0);
    let (worker, owner) = lane
        .join_with_owner(
            || {
                completed.fetch_add(1, Ordering::SeqCst);
                4
            },
            || -> () { panic!("owner") },
        )
        .unwrap();
    assert_eq!(worker.unwrap(), 4);
    assert!(owner.is_err());
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    runtime.shutdown().unwrap();
}

#[test]
fn mutable_chunks_have_stable_indices_and_all_settle_after_item_panic() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::Low);
    let mut values = [0usize; 10];
    let visited = std::array::from_fn::<_, 4, _>(|_| AtomicUsize::new(0));
    let result = lane
        .for_each_chunk(
            &mut values,
            NonZeroUsize::new(3).unwrap(),
            |start, chunk| {
                for value in chunk {
                    *value = start + 1;
                }
                visited[start / 3].fetch_add(1, Ordering::SeqCst);
                if start == 3 {
                    panic!("one item");
                }
            },
        )
        .unwrap();
    assert!(result.is_err());
    assert_eq!(visited.map(|count| count.load(Ordering::SeqCst)), [1; 4]);
    assert_eq!(values, [1, 1, 1, 4, 4, 4, 7, 7, 7, 10]);

    let empty: [usize; 0] = [];
    let count = AtomicUsize::new(0);
    lane.for_each_read_chunk(&empty, NonZeroUsize::new(1).unwrap(), |_, _| {
        count.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap()
    .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    runtime.shutdown().unwrap();
}

#[test]
fn readonly_chunks_preserve_original_start_indices() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::Mid);
    let values = [1usize, 2, 3, 4, 5];
    let observed = std::sync::Mutex::new(Vec::new());
    lane.for_each_read_chunk(&values, NonZeroUsize::new(2).unwrap(), |start, chunk| {
        observed.lock().unwrap().push((start, chunk.to_vec()));
    })
    .unwrap()
    .unwrap();
    let mut observed = observed.into_inner().unwrap();
    observed.sort_by_key(|(start, _)| *start);
    assert_eq!(observed, [(0, vec![1, 2]), (2, vec![3, 4]), (4, vec![5])]);
    runtime.shutdown().unwrap();
}

#[test]
fn nested_context_routes_and_rejection_preserves_work() {
    let mut other = runtime();
    let mut runtime = runtime();
    let high = runtime.lane(SWExecutionClass::High);
    let mid = runtime.lane(SWExecutionClass::Mid);
    let foreign_high = other.lane(SWExecutionClass::High);
    let invoked = AtomicUsize::new(0);

    let (same_lane, rejections) = high
        .join(
            || {
                let (left, right) = high.join(|| 2, || 3).unwrap();
                left.unwrap() + right.unwrap()
            },
            || {
                let cross_lane = mid
                    .join(
                        || invoked.fetch_add(1, Ordering::SeqCst),
                        || invoked.fetch_add(1, Ordering::SeqCst),
                    )
                    .unwrap_err();
                assert_eq!(cross_lane.reason, SWExecutionError::InvalidContext);
                let foreign = foreign_high
                    .join_with_owner(
                        || invoked.fetch_add(1, Ordering::SeqCst),
                        || invoked.fetch_add(1, Ordering::SeqCst),
                    )
                    .unwrap_err();
                assert_eq!(foreign.reason, SWExecutionError::InvalidContext);
                (cross_lane, foreign)
            },
        )
        .unwrap();
    assert_eq!(same_lane.unwrap(), 5);
    let (cross_lane, foreign) = rejections.unwrap();
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    (cross_lane.left)();
    (cross_lane.right)();
    (foreign.left)();
    (foreign.right)();
    assert_eq!(invoked.load(Ordering::SeqCst), 4);

    other.shutdown().unwrap();
    runtime.shutdown().unwrap();

    let closed = high.join(|| 7, || 11).unwrap_err();
    assert_eq!(closed.reason, SWExecutionError::Closed);
    assert_eq!((closed.left)(), 7);
    assert_eq!((closed.right)(), 11);

    let mut values = [0usize; 2];
    let rejected = high
        .for_each_chunk(&mut values, NonZeroUsize::new(1).unwrap(), |_, chunk| {
            chunk[0] = 9;
        })
        .unwrap_err();
    assert_eq!(rejected.reason, SWExecutionError::Closed);
    assert_eq!(values, [0, 0]);
    (rejected.operation)(0, &mut values[..1]);
    assert_eq!(values, [9, 0]);
}
