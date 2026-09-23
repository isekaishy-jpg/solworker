use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use micropool::ThreadPoolBuilder;

use super::MicropoolBackend;

fn backend() -> MicropoolBackend {
    MicropoolBackend::try_build_with(1, |_, worker| Ok::<_, ()>(std::thread::spawn(worker)))
        .unwrap()
}

#[test]
fn one_worker_unit_is_advertised_before_owner_runs() {
    let pool = backend();
    let borrowed = AtomicBool::new(false);
    let (started_tx, started_rx) = mpsc::channel();

    let (worker, owner) = pool.with_context(|| {
        pool.join_with_owner(
            || {
                borrowed.store(true, Ordering::Release);
                started_tx.send(()).unwrap();
                17
            },
            || {
                started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                borrowed.load(Ordering::Acquire)
            },
        )
    });

    assert_eq!((worker, owner), (17, true));
    pool.stop_and_join();
}

#[test]
fn nested_owner_overlap_helps_same_lane_and_settles_after_owner_panic() {
    let pool = backend();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let nested_ran = AtomicBool::new(false);

    let (worker, owner) = pool.with_context(|| {
        pool.join_with_owner(
            move || {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                11
            },
            || {
                started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                let (nested_worker, nested_owner) = pool.join_with_owner(
                    || {
                        nested_ran.store(true, Ordering::Release);
                        23
                    },
                    || catch_unwind(AssertUnwindSafe(|| panic!("owner failure"))),
                );
                release_tx.send(()).unwrap();
                (nested_worker, nested_owner)
            },
        )
    });

    assert_eq!(worker, 11);
    assert_eq!(owner.0, 23);
    assert!(owner.1.is_err());
    assert!(nested_ran.load(Ordering::Acquire));
    pool.stop_and_join();
}

#[test]
fn saturated_nested_overlap_runs_owner_then_worker_on_caller() {
    let pool = ThreadPoolBuilder::default()
        .num_threads(1)
        .max_jobs(1)
        .build();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let stage = std::sync::atomic::AtomicU8::new(0);
    let caller = std::thread::current().id();

    let (outer_worker, inner) = pool.with_pool_context(|| {
        pool.join_with_owner(
            move || {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                5
            },
            || {
                started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                let result = pool.join_with_owner(
                    || {
                        assert_eq!(std::thread::current().id(), caller);
                        assert_eq!(stage.load(Ordering::Acquire), 1);
                        stage.store(2, Ordering::Release);
                        7
                    },
                    || {
                        stage.store(1, Ordering::Release);
                        9
                    },
                );
                release_tx.send(()).unwrap();
                result
            },
        )
    });

    assert_eq!((outer_worker, inner), (5, (7, 9)));
    assert_eq!(stage.load(Ordering::Acquire), 2);
    pool.stop_and_join();
}

#[cfg(target_pointer_width = "64")]
#[test]
fn oversized_index_count_starts_at_zero_in_serial_fallback() {
    let pool = backend();
    let called = AtomicBool::new(false);
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        pool.invoke_indexed(i64::MAX as usize + 1, |index| {
            if index == 0 {
                called.store(true, Ordering::Release);
            }
            panic!("stop after the first index");
        });
    }));

    assert!(outcome.is_err());
    assert!(called.load(Ordering::Acquire));
    pool.stop_and_join();
}
