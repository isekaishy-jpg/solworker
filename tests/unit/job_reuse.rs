use super::JobPool;
use crate::scheduler::{Decision, Envelope};
use crate::task::SWTaskStatus;
use std::sync::mpsc;
use std::sync::{Arc, Barrier, Weak};
use std::time::Duration;

fn empty_envelope() -> Envelope {
    Box::new(|_| -> crate::scheduler::Finish { Box::new(|| {}) })
}

#[test]
fn batch_identity_preparation_allows_concurrent_recycler_return_and_cleans_raw_checkout_on_panic() {
    let pool = JobPool::new(4);
    let first = pool.prepare(1, Weak::new());
    let second = pool.prepare(2, Weak::new());
    drop((first, second));
    let retiring = pool.prepare(3, Weak::new());
    let (release_send, release_recv) = mpsc::channel();
    let (returned_send, returned_recv) = mpsc::channel();
    let mut jobs = Vec::new();
    let mut raw = super::JobCheckouts::default();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            release_recv.recv_timeout(Duration::from_secs(10)).unwrap();
            drop(retiring);
            returned_send.send(()).unwrap();
        });
        pool.prepare_many_into(
            (0..2_usize).map(|offset| {
                if offset == 0 {
                    // The actual identity iterator executes at the reset cut.
                    // A different control's final return must finish before
                    // preparation continues, without an arbitrary sleep.
                    release_send.send(()).unwrap();
                    returned_recv.recv_timeout(Duration::from_secs(10)).unwrap();
                }
                4 + offset as u64
            }),
            Weak::new(),
            &mut jobs,
            &mut raw,
        );
    });
    assert_eq!(jobs.iter().map(|job| job.id()).collect::<Vec<_>>(), [4, 5]);
    assert!(raw.entries.is_empty());
    assert_eq!(pool.recycled.lock().unwrap().free.len(), 1);
    jobs.clear();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.prepare_many_into(
            (0..3_usize).map(|offset| {
                assert!(pool.recycled.try_lock().is_ok());
                assert_ne!(offset, 1, "identity preparation failed");
                6 + offset as u64
            }),
            Weak::new(),
            &mut jobs,
            &mut raw,
        );
    }));
    assert!(result.is_err());
    assert!(
        raw.entries.is_empty(),
        "unused raw controls are disposed on unwind"
    );
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id(), 6);
    assert!(pool.recycled.lock().unwrap().free.is_empty());
    jobs.clear();
    pool.prepare_many_into([7, 8].into_iter(), Weak::new(), &mut jobs, &mut raw);
    assert_eq!(jobs.iter().map(|job| job.id()).collect::<Vec<_>>(), [7, 8]);
    assert!(raw.entries.is_empty());
}

fn settle(job: &super::JobHandle) {
    let finish = job
        .take_envelope()
        .expect("accepted job retains its envelope")
        .settle(Decision::Suppress(SWTaskStatus::Cancelled));
    finish();
}

#[test]
fn job_control_returns_only_after_backend_and_record_handles_release_it() {
    let pool = JobPool::new(2);
    let record = pool.acquire(1, Weak::new(), empty_envelope());
    let original = Arc::as_ptr(record.job.as_ref().unwrap());
    let backend = record.clone();
    settle(&record);
    drop(record);
    assert!(pool.recycled.lock().unwrap().free.is_empty());

    let overlapping = pool.acquire(2, Weak::new(), empty_envelope());
    assert_ne!(Arc::as_ptr(overlapping.job.as_ref().unwrap()), original);
    settle(&overlapping);
    drop(overlapping);
    drop(backend);
    assert_eq!(pool.recycled.lock().unwrap().free.len(), 2);

    let reused = pool.acquire(3, Weak::new(), empty_envelope());
    assert_eq!(Arc::as_ptr(reused.job.as_ref().unwrap()), original);
    assert_eq!(reused.id(), 3);
    settle(&reused);
}

#[test]
fn unsettled_controls_and_full_cache_are_discarded() {
    let pool = JobPool::new(1);
    let unsettled = pool.acquire(1, Weak::new(), empty_envelope());
    drop(unsettled);
    assert!(pool.recycled.lock().unwrap().free.is_empty());

    let first = pool.acquire(2, Weak::new(), empty_envelope());
    let first_address = Arc::as_ptr(first.job.as_ref().unwrap());
    let backend = first.clone();
    settle(&first);
    drop(first);

    let second = pool.acquire(3, Weak::new(), empty_envelope());
    let second_address = Arc::as_ptr(second.job.as_ref().unwrap());
    assert_ne!(first_address, second_address);
    settle(&second);
    drop(second);
    drop(backend);
    assert_eq!(pool.recycled.lock().unwrap().free.len(), 1);

    let reused = pool.acquire(4, Weak::new(), empty_envelope());
    assert_eq!(Arc::as_ptr(reused.job.as_ref().unwrap()), second_address);
    settle(&reused);
}

#[test]
fn concurrent_final_releases_return_one_exclusive_control() {
    let pool = JobPool::new(1);
    let first = pool.acquire(1, Weak::new(), empty_envelope());
    let address = Arc::as_ptr(first.job.as_ref().unwrap());
    settle(&first);
    let second = first.clone();
    let barrier = Arc::new(Barrier::new(2));
    std::thread::scope(|scope| {
        let other_barrier = Arc::clone(&barrier);
        scope.spawn(move || {
            barrier.wait();
            drop(first);
        });
        scope.spawn(move || {
            other_barrier.wait();
            drop(second);
        });
    });

    assert_eq!(pool.recycled.lock().unwrap().free.len(), 1);
    let reused = pool.acquire(2, Weak::new(), empty_envelope());
    assert_eq!(Arc::as_ptr(reused.job.as_ref().unwrap()), address);
    settle(&reused);
}

#[test]
fn weak_membership_does_not_retain_settled_jobs_or_retarget_their_identity() {
    let pool = JobPool::new(1);
    let first = pool.acquire(1, Weak::new(), empty_envelope());
    let old_address = Arc::as_ptr(first.job.as_ref().unwrap());
    let membership = first.downgrade();
    assert_eq!(membership.upgrade().unwrap().id(), 1);
    settle(&first);
    drop(first);
    assert!(membership.upgrade().is_none());
    assert!(pool.recycled.lock().unwrap().free.is_empty());

    let next = pool.acquire(2, Weak::new(), empty_envelope());
    let new_address = Arc::as_ptr(next.job.as_ref().unwrap());
    assert_ne!(new_address, old_address);
    assert!(membership.upgrade().is_none());
    assert_eq!(next.id(), 2);
    settle(&next);
    drop(next);
    drop(membership);
    let reused = pool.acquire(3, Weak::new(), empty_envelope());
    assert_eq!(Arc::as_ptr(reused.job.as_ref().unwrap()), new_address);
    assert_eq!(reused.id(), 3);
    settle(&reused);
}
