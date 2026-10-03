use super::JobPool;
use crate::scheduler::{Decision, Envelope};
use crate::task::SWTaskStatus;
use std::sync::{Arc, Barrier, Weak};

fn empty_envelope() -> Envelope {
    Box::new(|_| -> crate::scheduler::Finish { Box::new(|| {}) })
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
