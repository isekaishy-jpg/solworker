use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

#[test]
fn nested_leases_keep_backing_private_and_cache_only_empty_bounded_buffers() {
    let pool = ScratchPool::new();
    let mut outer = pool.acquire();
    outer.parents.extend(0..64);
    let outer_backing = outer.parents.as_ptr();
    let mut inner = pool.acquire();
    inner.parents.extend(64..128);
    assert_ne!(outer_backing, inner.parents.as_ptr());
    drop(inner);
    assert_eq!(outer.parents, (0..64).collect::<Vec<_>>());
    drop(outer);
    let recycled = pool.acquire();
    assert!(recycled.parents.is_empty());
    assert!(recycled.parents.capacity() >= 64);
    drop(recycled);

    let mut leases = (0..IDLE_COUNT + 1)
        .map(|_| pool.acquire())
        .collect::<Vec<_>>();
    for lease in &mut leases {
        lease.parents.reserve(128);
    }
    drop(leases);
    let idle = pool.idle.lock().unwrap();
    assert_eq!(idle.entries.iter().flatten().count(), IDLE_COUNT);
    assert!(idle.bytes <= IDLE_BYTES);
    drop(idle);

    let mut oversized = pool.acquire();
    oversized
        .parents
        .reserve(IDLE_BYTES / std::mem::size_of::<u64>() + 1);
    drop(oversized);
    let idle = pool.idle.lock().unwrap();
    assert!(
        idle.entries
            .iter()
            .flatten()
            .all(|entry| entry.bytes() <= IDLE_BYTES)
    );
    assert!(idle.bytes <= IDLE_BYTES);
}

struct ReenterOnDrop(Arc<ScratchPool>, Arc<AtomicUsize>);

impl Drop for ReenterOnDrop {
    fn drop(&mut self) {
        assert!(
            self.0.idle.try_lock().is_ok(),
            "owning cleanup runs unlocked"
        );
        let mut nested = self.0.acquire();
        nested.parents.push(17);
        self.1.fetch_add(1, Ordering::SeqCst);
        panic!("cleanup itself panics");
    }
}

#[test]
fn panic_cleanup_releases_owning_values_before_returning_storage() {
    let pool = Arc::new(ScratchPool::new());
    let observed = Arc::clone(&pool);
    let drops = Arc::new(AtomicUsize::new(0));
    let capture = ReenterOnDrop(Arc::clone(&pool), Arc::clone(&drops));
    let result = std::panic::catch_unwind(move || {
        let mut lease = pool.acquire();
        lease.finishes.push((0, Box::new(move || drop(capture))));
        panic!("unwind active preparation");
    });
    assert!(result.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&observed), 1);
    let idle = observed.idle.lock().unwrap();
    assert!(
        idle.entries
            .iter()
            .flatten()
            .all(|entry| entry.finishes.is_empty())
    );
}

#[test]
fn concurrent_and_contended_checkouts_never_share_active_storage() {
    let pool = Arc::new(ScratchPool::new());
    let held = pool.idle.lock().unwrap();
    let mut independent = pool.acquire();
    independent.parents.extend(0..64);
    drop(held);
    let barrier = Arc::new(Barrier::new(2));
    let child_pool = Arc::clone(&pool);
    let child_barrier = Arc::clone(&barrier);
    let child = std::thread::spawn(move || {
        let mut lease = child_pool.acquire();
        lease.parents.extend(64..128);
        let backing = lease.parents.as_ptr() as usize;
        child_barrier.wait();
        child_barrier.wait();
        assert_eq!(lease.parents, (64..128).collect::<Vec<_>>());
        backing
    });
    barrier.wait();
    assert_eq!(independent.parents, (0..64).collect::<Vec<_>>());
    barrier.wait();
    assert_ne!(independent.parents.as_ptr() as usize, child.join().unwrap());
}

#[test]
fn retirement_releases_idle_storage_and_rejects_late_active_returns() {
    let pool = ScratchPool::new();
    let mut active = pool.acquire();
    active.parents.reserve(64);
    let mut idle = pool.acquire();
    idle.parents.reserve(64);
    drop(idle);
    pool.retire();
    assert_eq!(pool.idle.lock().unwrap().bytes, 0);
    active.parents.push(23);
    drop(active);
    let mut late = pool.acquire();
    assert_eq!(late.parents.capacity(), 0);
    late.parents.reserve(64);
    drop(late);
    let idle = pool.idle.lock().unwrap();
    assert!(idle.retired);
    assert!(idle.entries.iter().all(Option::is_none));
    assert_eq!(idle.bytes, 0);
}
