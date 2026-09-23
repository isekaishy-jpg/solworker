use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

use solworker::{
    SWCost, SWExternalAccessOptions, SWLimits, SWOwnedLimits, SWRuntime, SWRuntimeConfig,
    SWSpawnError, SWWorkerConfig,
};

struct DropFlag(Arc<AtomicUsize>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn runtime() -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(16, 16, [8; 3], [8; 3]).unwrap();
    SWRuntime::builder(config)
        .with_owned_limits(limits)
        .with_external_capacity(NonZeroUsize::new(4).unwrap())
        .build()
        .unwrap()
}

#[test]
fn physical_destruction_is_guarded_and_settles_accounting_even_on_unwind() {
    type Observation = (
        usize,
        Result<solworker::SWTaskStatus, solworker::SWWaitError>,
    );
    struct Probe {
        set: solworker::SWWorkSet,
        observation: Arc<std::sync::Mutex<Option<Observation>>>,
        panic: bool,
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            let active = self.set.progress().active_work;
            let wait = solworker::SWTask::ready(()).completion().wait();
            *self.observation.lock().unwrap() = Some((active, wait));
            if self.panic {
                panic!("physical resource destructor");
            }
        }
    }
    for active in [false, true] {
        for panic in [false, true] {
            let mut runtime = runtime();
            let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
            let observation = Arc::new(std::sync::Mutex::new(None));
            let prepared = runtime
                .prepare_external(
                    Probe {
                        set: set.clone(),
                        observation: Arc::clone(&observation),
                        panic,
                    },
                    SWExternalAccessOptions {
                        work_set: Some(&set),
                        ..Default::default()
                    },
                )
                .unwrap();
            set.seal();
            let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if active {
                    // No foreign accessor was exposed; this thread may destroy T.
                    unsafe { prepared.activate().unwrap().release() };
                } else {
                    drop(prepared);
                }
            }));
            assert_eq!(cleanup.is_err(), panic);
            assert_eq!(
                observation.lock().unwrap().take(),
                Some((1, Err(solworker::SWWaitError::ExecutionContext)))
            );
            assert!(set.is_drained());
            assert_eq!(runtime.external_progress().active, 0);
            assert_eq!(runtime.external_progress().prepared, 0);
            // The participation guard must also unwind correctly.
            assert_eq!(
                solworker::SWTask::ready(()).completion().wait(),
                Ok(solworker::SWTaskStatus::Succeeded)
            );
            runtime.shutdown().unwrap();
        }
    }
}

#[test]
fn acknowledged_release_transfers_resource_after_physical_count_ends() {
    let mut runtime = runtime();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut prepared = runtime
        .prepare_external(
            DropFlag(Arc::clone(&dropped)),
            SWExternalAccessOptions::default(),
        )
        .ok()
        .unwrap();
    assert_eq!(prepared.get().0.load(Ordering::SeqCst), 0);
    prepared.get_mut().0.fetch_add(0, Ordering::SeqCst);
    assert_eq!(runtime.external_progress().prepared, 1);

    let active = prepared.activate().ok().unwrap();
    assert_eq!(runtime.external_progress().active, 1);
    let pointer = unsafe { active.as_mut_ptr() };
    assert!(!pointer.is_null());

    let retained = unsafe { active.acknowledge_release() };
    assert_eq!(runtime.external_progress().active, 0);
    assert_eq!(runtime.external_progress().orphaned, 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(retained);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    runtime.shutdown().unwrap();
}

#[test]
fn prepared_resource_is_returned_if_activation_closes() {
    let mut runtime = runtime();
    let dropped = Arc::new(AtomicUsize::new(0));
    let prepared = runtime
        .prepare_external(
            DropFlag(Arc::clone(&dropped)),
            SWExternalAccessOptions::default(),
        )
        .ok()
        .unwrap();
    runtime.abandon();
    let rejected = prepared.activate().err().unwrap();
    assert_eq!(rejected.reason, SWSpawnError::Closed);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(rejected.prepared.into_retained());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.external_progress().prepared, 0);
}

#[test]
fn lost_ticket_orphans_storage_even_after_runtime_drop() {
    let mut runtime = runtime();
    let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let active = runtime
        .prepare_external(
            DropFlag(Arc::clone(&dropped)),
            SWExternalAccessOptions {
                work_set: Some(&set),
                ..Default::default()
            },
        )
        .ok()
        .unwrap()
        .activate()
        .ok()
        .unwrap();
    set.cancel();
    runtime.abandon();
    assert_eq!(runtime.external_progress().active, 1);
    assert_eq!(set.progress().active_work, 1);
    drop(active);
    assert_eq!(runtime.external_progress().orphaned, 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(runtime);
    drop(set);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
}

#[test]
fn cancelled_set_rejects_prepared_activation_and_returns_resource() {
    let mut runtime = runtime();
    let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let prepared = runtime
        .prepare_external(
            DropFlag(Arc::clone(&dropped)),
            SWExternalAccessOptions {
                work_set: Some(&set),
                ..Default::default()
            },
        )
        .ok()
        .unwrap();
    set.cancel();
    let rejected = prepared.activate().err().unwrap();
    assert_eq!(rejected.reason, SWSpawnError::Closed);
    drop(rejected.prepared.into_retained());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(set.progress().active_work, 0);
    runtime.abandon();
}

#[test]
fn bytes_and_set_work_follow_resource_through_handoff() {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let owned = SWOwnedLimits::new(16, 16, [8; 3], [8; 3]).unwrap();
    let capacity = SWLimits::new(SWCost::new(4, 0, 0, 64), SWCost::default(), 0, None).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(owned)
        .with_capacity_limits(capacity)
        .with_external_capacity(NonZeroUsize::new(4).unwrap())
        .build()
        .unwrap();
    let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let options = SWExternalAccessOptions {
        work_set: Some(&set),
        cost: SWCost::new(1, 0, 0, 16),
        ..Default::default()
    };
    let active = runtime
        .prepare_external(vec![1_u8; 16], options)
        .ok()
        .unwrap()
        .activate()
        .ok()
        .unwrap();
    set.seal();
    assert_eq!(set.progress().active_work, 1);
    assert_eq!(runtime.capacity_usage().unwrap().ordinary.bytes, 16);

    let retained = unsafe { active.acknowledge_release() };
    assert!(set.is_drained());
    assert_eq!(runtime.capacity_usage().unwrap().ordinary.bytes, 16);
    assert_eq!(retained.len(), 16);
    drop(retained);
    assert_eq!(runtime.capacity_usage().unwrap().ordinary.bytes, 0);
    runtime.shutdown().unwrap();
}

#[test]
fn physical_cleanup_may_finish_on_another_thread() {
    struct BlockingDrop {
        started: mpsc::Sender<()>,
        finish: mpsc::Receiver<()>,
    }
    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            self.started.send(()).unwrap();
            self.finish.recv().unwrap();
        }
    }

    let mut runtime = runtime();
    let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let (started_send, started_recv) = mpsc::channel();
    let (finish_send, finish_recv) = mpsc::channel();
    let active = runtime
        .prepare_external(
            BlockingDrop {
                started: started_send,
                finish: finish_recv,
            },
            SWExternalAccessOptions {
                work_set: Some(&set),
                ..Default::default()
            },
        )
        .ok()
        .unwrap()
        .activate()
        .ok()
        .unwrap();
    set.seal();
    let cleanup = std::thread::spawn(move || unsafe { active.release() });
    started_recv.recv().unwrap();
    assert_eq!(runtime.external_progress().active, 1);
    assert_eq!(set.progress().active_work, 1);
    finish_send.send(()).unwrap();
    cleanup.join().unwrap();
    assert_eq!(runtime.external_progress().active, 0);
    assert!(set.is_drained());
    runtime.shutdown().unwrap();
}
