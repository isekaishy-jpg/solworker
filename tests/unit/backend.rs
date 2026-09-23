use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use micropool::ThreadPoolBuilder;

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn partial_worker_launch_joins_earlier_workers() {
    let exited = Arc::new(AtomicBool::new(false));
    let result = ThreadPoolBuilder::default()
        .num_threads(2)
        .try_build_with(|index, worker| {
            if index == 1 {
                return Err("second worker launch failed");
            }
            let exited = exited.clone();
            Ok(thread::spawn(move || {
                worker();
                exited.store(true, Ordering::Release);
            }))
        });

    assert!(matches!(result, Err("second worker launch failed")));
    assert!(exited.load(Ordering::Acquire));
}

#[test]
fn stop_discards_queued_callable_and_returns_rejected_handoff() {
    let pool = ThreadPoolBuilder::default().num_threads(1).build();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let running = pool
        .try_spawn_owned(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .ok()
        .expect("running task should be accepted");
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let dropped = Arc::new(AtomicUsize::new(0));
    let ran = Arc::new(AtomicBool::new(false));
    let drop_count = DropCount(dropped.clone());
    let ran_task = ran.clone();
    let queued = pool
        .try_spawn_owned(move || {
            let _capture = drop_count;
            ran_task.store(true, Ordering::Release);
        })
        .ok()
        .expect("queued task should be accepted");

    pool.begin_stop();
    let rejected_capture = DropCount(dropped.clone());
    let rejected = pool.try_spawn_owned(move || drop(rejected_capture));
    assert!(rejected.is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(rejected.err());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    pool.stop_and_detach();
    queued.help();
    assert!(!queued.complete());
    assert!(!ran.load(Ordering::Acquire));
    assert_eq!(dropped.load(Ordering::SeqCst), 2);

    release_tx.send(()).unwrap();
    running.join();
}
