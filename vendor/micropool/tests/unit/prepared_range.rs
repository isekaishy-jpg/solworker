//! Publication tests exercise real wrappers, stop ordering and the event loop.
#![allow(clippy::missing_docs_in_private_items)]

use super::{ThreadPoolBuilder, ThreadPoolState};
use crate::util::event_tests::Point;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, Weak, mpsc};
use std::thread;
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(5);

struct Capture {
    id: usize,
    state: Weak<ThreadPoolState>,
    dropped: mpsc::Sender<(usize, bool)>,
}

struct PanicCapture(mpsc::Sender<usize>);

impl Drop for PanicCapture {
    fn drop(&mut self) {
        self.0.send(0).unwrap();
        panic!("capture disposal failed");
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        let state = self.state.upgrade().unwrap();
        let unlocked = state.tasks.try_lock().is_some();
        // Reentrant stop takes the same queue lock. Only attempt it if the
        // assertion will pass, so a regression fails without deadlocking.
        if unlocked {
            state.request_stop();
        }
        self.dropped.send((self.id, unlocked)).unwrap();
    }
}

#[test]
fn accepted_wrappers_are_independent_worker_tasks() {
    let pool = ThreadPoolBuilder::default()
        .num_threads(2)
        .idle_spin_cycles(0)
        .build();
    let (started, receive) = mpsc::channel();
    let mut releases = Vec::new();
    let prepared = std::array::from_fn::<_, 2, _>(|id| {
        let (release, resume) = mpsc::channel();
        releases.push(release);
        let started = started.clone();
        Some(pool.prepare_owned(move || {
            started.send((id, thread::current().id())).unwrap();
            resume.recv_timeout(DEADLINE).unwrap();
        }))
    });
    assert!(pool.try_spawn_prepared_range(prepared).is_ok());
    let first = receive.recv_timeout(DEADLINE);
    let second = receive.recv_timeout(DEADLINE);
    // Releasing both also permits cleanup when an execution assertion fails.
    for release in releases {
        let _ = release.send(());
    }
    let first = first.unwrap();
    let second = second.unwrap();
    assert_ne!(first.0, second.0);
    assert_ne!(first.1, second.1);
    assert_ne!(first.1, thread::current().id());
    pool.stop_and_join();
}

#[test]
fn stop_refusal_and_queued_disposal_release_every_capture_outside_queue_lock() {
    let pool = ThreadPoolBuilder::default().num_threads(0).build();
    let (dropped, receive) = mpsc::channel();
    let ran = Arc::new(AtomicBool::new(false));
    let prepare = |id| {
        let capture = Capture {
            id,
            state: Arc::downgrade(&pool.state),
            dropped: dropped.clone(),
        };
        let ran = ran.clone();
        Some(pool.prepare_owned(move || {
            ran.store(true, Ordering::Release);
            drop(capture);
        }))
    };
    assert!(
        pool.try_spawn_prepared_range([prepare(0), prepare(1)])
            .is_ok()
    );
    pool.begin_stop();
    let refused = pool
        .try_spawn_prepared_range([prepare(2), prepare(3)])
        .err()
        .expect("stop must refuse the entire portion");
    assert!(
        receive.try_recv().is_err(),
        "refusal retains capture ownership"
    );
    drop(refused);
    let mut observed = vec![
        receive.recv_timeout(DEADLINE).unwrap(),
        receive.recv_timeout(DEADLINE).unwrap(),
    ];
    pool.stop_and_detach();
    observed.push(receive.recv_timeout(DEADLINE).unwrap());
    observed.push(receive.recv_timeout(DEADLINE).unwrap());
    observed.sort_unstable();
    assert_eq!(observed, [(0, true), (1, true), (2, true), (3, true)]);
    assert!(receive.try_recv().is_err());
    assert!(!ran.load(Ordering::Acquire));
}

#[test]
fn racing_stop_orders_before_or_after_the_whole_portion() {
    for _ in 0..32 {
        let pool = ThreadPoolBuilder::default().num_threads(0).build();
        let (dropped, receive) = mpsc::channel();
        let ran = Arc::new(AtomicBool::new(false));
        let prepared = std::array::from_fn::<_, 8, _>(|id| {
            let capture = Capture {
                id,
                state: Arc::downgrade(&pool.state),
                dropped: dropped.clone(),
            };
            let ran = ran.clone();
            Some(pool.prepare_owned(move || {
                ran.store(true, Ordering::Release);
                drop(capture);
            }))
        });
        let start = Barrier::new(3);
        thread::scope(|scope| {
            // Both contenders rendezvous while the actual serialization lock
            // is held; either may win once it is released.
            let queue = pool.state.tasks.lock();
            let offer = scope.spawn(|| {
                start.wait();
                pool.try_spawn_prepared_range(prepared)
            });
            let stop = scope.spawn(|| {
                start.wait();
                pool.begin_stop();
            });
            start.wait();
            drop(queue);
            let result = offer.join().unwrap();
            stop.join().unwrap();
            match result {
                Ok(()) => assert_eq!(pool.state.tasks.lock().len(), 8),
                Err(refused) => {
                    assert_eq!(pool.state.tasks.lock().len(), 0);
                    assert!(refused.iter().all(Option::is_some));
                    assert!(receive.try_recv().is_err());
                    drop(refused);
                }
            }
        });
        pool.stop_and_detach();
        let mut observed = (0..8)
            .map(|_| receive.recv_timeout(DEADLINE).unwrap())
            .collect::<Vec<_>>();
        observed.sort_unstable();
        assert_eq!(observed, (0..8).map(|id| (id, true)).collect::<Vec<_>>());
        assert!(!ran.load(Ordering::Acquire));
        assert!(receive.try_recv().is_err());
    }
}

#[test]
fn publication_wakes_worker_when_it_has_armed_but_not_entered_os_wait() {
    let startup = Arc::new(Barrier::new(2));
    let worker_startup = startup.clone();
    let pool = ThreadPoolBuilder::default()
        .num_threads(1)
        .idle_spin_cycles(0)
        .spawn_handler(move |_, worker| {
            let startup = worker_startup.clone();
            thread::spawn(move || {
                startup.wait();
                worker();
            })
        })
        .build();
    let (armed, armed_receive) = mpsc::channel();
    let (resume, resume_receive) = mpsc::channel();
    let resume_receive = Mutex::new(resume_receive);
    let held = AtomicBool::new(false);
    let notifications = Arc::new(AtomicUsize::new(0));
    let notify_count = notifications.clone();
    let state = Arc::downgrade(&pool.state);
    pool.state
        .on_change
        .test_hook
        .set(move |point, _| match point {
            Point::BeforeWait(_) if !held.swap(true, Ordering::Relaxed) => {
                armed.send(()).unwrap();
                resume_receive
                    .lock()
                    .unwrap()
                    .recv_timeout(DEADLINE)
                    .unwrap();
            }
            Point::AfterIncrement => {
                assert!(state.upgrade().unwrap().tasks.try_lock().is_some());
                notify_count.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        });
    startup.wait();
    armed_receive.recv_timeout(DEADLINE).unwrap();
    let (ran, receive) = mpsc::channel();
    let prepared = std::array::from_fn::<_, 4, _>(|id| {
        let ran = ran.clone();
        Some(pool.prepare_owned(move || ran.send(id * id).unwrap()))
    });
    assert!(pool.try_spawn_prepared_range(prepared).is_ok());
    assert_eq!(notifications.load(Ordering::Relaxed), 1);
    resume.send(()).unwrap();
    let mut values = (0..4)
        .map(|_| receive.recv_timeout(DEADLINE).unwrap())
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, [0, 1, 4, 9]);
    // Reuse the ordinary event path for a later portion as well.
    let ran = ran.clone();
    assert!(
        pool.try_spawn_prepared_range([Some(pool.prepare_owned(move || ran.send(25).unwrap()))])
            .is_ok()
    );
    assert_eq!(receive.recv_timeout(DEADLINE).unwrap(), 25);
    pool.state.on_change.test_hook.set(|_, _| {});
    pool.stop_and_join();
}

#[test]
fn unwind_during_portion_preparation_drops_prior_wrappers_without_publication() {
    let pool = ThreadPoolBuilder::default().num_threads(0).build();
    let (dropped, receive) = mpsc::channel();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        std::array::from_fn::<_, 6, _>(|id| {
            if id == 3 {
                panic!("preparation failed before publication");
            }
            let capture = Capture {
                id,
                state: Arc::downgrade(&pool.state),
                dropped: dropped.clone(),
            };
            Some(pool.prepare_owned(move || drop(capture)))
        })
    }));
    assert!(result.is_err());
    assert!(pool.state.tasks.lock().is_empty());
    let mut observed = (0..3)
        .map(|_| receive.recv_timeout(DEADLINE).unwrap())
        .collect::<Vec<_>>();
    observed.sort_unstable();
    assert_eq!(observed, [(0, true), (1, true), (2, true)]);
    assert!(receive.try_recv().is_err());
    pool.stop_and_detach();
}

#[test]
fn panic_in_discarded_capture_does_not_retain_sibling_wrappers() {
    let pool = ThreadPoolBuilder::default().num_threads(0).build();
    let (panic_dropped, panic_receive) = mpsc::channel();
    let (dropped, receive) = mpsc::channel();
    let panic_capture = PanicCapture(panic_dropped);
    let first = pool.prepare_owned(move || drop(panic_capture));
    let prepare = |id| {
        let capture = Capture {
            id,
            state: Arc::downgrade(&pool.state),
            dropped: dropped.clone(),
        };
        pool.prepare_owned(move || drop(capture))
    };
    assert!(
        pool.try_spawn_prepared_range([Some(first), Some(prepare(1)), Some(prepare(2))])
            .is_ok()
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| pool.stop_and_detach()));
    assert!(result.is_err());
    assert_eq!(panic_receive.recv_timeout(DEADLINE).unwrap(), 0);
    assert!(panic_receive.try_recv().is_err());
    let mut observed = [
        receive.recv_timeout(DEADLINE).unwrap(),
        receive.recv_timeout(DEADLINE).unwrap(),
    ];
    observed.sort_unstable();
    assert_eq!(observed, [(1, true), (2, true)]);
    assert!(receive.try_recv().is_err());
}

#[test]
fn empty_and_refused_portions_do_not_add_submission_notifications() {
    let pool = ThreadPoolBuilder::default().num_threads(0).build();
    let notifications = Arc::new(AtomicUsize::new(0));
    let notify_count = notifications.clone();
    pool.state.on_change.test_hook.set(move |point, _| {
        if point == Point::AfterIncrement {
            notify_count.fetch_add(1, Ordering::Relaxed);
        }
    });
    assert!(
        pool.try_spawn_prepared_range::<3>([None, None, None])
            .is_ok()
    );
    assert_eq!(notifications.load(Ordering::Relaxed), 0);
    pool.begin_stop();
    assert_eq!(notifications.load(Ordering::Relaxed), 1);
    assert!(pool.try_spawn_prepared_range::<0>([]).is_ok());
    let (dropped, receive) = mpsc::channel();
    let capture = Capture {
        id: 7,
        state: Arc::downgrade(&pool.state),
        dropped,
    };
    let refused = pool.try_spawn_prepared_range([Some(pool.prepare_owned(move || drop(capture)))]);
    assert!(refused.is_err());
    assert_eq!(notifications.load(Ordering::Relaxed), 1);
    assert!(receive.try_recv().is_err());
    drop(refused);
    assert_eq!(receive.recv_timeout(DEADLINE).unwrap(), (7, true));
    pool.stop_and_detach();
}
