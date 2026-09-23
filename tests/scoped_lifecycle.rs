use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use solworker::{
    SWExecutionClass, SWExecutionError, SWRuntime, SWRuntimeConfig, SWRuntimeState,
    SWShutdownError, SWWorkerConfig,
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn build_runtime() -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    SWRuntime::builder(config).build().unwrap()
}

struct ExitSignal(Sender<()>);

impl Drop for ExitSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

thread_local! {
    static WORKER_EXIT: RefCell<Option<ExitSignal>> = const { RefCell::new(None) };
}

#[test]
fn shutdown_closes_roots_but_waits_for_active_scope_and_its_nested_work() {
    let runtime = build_runtime();
    let lane = runtime.lane(SWExecutionClass::High);
    let scope_lane = lane.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (scope_done_tx, scope_done_rx) = mpsc::channel();
    let scope = thread::spawn(move || {
        let borrowed = [17];
        let result = scope_lane
            .join_with_owner(
                || borrowed[0],
                || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(TIMEOUT).unwrap();
                    let (left, right) = scope_lane.join(|| 2, || 3).unwrap();
                    left.unwrap() + right.unwrap()
                },
            )
            .unwrap();
        let _ = scope_done_tx.send(result);
    });
    entered_rx.recv_timeout(TIMEOUT).unwrap();

    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let host = thread::spawn(move || {
        let mut runtime = runtime;
        started_tx.send(()).unwrap();
        let result = runtime.shutdown();
        done_tx.send((result, runtime.state())).unwrap();
    });
    started_rx.recv_timeout(TIMEOUT).unwrap();

    // Admission is the same lock as close. A racing attempt may be accepted
    // before close, but once Closed is observed no later root may run.
    let deadline = Instant::now() + TIMEOUT;
    let closed = loop {
        match lane.join(|| 1, || 1) {
            Ok(_) if Instant::now() < deadline => thread::yield_now(),
            Ok(_) => break false,
            Err(rejected) => break rejected.reason == SWExecutionError::Closed,
        }
    };
    let waited = done_rx.try_recv().is_err();
    let _ = release_tx.send(()); // Rescue the borrowed scope before assertions.
    let outcomes = scope_done_rx.recv_timeout(TIMEOUT);
    let shutdown = done_rx.recv_timeout(TIMEOUT);
    if outcomes.is_ok() {
        scope.join().unwrap();
    }
    if shutdown.is_ok() {
        host.join().unwrap();
    }

    assert!(closed, "shutdown did not close new root admission");
    assert!(
        waited,
        "shutdown returned while borrowed owner work was active"
    );
    let outcomes = outcomes.expect("borrowed scope did not settle");
    assert_eq!(outcomes.0.unwrap(), 17);
    assert_eq!(outcomes.1.unwrap(), 5);
    assert_eq!(shutdown.unwrap(), (Ok(()), SWRuntimeState::Stopped));
    assert_eq!(
        lane.join(|| 1, || 2).unwrap_err().reason,
        SWExecutionError::Closed
    );
}

#[test]
fn abandonment_preserves_active_scope_and_stale_lanes_remain_closed() {
    for explicit in [true, false] {
        let (exit_tx, exit_rx) = mpsc::channel();
        let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
        let runtime = SWRuntime::builder(config)
            .with_worker_setup(move |_, _| {
                WORKER_EXIT.with(|slot| {
                    *slot.borrow_mut() = Some(ExitSignal(exit_tx.clone()));
                });
                Ok(())
            })
            .build()
            .unwrap();
        let lane = runtime.lane(SWExecutionClass::Mid);
        let scope_lane = lane.clone();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (scope_done_tx, scope_done_rx) = mpsc::channel();
        let scope = thread::spawn(move || {
            let borrowed = [29];
            let result = scope_lane
                .join_with_owner(
                    || borrowed[0],
                    || {
                        entered_tx.send(()).unwrap();
                        release_rx.recv_timeout(TIMEOUT).unwrap();
                        let (left, right) = scope_lane.join(|| 4, || 6).unwrap();
                        left.unwrap() + right.unwrap()
                    },
                )
                .unwrap();
            let _ = scope_done_tx.send(result);
        });
        entered_rx.recv_timeout(TIMEOUT).unwrap();

        if explicit {
            let mut runtime = runtime;
            runtime.abandon();
            assert_eq!(runtime.state(), SWRuntimeState::Abandoned);
        } else {
            drop(runtime);
        }
        let closed = lane.join(|| 1, || 2).unwrap_err().reason;
        // All workers must have exited while the owner still holds borrowed
        // storage. Its later nested work has only the caller as executor.
        let workers_exited = (0..3).all(|_| exit_rx.recv_timeout(TIMEOUT).is_ok());
        let _ = release_tx.send(()); // Rescue the owner before checking results.
        let outcomes = scope_done_rx.recv_timeout(TIMEOUT);
        if outcomes.is_ok() {
            scope.join().unwrap();
        }
        let outcomes = outcomes.expect("active scope did not settle after abandonment");
        assert_eq!(closed, SWExecutionError::Closed);
        assert!(
            workers_exited,
            "abandoned workers did not exit before owner release"
        );
        assert_eq!(outcomes.0.unwrap(), 29);
        assert_eq!(outcomes.1.unwrap(), 10);

        let mut replacement = build_runtime();
        let fresh = replacement.lane(SWExecutionClass::Mid);
        assert_eq!(
            lane.join(|| 1, || 2).unwrap_err().reason,
            SWExecutionError::Closed
        );
        assert_eq!(fresh.join(|| 1, || 2).unwrap().0.unwrap(), 1);
        replacement.shutdown().unwrap();
    }
}

#[test]
fn shutdown_rejects_participating_caller_and_worker_without_closing_runtime() {
    let runtime = Arc::new(Mutex::new(build_runtime()));
    let lane = runtime.lock().unwrap().lane(SWExecutionClass::Low);
    let branch_runtime = Arc::clone(&runtime);
    let (worker_checked_tx, worker_checked_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let participant = thread::spawn(move || {
        let worker_runtime = Arc::clone(&branch_runtime);
        let results = lane
            .join_with_owner(
                move || {
                    let result = worker_runtime.lock().unwrap().shutdown();
                    worker_checked_tx.send(()).unwrap();
                    result
                },
                || {
                    worker_checked_rx.recv_timeout(TIMEOUT).unwrap();
                    branch_runtime.lock().unwrap().shutdown()
                },
            )
            .unwrap();
        done_tx.send(results).unwrap();
    });

    // If a context check regresses, do not join a thread stuck waiting for its
    // own active lease. The harness can report the bounded failure.
    let results = done_rx.recv_timeout(TIMEOUT);
    if results.is_ok() {
        participant.join().unwrap();
    }
    let (worker, owner) = results.expect("shutdown blocked inside a participating closure");
    assert_eq!(worker.unwrap(), Err(SWShutdownError::ExecutionContext));
    assert_eq!(owner.unwrap(), Err(SWShutdownError::ExecutionContext));

    struct ShutdownOnDrop {
        runtime: Arc<Mutex<SWRuntime>>,
        result: Sender<Result<(), SWShutdownError>>,
    }

    impl Drop for ShutdownOnDrop {
        fn drop(&mut self) {
            let result = self.runtime.lock().unwrap().shutdown();
            let _ = self.result.send(result);
        }
    }

    let cleanup_lane = runtime.lock().unwrap().lane(SWExecutionClass::Low);
    let (cleanup_result_tx, cleanup_result_rx) = mpsc::channel();
    let (cleanup_done_tx, cleanup_done_rx) = mpsc::channel();
    let capture = ShutdownOnDrop {
        runtime: Arc::clone(&runtime),
        result: cleanup_result_tx,
    };
    let cleanup = thread::spawn(move || {
        let empty: [u8; 0] = [];
        let result =
            cleanup_lane.for_each_read_chunk(&empty, NonZeroUsize::new(1).unwrap(), move |_, _| {
                let _keep_capture = &capture;
            });
        let _ = cleanup_done_tx.send(result.is_ok());
    });
    let cleanup_result = cleanup_result_rx.recv_timeout(TIMEOUT);
    let cleanup_done = cleanup_done_rx.recv_timeout(TIMEOUT);
    if cleanup_done.is_ok() {
        cleanup.join().unwrap();
    }
    assert_eq!(
        cleanup_result.expect("batch capture cleanup blocked shutdown"),
        Err(SWShutdownError::ExecutionContext)
    );
    assert!(cleanup_done.unwrap());

    let mut runtime = Arc::try_unwrap(runtime).ok().unwrap().into_inner().unwrap();
    assert_eq!(runtime.state(), SWRuntimeState::Running);
    runtime.shutdown().unwrap();
}
