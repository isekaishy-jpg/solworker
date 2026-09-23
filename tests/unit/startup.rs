use std::cell::RefCell;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use super::{Phase, Startup};
use crate::{SWBuildError, SWExecutionClass, SWRuntime, SWRuntimeConfig, SWWorkerConfig};

struct CountExit(Arc<AtomicUsize>);

impl Drop for CountExit {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

thread_local! {
    static WORKER_EXIT: RefCell<Option<CountExit>> = const { RefCell::new(None) };
}

#[test]
fn spawn_failure_releases_gated_workers_and_joins_all_constructed_pools() {
    // Same-pool partial construction and later-pool rollback use the same
    // failure contract but own their handles at different levels.
    for (failed_class, expected_workers) in
        [(SWExecutionClass::Low, 1), (SWExecutionClass::High, 5)]
    {
        let startup = Arc::new(Startup::new());
        let build_startup = Arc::clone(&startup);
        let observe_startup = Arc::clone(&startup);
        let exits = Arc::new(AtomicUsize::new(0));
        let worker_exits = Arc::clone(&exits);
        let (done_tx, done_rx) = mpsc::channel();
        let host = thread::spawn(move || {
            let config = SWRuntimeConfig::new(6, [SWWorkerConfig::new(2); 3]).unwrap();
            let result = SWRuntime::builder(config)
                .with_worker_setup(move |_, _| {
                    WORKER_EXIT.with(|slot| {
                        *slot.borrow_mut() = Some(CountExit(Arc::clone(&worker_exits)));
                    });
                    Ok(())
                })
                .build_with(build_startup, |class, worker, builder, run| {
                    if class == failed_class && worker == 1 {
                        // Observe readiness under the gate's own lock: all
                        // earlier workers have finished setup and are waiting
                        // for start/abort before this launch is rejected.
                        let state = observe_startup.lock();
                        let (state, timeout) = observe_startup
                            .changed
                            .wait_timeout_while(state, Duration::from_secs(5), |state| {
                                state.phase == Phase::Preparing && state.ready < expected_workers
                            })
                            .unwrap();
                        let gated = !timeout.timed_out()
                            && state.phase == Phase::Preparing
                            && state.ready == expected_workers;
                        drop(state);
                        if !gated {
                            return Err(io::Error::other("workers did not reach startup gate"));
                        }
                        Err(io::Error::other("injected OS spawn failure"))
                    } else {
                        builder.spawn(run)
                    }
                });
            done_tx.send(result).unwrap();
        });

        let completed = done_rx.recv_timeout(Duration::from_secs(10));
        // If abort-before-join regresses, open the gate ourselves before
        // failing. This bounds the test without leaving parked worker threads.
        startup.abort();
        host.join().unwrap();
        let result = completed.expect("spawn rollback joined before opening the abort gate");
        match result {
            Err(SWBuildError::Spawn {
                class,
                worker,
                source,
            }) => {
                assert_eq!(class, failed_class);
                assert_eq!(worker, 1);
                assert_eq!(source.to_string(), "injected OS spawn failure");
            }
            _ => panic!("expected the injected spawn error"),
        }
        assert_eq!(exits.load(Ordering::SeqCst), expected_workers);
    }
}
