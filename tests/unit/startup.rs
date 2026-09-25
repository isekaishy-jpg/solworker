use std::cell::RefCell;
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use super::{BackendOwner, Phase, RuntimeControl, Startup};
use crate::external::PhysicalRegistry;
use crate::notification::SWNotifyLimits;
use crate::{SWBuildError, SWExecutionClass, SWRuntime, SWRuntimeConfig, SWWorkerConfig};

const LOCK_TIMEOUT: Duration = Duration::from_secs(5);

fn notification_control() -> (Arc<RuntimeControl>, Arc<BackendOwner>) {
    let backend = Arc::new(BackendOwner::new(Vec::new()));
    let physical = PhysicalRegistry::new(NonZeroUsize::new(2), 0);
    let control = Arc::new(RuntimeControl::new_with_notifications(
        &backend,
        physical,
        Some(SWNotifyLimits {
            routes: 1,
            bindings: 1,
        }),
    ));
    (control, backend)
}

fn control_readable_during_source_publication(
    control: Arc<RuntimeControl>,
    mutate: impl FnOnce(Arc<RuntimeControl>) + Send + 'static,
    observe: impl Fn(&RuntimeControl) -> bool + Send + 'static,
) {
    let domain = Arc::clone(control.notification_domain().unwrap());
    let mut route = domain.create_route(|| Ok(())).unwrap();
    let _binding = route.watch_source(&domain.progress_source()).unwrap();

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let holder = thread::spawn(move || {
        domain.with_progress_source_lock_for_test(|| {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(LOCK_TIMEOUT).unwrap();
        });
    });
    entered_rx.recv_timeout(LOCK_TIMEOUT).unwrap();

    let mutator_control = Arc::clone(&control);
    let mutator = thread::spawn(move || mutate(mutator_control));
    let (probe_tx, probe_rx) = mpsc::channel();
    let probe = thread::spawn(move || {
        let deadline = Instant::now() + LOCK_TIMEOUT;
        while Instant::now() < deadline {
            if observe(&control) {
                let _ = probe_tx.send(true);
                return;
            }
            thread::yield_now();
        }
        let _ = probe_tx.send(false);
    });
    // Release and join even when the assertion fails, so a broken lock
    // boundary does not leave a blocked thread behind.
    let readable = probe_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap_or(false);
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    mutator.join().unwrap();
    probe.join().unwrap();
    assert!(
        readable,
        "control remained locked during source publication"
    );
    route.close().unwrap();
}

#[test]
fn admission_publication_releases_control_before_source_lock() {
    let (control, _backend) = notification_control();
    control_readable_during_source_publication(
        Arc::clone(&control),
        |control| {
            let admission = control.admit_owned(false).unwrap();
            drop(admission);
        },
        |control| control.active_leases() == 1,
    );
    let admission = control.admit_owned(false).unwrap();
    control_readable_during_source_publication(
        control,
        move |_| drop(admission),
        |control| control.active_leases() == 0,
    );
}

#[test]
fn closure_publication_releases_control_after_sealing_sets() {
    let (control, _backend) = notification_control();
    let set = control.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let observed_set = set.clone();
    control_readable_during_source_publication(
        control,
        |control| control.begin_shutdown(),
        move |control| {
            control.phase() == crate::runtime::SWRuntimeState::Closing
                && observed_set.progress().sealed
        },
    );
}

#[test]
fn last_upgraded_set_drops_after_quiescence_scan_unlocks_control() {
    let (control, _backend) = notification_control();
    let set = control.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    control.begin_shutdown();

    let (drop_started_tx, drop_started_rx) = mpsc::channel();
    let (release_drop_tx, release_drop_rx) = mpsc::channel();
    set.inner.on_drop_for_test(move || {
        drop_started_tx.send(()).unwrap();
        release_drop_rx.recv_timeout(LOCK_TIMEOUT).unwrap();
    });

    let (upgraded_tx, upgraded_rx) = mpsc::channel();
    let (release_scan_tx, release_scan_rx) = mpsc::channel();
    control.on_quiescence_scan_for_test(move || {
        upgraded_tx.send(()).unwrap();
        release_scan_rx.recv_timeout(LOCK_TIMEOUT).unwrap();
    });
    let scan_control = Arc::clone(&control);
    let scanner = thread::spawn(move || scan_control.is_quiescent());
    upgraded_rx.recv_timeout(LOCK_TIMEOUT).unwrap();
    drop(set);
    release_scan_tx.send(()).unwrap();
    drop_started_rx.recv_timeout(LOCK_TIMEOUT).unwrap();

    let (probe_tx, probe_rx) = mpsc::channel();
    let probe_control = Arc::clone(&control);
    let probe = thread::spawn(move || {
        probe_tx.send(probe_control.phase()).unwrap();
    });
    let readable = probe_rx.recv_timeout(Duration::from_secs(2));
    release_drop_tx.send(()).unwrap();
    scanner.join().unwrap();
    probe.join().unwrap();
    assert_eq!(readable.unwrap(), crate::runtime::SWRuntimeState::Closing);
}

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
