use std::cell::RefCell;
use std::collections::HashSet;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use solworker::{
    SWBuildError, SWConfigError, SWExecutionClass, SWRuntime, SWRuntimeConfig, SWRuntimeState,
    SWThreadPriority, SWWorkerConfig,
};

fn config(counts: [usize; 3], budget: usize) -> SWRuntimeConfig {
    SWRuntimeConfig::new(budget, counts.map(SWWorkerConfig::new)).unwrap()
}

struct ExitCounter(Arc<AtomicUsize>);

impl Drop for ExitCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

thread_local! {
    static WORKER_EXIT: RefCell<Option<ExitCounter>> = const { RefCell::new(None) };
}

fn count_worker_exit(exits: &Arc<AtomicUsize>) {
    WORKER_EXIT.with(|slot| {
        *slot.borrow_mut() = Some(ExitCounter(Arc::clone(exits)));
    });
}

struct BlockingExit {
    entered: Sender<()>,
    release: Arc<Mutex<Receiver<()>>>,
}

impl Drop for BlockingExit {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self.release.lock().unwrap().recv();
    }
}

thread_local! {
    static BLOCK_WORKER_EXIT: RefCell<Option<BlockingExit>> = const { RefCell::new(None) };
}

struct NotifyExit(Sender<()>);

impl Drop for NotifyExit {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

thread_local! {
    static NOTIFY_WORKER_EXIT: RefCell<Option<NotifyExit>> = const { RefCell::new(None) };
}

#[test]
fn worker_config_validates_class_routes_and_budget_ceiling() {
    let one = SWWorkerConfig::new(1);
    let zero = SWWorkerConfig::new(0);
    let cases = [
        (0, [one, one, one], SWConfigError::ZeroBudget),
        (
            3,
            [zero, one, one],
            SWConfigError::ZeroWorkers(SWExecutionClass::Low),
        ),
        (
            3,
            [one, zero, one],
            SWConfigError::ZeroWorkers(SWExecutionClass::Mid),
        ),
        (
            3,
            [one, one, zero],
            SWConfigError::ZeroWorkers(SWExecutionClass::High),
        ),
        (
            3,
            [SWWorkerConfig::new(2), one, one],
            SWConfigError::BudgetExceeded {
                budget: 3,
                configured: 4,
            },
        ),
        (
            usize::MAX,
            [SWWorkerConfig::new(usize::MAX), one, one],
            SWConfigError::WorkerCountOverflow,
        ),
    ];
    for (budget, classes, expected) in cases {
        assert_eq!(SWRuntimeConfig::new(budget, classes), Err(expected));
    }

    let configured = config([1, 2, 1], 5);
    assert_eq!(configured.worker_budget(), 5);
    assert_eq!(
        configured.workers_for(SWExecutionClass::Low).worker_count(),
        1
    );
    assert_eq!(
        configured.workers_for(SWExecutionClass::Mid).worker_count(),
        2
    );
    assert_eq!(
        configured
            .workers_for(SWExecutionClass::High)
            .worker_count(),
        1
    );
    let priority = one.with_priority(SWThreadPriority::BelowNormal);
    assert_eq!(
        priority.requested_priority(),
        Some(SWThreadPriority::BelowNormal)
    );
    assert_eq!(one.requested_priority(), None);
}

#[test]
fn requested_worker_priority_is_applied_or_reported() {
    let classes = [
        SWWorkerConfig::new(1).with_priority(SWThreadPriority::BelowNormal),
        SWWorkerConfig::new(1),
        SWWorkerConfig::new(1),
    ];
    let config = SWRuntimeConfig::new(3, classes).unwrap();
    let result = SWRuntime::builder(config).build();

    #[cfg(windows)]
    {
        let mut runtime = result.unwrap();
        runtime.shutdown();
    }
    #[cfg(not(windows))]
    assert!(matches!(
        result,
        Err(SWBuildError::Setup {
            class: SWExecutionClass::Low,
            worker: 0,
            source: solworker::SWWorkerSetupError::UnsupportedPriority(
                SWThreadPriority::BelowNormal
            ),
        })
    ));
}

#[test]
fn build_prepares_distinct_workers_in_every_class_before_returning() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let hook_observed = Arc::clone(&observed);
    let capture = Arc::new(());
    let weak_capture = Arc::downgrade(&capture);
    let mut runtime = SWRuntime::builder(config([2, 1, 2], 6))
        .with_worker_setup(move |class, index| {
            let _keep_capture = &capture;
            hook_observed
                .lock()
                .unwrap()
                .push((class, index, thread::current().id()));
            Ok(())
        })
        .build()
        .unwrap();

    assert_eq!(runtime.state(), SWRuntimeState::Running);
    assert!(weak_capture.upgrade().is_none());
    assert_eq!(runtime.config().worker_budget(), 6);
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 5);
    let routes: HashSet<_> = observed
        .iter()
        .map(|(class, index, _)| (*class, *index))
        .collect();
    assert_eq!(
        routes,
        HashSet::from([
            (SWExecutionClass::Low, 0),
            (SWExecutionClass::Low, 1),
            (SWExecutionClass::Mid, 0),
            (SWExecutionClass::High, 0),
            (SWExecutionClass::High, 1),
        ])
    );
    let threads: HashSet<_> = observed.iter().map(|(_, _, id)| *id).collect();
    assert_eq!(threads.len(), 5);
    drop(observed);
    runtime.shutdown();
    assert_eq!(runtime.state(), SWRuntimeState::Stopped);
}

#[test]
fn later_class_setup_error_joins_all_started_workers() {
    let started = Arc::new(AtomicUsize::new(0));
    let exited = Arc::new(AtomicUsize::new(0));
    let hook_started = Arc::clone(&started);
    let hook_exited = Arc::clone(&exited);
    let result = SWRuntime::builder(config([1, 1, 1], 3))
        .with_worker_setup(move |class, _| {
            count_worker_exit(&hook_exited);
            hook_started.fetch_add(1, Ordering::SeqCst);
            if class == SWExecutionClass::High {
                Err(io::Error::other("injected setup failure"))
            } else {
                Ok(())
            }
        })
        .build();

    assert!(matches!(
        result,
        Err(SWBuildError::Hook {
            class: SWExecutionClass::High,
            worker: 0,
            ..
        })
    ));
    assert_eq!(started.load(Ordering::SeqCst), 3);
    assert_eq!(exited.load(Ordering::SeqCst), 3);
}

#[test]
fn later_class_setup_panic_joins_all_started_workers() {
    let started = Arc::new(AtomicUsize::new(0));
    let exited = Arc::new(AtomicUsize::new(0));
    let hook_started = Arc::clone(&started);
    let hook_exited = Arc::clone(&exited);
    let result = SWRuntime::builder(config([1, 1, 1], 3))
        .with_worker_setup(move |class, _| {
            count_worker_exit(&hook_exited);
            hook_started.fetch_add(1, Ordering::SeqCst);
            if class == SWExecutionClass::High {
                panic!("injected setup panic");
            }
            Ok(())
        })
        .build();

    assert!(matches!(
        result,
        Err(SWBuildError::SetupPanicked {
            class: SWExecutionClass::High,
            worker: 0,
        })
    ));
    assert_eq!(started.load(Ordering::SeqCst), 3);
    assert_eq!(exited.load(Ordering::SeqCst), 3);
}

#[test]
fn terminal_lifecycle_calls_are_idempotent() {
    let exited = Arc::new(AtomicUsize::new(0));
    let hook_exited = Arc::clone(&exited);
    let mut stopped = SWRuntime::builder(config([1, 1, 1], 3))
        .with_worker_setup(move |_, _| {
            count_worker_exit(&hook_exited);
            Ok(())
        })
        .build()
        .unwrap();
    stopped.shutdown();
    assert_eq!(exited.load(Ordering::SeqCst), 3);
    stopped.shutdown();
    stopped.abandon();
    assert_eq!(stopped.state(), SWRuntimeState::Stopped);

    let mut abandoned = SWRuntime::builder(config([1, 1, 1], 3)).build().unwrap();
    abandoned.abandon();
    abandoned.abandon();
    abandoned.shutdown();
    assert_eq!(abandoned.state(), SWRuntimeState::Abandoned);
}

#[test]
fn shutdown_stops_every_class_before_joining_worker_cleanup() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let peer_exit = release_tx.clone();
    let mut runtime = SWRuntime::builder(config([1, 1, 1], 3))
        .with_worker_setup(move |class, _| {
            match class {
                SWExecutionClass::Low => BLOCK_WORKER_EXIT.with(|slot| {
                    *slot.borrow_mut() = Some(BlockingExit {
                        entered: entered_tx.clone(),
                        release: Arc::clone(&release_rx),
                    });
                }),
                SWExecutionClass::High => NOTIFY_WORKER_EXIT.with(|slot| {
                    *slot.borrow_mut() = Some(NotifyExit(peer_exit.clone()));
                }),
                SWExecutionClass::Mid => {}
            }
            Ok(())
        })
        .build()
        .unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let host = thread::spawn(move || {
        runtime.shutdown();
        done_tx.send(runtime.state()).unwrap();
    });

    let entered = entered_rx.recv_timeout(Duration::from_secs(5));
    let completed = done_rx.recv_timeout(Duration::from_secs(5));
    // Rescue a regressed stop-one/join-one implementation before failing, so
    // this assertion cannot leave the test process stuck in worker cleanup.
    let _ = release_tx.send(());
    host.join().unwrap();
    assert!(entered.is_ok(), "Low worker never entered its cleanup");
    assert_eq!(completed.unwrap(), SWRuntimeState::Stopped);
}

#[test]
fn abandonment_returns_while_worker_destructors_are_blocked() {
    for explicit in [true, false] {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let hook_release = Arc::clone(&release_rx);
        let runtime = SWRuntime::builder(config([1, 1, 1], 3))
            .with_worker_setup(move |_, _| {
                let exit = BlockingExit {
                    entered: entered_tx.clone(),
                    release: Arc::clone(&hook_release),
                };
                BLOCK_WORKER_EXIT.with(|slot| *slot.borrow_mut() = Some(exit));
                Ok(())
            })
            .build()
            .unwrap();
        let (returned_tx, returned_rx) = mpsc::channel();
        let terminal = thread::spawn(move || {
            if explicit {
                let mut runtime = runtime;
                runtime.abandon();
                assert_eq!(runtime.state(), SWRuntimeState::Abandoned);
            } else {
                drop(runtime);
            }
            returned_tx.send(()).unwrap();
        });

        let entered = entered_rx.recv_timeout(Duration::from_secs(2));
        let returned = returned_rx.recv_timeout(Duration::from_secs(2));
        for _ in 0..3 {
            let _ = release_tx.send(());
        }
        terminal.join().unwrap();
        assert!(
            entered.is_ok(),
            "worker did not enter destructor; explicit={explicit}"
        );
        assert!(
            returned.is_ok(),
            "terminal call joined worker; explicit={explicit}"
        );
    }
}
