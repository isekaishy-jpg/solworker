use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, mpsc};
use std::thread;

fn domain() -> Arc<NotifyDomain> {
    NotifyDomain::new(
        7,
        SWNotifyLimits {
            routes: 2,
            bindings: 2,
        },
    )
}

#[test]
fn queued_claim_covers_rearm_before_dispatch() {
    let domain = domain();
    let source = NotifySource::new(&domain);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut route = domain
        .create_route(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let _binding = route.watch_source(&source).unwrap();
    calls.store(0, Ordering::SeqCst);
    route.prepare_wait().unwrap();

    let scope = NotificationScope::enter();
    source.publish();
    let stamp = route.prepare_wait().unwrap();
    source.publish();
    assert!(route.changed_since(stamp).unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(scope);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let stamp = route.prepare_wait().unwrap();
    source.publish();
    assert!(route.changed_since(stamp).unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn close_counts_delayed_claim_and_recycles_after_retirement() {
    let domain = NotifyDomain::new(
        8,
        SWNotifyLimits {
            routes: 1,
            bindings: 1,
        },
    );
    let source = NotifySource::new(&domain);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut route = domain
        .create_route(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let binding = route.watch_source(&source).unwrap();
    calls.store(0, Ordering::SeqCst);
    route.prepare_wait().unwrap();
    let (claimed_tx, claimed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let publisher = thread::spawn(move || {
        let scope = NotificationScope::enter();
        source.publish();
        claimed_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(scope);
    });
    claimed_rx.recv().unwrap();
    route.close().unwrap();
    assert!(!route.is_quiescent());
    drop(binding);
    release_tx.send(()).unwrap();
    publisher.join().unwrap();
    assert!(route.is_quiescent());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(route);
    assert!(domain.create_route(|| Ok(())).is_ok());
}

#[test]
fn terminal_detaches_and_registration_rechecks() {
    let domain = domain();
    let source = NotifySource::new(&domain);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut route = domain
        .create_route(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    source.publish_terminal();
    let binding = route.watch_source(&source).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!lock(&binding.cell.as_ref().unwrap().state).active);
    let stamp = route.prepare_wait().unwrap();
    source.publish();
    assert!(!route.changed_since(stamp).unwrap());
}

#[test]
fn callback_fault_and_reentry_are_visible() {
    let domain = domain();
    let source = NotifySource::new(&domain);
    let checked = Arc::new(AtomicUsize::new(0));
    let checked_in_callback = Arc::clone(&checked);
    let domain_in_callback = Arc::clone(&domain);
    let mut route = domain
        .create_route(move || {
            let rejected = domain_in_callback.create_route(|| Ok(())).err().unwrap();
            assert_eq!(rejected.reason, SWNotifyError::InvalidContext);
            checked_in_callback.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("adapter failed"))
        })
        .unwrap();
    let _binding = route.watch_source(&source).unwrap();
    assert_eq!(checked.load(Ordering::SeqCst), 1);
    assert_eq!(
        route.fault(),
        Some(SWNotifyFault::Error("adapter failed".into()))
    );
    assert_eq!(route.prepare_wait(), Err(SWNotifyError::Faulted));
}

#[test]
fn bind_and_terminal_publication_have_no_lost_recheck() {
    let domain = domain();
    let source = NotifySource::new(&domain);
    let barrier = Arc::new(Barrier::new(2));
    let source_for_publisher = source.clone();
    let barrier_for_publisher = Arc::clone(&barrier);
    let publisher = thread::spawn(move || {
        barrier_for_publisher.wait();
        source_for_publisher.publish_terminal();
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut route = domain
        .create_route(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    barrier.wait();
    let _binding = route.watch_source(&source).unwrap();
    publisher.join().unwrap();
    assert!(calls.load(Ordering::SeqCst) >= 1);
    assert!(lock(&source.inner.state).terminal);
}

#[test]
fn bounded_protocol_schedules_match_pending_queued_model() {
    #[derive(Clone, Copy)]
    enum Step {
        Begin,
        Publish,
        Arm,
        End,
        Close,
    }
    #[derive(Default)]
    struct Model {
        pending: bool,
        queued: bool,
        closed: bool,
        claims: usize,
        calls: usize,
        depth: usize,
    }
    impl Model {
        fn step(&mut self, step: Step) {
            match step {
                Step::Begin => self.depth += 1,
                Step::Publish if !self.closed => {
                    if !self.pending {
                        self.pending = true;
                        if !self.queued {
                            self.queued = true;
                            self.claims += 1;
                        }
                    }
                    if self.depth == 0 {
                        self.drain();
                    }
                }
                Step::Arm if !self.closed => self.pending = false,
                Step::End => {
                    self.depth -= 1;
                    if self.depth == 0 {
                        self.drain();
                    }
                }
                Step::Close => self.closed = true,
                _ => {}
            }
        }
        fn drain(&mut self) {
            if self.queued {
                self.queued = false;
                self.claims -= 1;
                self.calls += 1;
            }
        }
    }
    let schedules: &[&[Step]] = &[
        &[Step::Begin, Step::Publish, Step::Publish, Step::End],
        &[
            Step::Begin,
            Step::Publish,
            Step::Arm,
            Step::Publish,
            Step::End,
        ],
        &[Step::Begin, Step::Publish, Step::Close, Step::End],
        &[Step::Publish, Step::Arm, Step::Publish, Step::Close],
    ];
    for schedule in schedules {
        let domain = domain();
        let source = NotifySource::new(&domain);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let mut route = domain
            .create_route(move || {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        let _binding = route.watch_source(&source).unwrap();
        calls.store(0, Ordering::SeqCst);
        route.prepare_wait().unwrap();
        let mut model = Model::default();
        let mut scope = None;
        for &step in *schedule {
            match step {
                Step::Begin => scope = Some(NotificationScope::enter()),
                Step::Publish => source.publish(),
                Step::Arm => {
                    route.prepare_wait().unwrap();
                }
                Step::End => drop(scope.take()),
                Step::Close => route.close().unwrap(),
            }
            model.step(step);
            assert_eq!(calls.load(Ordering::SeqCst), model.calls);
            assert_eq!(lock(&route.cell.state).pending, model.pending);
            assert_eq!(lock(&route.cell.state).queued, model.queued);
            assert_eq!(lock(&route.cell.state).claims, model.claims);
            assert_eq!(route.is_quiescent(), model.closed && model.claims == 0);
        }
    }
}

#[test]
fn rearm_allows_concurrent_adapter_invocations() {
    use std::sync::atomic::AtomicBool;

    let domain = domain();
    let source = NotifySource::new(&domain);
    let calls = Arc::new(AtomicUsize::new(0));
    let block = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Barrier::new(2));
    let (entered_tx, entered_rx) = mpsc::channel();
    let observed = Arc::clone(&calls);
    let should_block = Arc::clone(&block);
    let callback_release = Arc::clone(&release);
    let mut route = domain
        .create_route(move || {
            let previous = observed.fetch_add(1, Ordering::SeqCst);
            if should_block.load(Ordering::SeqCst) && previous == 0 {
                entered_tx.send(()).unwrap();
                callback_release.wait();
            }
            Ok(())
        })
        .unwrap();
    let _binding = route.watch_source(&source).unwrap();
    calls.store(0, Ordering::SeqCst);
    block.store(true, Ordering::SeqCst);
    route.prepare_wait().unwrap();

    let first_source = source.clone();
    let first = thread::spawn(move || first_source.publish());
    entered_rx.recv().unwrap();
    route.prepare_wait().unwrap();
    source.publish();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    release.wait();
    first.join().unwrap();
}

#[test]
fn panicking_adapter_faults_without_unwinding_publisher() {
    let domain = domain();
    let source = NotifySource::new(&domain);
    let mut route = domain
        .create_route(|| -> io::Result<()> { panic!("signal panic") })
        .unwrap();
    let _binding = route.watch_source(&source).unwrap();
    assert_eq!(route.fault(), Some(SWNotifyFault::Panicked));
    assert_eq!(route.prepare_wait(), Err(SWNotifyError::Faulted));
}

#[test]
fn panicking_error_format_and_drop_do_not_strand_claim() {
    struct ErrorBomb;
    impl std::fmt::Debug for ErrorBomb {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("error bomb")
        }
    }
    impl std::fmt::Display for ErrorBomb {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("format panic")
        }
    }
    impl std::error::Error for ErrorBomb {}
    impl Drop for ErrorBomb {
        fn drop(&mut self) {
            panic!("drop panic")
        }
    }

    let domain = domain();
    let source = NotifySource::new(&domain);
    let mut route = domain
        .create_route(|| Err(io::Error::other(ErrorBomb)))
        .unwrap();
    let _binding = route.watch_source(&source).unwrap();
    assert_eq!(route.fault(), Some(SWNotifyFault::Panicked));
    route.close().unwrap();
    assert!(route.is_quiescent());
}

#[test]
fn panicking_signal_capture_drop_does_not_unwind_close() {
    struct DropBomb;
    impl Drop for DropBomb {
        fn drop(&mut self) {
            panic!("capture drop panic")
        }
    }
    let domain = domain();
    let bomb = DropBomb;
    let mut route = domain
        .create_route(move || {
            let _keep_capture = &bomb;
            Ok(())
        })
        .unwrap();
    route.close().unwrap();
    assert!(route.is_quiescent());
    assert_eq!(route.fault(), Some(SWNotifyFault::Panicked));
    drop(route);
    assert!(domain.create_route(|| Ok(())).is_ok());
}

#[test]
fn ready_token_is_inert_and_honors_route_lifecycle() {
    let domain = domain();
    let task = crate::task::SWTask::ready(3usize);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut route = domain
        .create_route(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let binding = route.watch_completion(&task.completion()).unwrap();
    assert!(binding.cell.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    route.close().unwrap();
    assert!(matches!(
        route.watch_completion(&task.completion()),
        Err(SWNotifyError::Closed)
    ));
}

#[test]
fn stale_detach_snapshot_cannot_unlink_reused_binding_cell() {
    let domain = NotifyDomain::new(
        17,
        SWNotifyLimits {
            routes: 1,
            bindings: 1,
        },
    );
    let source = NotifySource::new(&domain);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut route = domain
        .create_route(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let first = route.watch_source(&source).unwrap();
    let cell = Arc::clone(first.cell.as_ref().unwrap());
    let old_generation = first.generation;
    let old_source = Arc::clone(&source.inner);
    drop(first);
    let second = route.watch_source(&source).unwrap();
    assert_eq!(cell.index, second.cell.as_ref().unwrap().index);
    assert_ne!(old_generation, second.generation);

    // Simulates a closer that captured the old source, then waited for its lock
    // while the first handle detached and the slot was reused.
    cell.detach_from_source(old_generation, &old_source);
    assert!(lock(&cell.state).active);
    route.prepare_wait().unwrap();
    let before = calls.load(Ordering::SeqCst);
    source.publish();
    assert_eq!(calls.load(Ordering::SeqCst), before + 1);
}

#[test]
fn limits_and_recycled_stamp_are_enforced() {
    let domain = NotifyDomain::new(
        23,
        SWNotifyLimits {
            routes: 1,
            bindings: 1,
        },
    );
    let source = NotifySource::new(&domain);
    let mut first = domain.create_route(|| Ok(())).unwrap();
    assert_eq!(
        domain.create_route(|| Ok(())).err().unwrap().reason,
        SWNotifyError::Full
    );
    let binding = first.watch_source(&source).unwrap();
    assert!(matches!(
        first.watch_source(&source),
        Err(SWNotifyError::Full)
    ));
    let old_stamp = first.prepare_wait().unwrap();
    drop(binding);
    let new_binding = first.watch_source(&source).unwrap();
    drop(new_binding);
    first.close().unwrap();
    drop(first);
    let second = domain.create_route(|| Ok(())).unwrap();
    assert_eq!(
        second.changed_since(old_stamp),
        Err(SWNotifyError::ForeignStamp)
    );
}

#[test]
fn thread_local_host_cleanup_can_publish_and_drop_routes() {
    struct ExitProbe {
        source: NotifySource,
        route: Option<SWNotifyRoute>,
        _binding: SWNotifyBinding,
        result: mpsc::Sender<(bool, bool)>,
    }
    impl Drop for ExitProbe {
        fn drop(&mut self) {
            // Catch independently so a regression reports a failed test rather
            // than aborting the process on a second TLS-destructor panic.
            let published = catch_unwind(AssertUnwindSafe(|| self.source.publish())).is_ok();
            let route = self.route.take();
            let closed = catch_unwind(AssertUnwindSafe(|| drop(route))).is_ok();
            let _ = self.result.send((published, closed));
        }
    }
    thread_local! {
        static HOST: RefCell<Option<ExitProbe>> = const { RefCell::new(None) };
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let context = Arc::new(());
    let context_observer = Arc::downgrade(&context);
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        // Initialize host TLS first. Its destructor runs after any destructible
        // notification TLS initialized by route creation on this thread.
        HOST.with(|slot| assert!(slot.borrow().is_none()));
        let domain = domain();
        let source = NotifySource::new(&domain);
        let mut route = domain
            .create_route(move || {
                let _keep_context = &context;
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        let binding = route.watch_source(&source).unwrap();
        route.prepare_wait().unwrap();
        HOST.with(|slot| {
            *slot.borrow_mut() = Some(ExitProbe {
                source,
                route: Some(route),
                _binding: binding,
                result: tx,
            })
        });
    })
    .join()
    .unwrap();
    assert_eq!(rx.recv().unwrap(), (true, true));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(context_observer.upgrade().is_none());
}
