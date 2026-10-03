use super::*;
use crate::runtime::SWRuntime;
use crate::runtime::config::{SWExecutionClass, SWRuntimeConfig, SWWorkerConfig};
use crate::scheduler::{SWBatchSpawnOptions, SWCost, SWDependencyPolicy, SWLimits, SWOwnedLimits};
use crate::task::{SWOutcome, SWTask};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

fn runtime() -> SWRuntime {
    SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
        .with_owned_limits(SWOwnedLimits::new(20000, 20000, [4; 3], [2; 3]).unwrap())
        .with_capacity_limits(
            SWLimits::new(
                SWCost::new(20000, 20000, 0, 0),
                SWCost::new(20000, 20000, 0, 0),
                1,
                None,
            )
            .unwrap(),
        )
        .build()
        .unwrap()
}

fn scheduler(runtime: &SWRuntime) -> Arc<OwnedScheduler> {
    let group = runtime.lane(SWExecutionClass::High).group().unwrap();
    let scheduler = group.scheduler.upgrade().unwrap();
    group.seal();
    scheduler
}

#[test]
fn inline_delivery_reconciles_once_after_every_member_attachment_closes() {
    // A range must not inflate every ordinary prerequisite/finalization entry
    // by embedding its fixed slot table in the common activation queue.
    assert!(std::mem::size_of::<super::super::Activation>() <= 4 * std::mem::size_of::<usize>());
    assert_eq!(
        std::mem::size_of::<PrerequisiteRange>(),
        2 * std::mem::size_of::<usize>()
    );
    assert!(
        std::mem::size_of::<Descriptor<SMALL_RANGE>>() < std::mem::size_of::<Descriptor<QUANTUM>>()
    );
    eprintln!(
        "descriptor bytes: small={}, large={}, member={}, activation={}, record={}",
        std::mem::size_of::<Descriptor<SMALL_RANGE>>(),
        std::mem::size_of::<Descriptor<QUANTUM>>(),
        std::mem::size_of::<RangeMember>(),
        std::mem::size_of::<super::super::Activation>(),
        std::mem::size_of::<super::super::Record>(),
    );
    for policy in [
        SWDependencyPolicy::SuccessOnly,
        SWDependencyPolicy::OutcomeAware,
    ] {
        for count in [1, 2, 8, 9, 32, 63, 64, 65, 128, 257] {
            let mut runtime = runtime();
            let scheduler = scheduler(&runtime);
            let (input, sink) = SWTask::<()>::pending_pair();
            sink.finish(SWOutcome::Cancelled, false);
            let registrations = Arc::new(AtomicUsize::new(0));
            let observed_registrations = Arc::clone(&registrations);
            *scheduler.range_install_hook.lock().unwrap() = Some(Arc::new(move || {
                observed_registrations.fetch_add(1, Ordering::SeqCst);
            }));
            let members = runtime
                .lane(SWExecutionClass::High)
                .try_spawn_batch(
                    SWBatchSpawnOptions {
                        prerequisites: &[input.completion()],
                        dependency_policy: policy,
                        ..Default::default()
                    },
                    vec![|| 31usize; count],
                )
                .unwrap();
            let expected = if policy == SWDependencyPolicy::SuccessOnly {
                SWTaskStatus::PrerequisiteFailed
            } else {
                SWTaskStatus::Succeeded
            };
            for (mut task, _) in members {
                assert_eq!(
                    task.completion().wait_timeout(TIMEOUT).unwrap(),
                    Some(expected)
                );
                assert_eq!(
                    task.try_take(),
                    Some(if policy == SWDependencyPolicy::SuccessOnly {
                        SWOutcome::PrerequisiteFailed
                    } else {
                        SWOutcome::Success(31)
                    })
                );
            }
            runtime.shutdown().unwrap();
            assert_eq!(
                registrations.load(Ordering::SeqCst),
                count / QUANTUM + usize::from(count % QUANTUM >= 2),
                "member count {count}"
            );
            assert_eq!(scheduler.accounting.edges.load(Ordering::Acquire), 0);
            assert_eq!(scheduler.accounting.records.load(Ordering::Acquire), 0);
        }
    }
}

#[test]
fn cancellation_before_subscription_install_refunds_members_and_releases_signal_identity() {
    let mut runtime = runtime();
    let scheduler = scheduler(&runtime);
    let signal = scheduler.signals.acquire();
    let address = &*signal as *const _ as usize;
    let (input, sink) = SWTask::<()>::pending_with_signal(signal);
    let weak = Arc::downgrade(&scheduler);
    *scheduler.range_install_hook.lock().unwrap() = Some(Arc::new(move || {
        let scheduler = weak.upgrade().unwrap();
        let jobs: Vec<_> = scheduler
            .class_lock(SWExecutionClass::High)
            .records
            .values()
            .cloned()
            .collect();
        assert_eq!(jobs.len(), 3);
        for job in jobs {
            scheduler.suppress(&job, SWTaskStatus::Cancelled);
            assert!(job.record_lock().as_ref().unwrap().attaching);
        }
        // Cancellation has only detached interest; logical settlement is still
        // retained until the submitting thread closes attachment.
        assert_eq!(scheduler.accounting.records.load(Ordering::Acquire), 3);
    }));
    let members = runtime
        .lane(SWExecutionClass::High)
        .try_spawn_batch(
            SWBatchSpawnOptions {
                prerequisites: &[input.completion()],
                ..Default::default()
            },
            vec![|| 41usize; 3],
        )
        .unwrap();
    for (task, _) in &members {
        assert_eq!(task.status(), Some(SWTaskStatus::Cancelled));
    }
    assert_eq!(scheduler.accounting.edges.load(Ordering::Acquire), 0);
    assert_eq!(
        runtime.capacity_usage().unwrap().ordinary,
        SWCost::default()
    );
    sink.finish(SWOutcome::Success(()), false);
    drop(input);
    let recycled = scheduler.signals.acquire();
    assert_eq!(&*recycled as *const _ as usize, address);
    drop(recycled);
    runtime.shutdown().unwrap();
}

#[test]
fn cancelled_control_recycles_while_sibling_waits_and_old_authority_is_stale() {
    for count in [2, 8, 9, 32, 63, 64, 65, 128, 257] {
        check_cancelled_control_recycling(count);
    }
}

fn check_cancelled_control_recycling(count: usize) {
    let mut runtime = runtime();
    let scheduler = scheduler(&runtime);
    let (input, sink) = SWTask::<()>::pending_pair();
    let addresses = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&addresses);
    *scheduler.attachment_hook.lock().unwrap() = Some(Arc::new(move |job| {
        seen.lock().unwrap().push(&*job as *const _ as usize);
    }));
    let members = runtime
        .lane(SWExecutionClass::High)
        .try_spawn_batch(
            SWBatchSpawnOptions {
                prerequisites: &[input.completion()],
                ..Default::default()
            },
            vec![|| 53usize; count],
        )
        .unwrap();
    members[0].1.cancel();
    assert_eq!(members[0].0.status(), Some(SWTaskStatus::Cancelled));
    assert_eq!(members[1].0.status(), None);
    assert_eq!(
        scheduler.accounting.edges.load(Ordering::Acquire),
        count - 1
    );
    let (replacement, _) = runtime
        .lane(SWExecutionClass::High)
        .try_spawn_after(
            Default::default(),
            &[input.completion()],
            SWDependencyPolicy::SuccessOnly,
            || 59usize,
        )
        .unwrap();
    let addresses = addresses.lock().unwrap();
    assert_eq!(addresses[0], addresses[count]);
    drop(addresses);
    members[0].1.cancel();
    assert_eq!(replacement.status(), None);
    sink.finish(SWOutcome::Success(()), false);
    for (mut task, _) in members.into_iter().skip(1) {
        assert_eq!(
            task.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Succeeded)
        );
        assert_eq!(task.try_take(), Some(SWOutcome::Success(53)));
    }
    assert_eq!(
        replacement.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    runtime.shutdown().unwrap();
    assert_eq!(scheduler.accounting.edges.load(Ordering::Acquire), 0);
}

#[test]
fn final_detach_can_race_callback_already_removed_from_signal() {
    let mut runtime = runtime();
    let scheduler = scheduler(&runtime);
    let (input, sink) = SWTask::<()>::pending_pair();
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let release_recv = Mutex::new(release_recv);
    *scheduler.range_delivery_hook.lock().unwrap() = Some(Arc::new(move || {
        entered_send.send(()).unwrap();
        release_recv.lock().unwrap().recv_timeout(TIMEOUT).unwrap();
    }));
    let calls = Arc::new(AtomicUsize::new(0));
    let members = runtime
        .lane(SWExecutionClass::High)
        .try_spawn_batch(
            SWBatchSpawnOptions {
                prerequisites: &[input.completion()],
                ..Default::default()
            },
            (0..2)
                .map(|_| {
                    let calls = Arc::clone(&calls);
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .collect(),
        )
        .unwrap();
    let publisher = std::thread::spawn(move || sink.finish(SWOutcome::Success(()), false));
    entered_recv.recv_timeout(TIMEOUT).unwrap();
    for (task, producer) in &members {
        producer.cancel();
        assert_eq!(task.status(), Some(SWTaskStatus::Cancelled));
    }
    assert_eq!(scheduler.accounting.records.load(Ordering::Acquire), 0);
    release_send.send(()).unwrap();
    publisher.join().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    runtime.shutdown().unwrap();
}

struct Cleanup {
    drops: Arc<AtomicUsize>,
    action: Option<Box<dyn FnOnce() + Send>>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(action) = self.action.take() {
            action();
        }
    }
}

#[test]
fn cleanup_panic_and_reentrant_range_release_preserve_siblings_and_finalizations() {
    let mut runtime = runtime();
    let scheduler = scheduler(&runtime);
    let lane = runtime.lane(SWExecutionClass::High);
    let group = lane.group().unwrap();
    let (input, sink) = SWTask::<()>::pending_pair();
    let (next_input, next_sink) = SWTask::<()>::pending_pair();
    let next = lane
        .try_spawn_batch(
            SWBatchSpawnOptions {
                prerequisites: &[next_input.completion()],
                ..Default::default()
            },
            vec![|| 67usize; 2],
        )
        .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let actions: Vec<Option<Box<dyn FnOnce() + Send>>> = vec![
        Some(Box::new(|| panic!("member cleanup panic"))),
        Some(Box::new(move || {
            next_sink.finish(SWOutcome::Success(()), false)
        })),
        None,
    ];
    let members = lane
        .try_spawn_batch(
            SWBatchSpawnOptions {
                group: Some(&group),
                prerequisites: &[input.completion()],
                ..Default::default()
            },
            actions
                .into_iter()
                .map(|action| {
                    let cleanup = Cleanup {
                        drops: Arc::clone(&drops),
                        action,
                    };
                    move || {
                        drop(cleanup);
                        71usize
                    }
                })
                .collect(),
        )
        .unwrap();
    group.seal();
    sink.finish(SWOutcome::Cancelled, false);
    assert_eq!(members[0].0.status(), Some(SWTaskStatus::Abandoned));
    for (task, _) in members.iter().skip(1) {
        assert_eq!(task.status(), Some(SWTaskStatus::PrerequisiteFailed));
    }
    for (task, _) in next {
        assert_eq!(
            task.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Succeeded)
        );
    }
    assert!(group.completion().wait_timeout(TIMEOUT).unwrap().is_some());
    runtime.shutdown().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    assert_eq!(scheduler.accounting.records.load(Ordering::Acquire), 0);
    assert_eq!(scheduler.accounting.edges.load(Ordering::Acquire), 0);
}

#[test]
fn long_failed_ranges_drain_iteratively_on_a_small_stack() {
    std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(|| {
            let mut runtime = runtime();
            let scheduler = scheduler(&runtime);
            let lane = runtime.lane(SWExecutionClass::High);
            let (seed, sink) = SWTask::<()>::pending_pair();
            let mut predecessor = seed.completion();
            let mut all = Vec::new();
            for _ in 0..1500 {
                let members = lane
                    .try_spawn_batch(
                        SWBatchSpawnOptions {
                            prerequisites: &[predecessor],
                            ..Default::default()
                        },
                        vec![|| (); 2],
                    )
                    .unwrap();
                predecessor = members[0].0.completion();
                all.extend(members);
            }
            sink.finish(SWOutcome::Cancelled, false);
            assert_eq!(predecessor.status(), Some(SWTaskStatus::PrerequisiteFailed));
            assert!(
                all.iter()
                    .all(|(task, _)| task.status() == Some(SWTaskStatus::PrerequisiteFailed))
            );
            runtime.shutdown().unwrap();
            assert_eq!(scheduler.accounting.records.load(Ordering::Acquire), 0);
            assert_eq!(scheduler.accounting.edges.load(Ordering::Acquire), 0);
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn independent_member_demand_survives_sibling_cancel_and_refresh_race() {
    use crate::external::SWExternalOptions;
    use crate::scheduler::{SWDemandSnapshot, SWPriority};

    let urgent = SWPriority::new(0);
    let normal = SWPriority::new(1);
    let mut runtime =
        SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
            .with_owned_limits(SWOwnedLimits::new(8, 8, [4; 3], [2; 3]).unwrap())
            .with_demand_limits(vec![urgent, normal], 8)
            .build()
            .unwrap();
    let latest = Arc::new(Mutex::new(None::<SWDemandSnapshot>));
    let observed = Arc::clone(&latest);
    let (provider, input, _) = runtime
        .external::<()>(SWExternalOptions {
            provider_demand: Some(Arc::new(move |snapshot| {
                let mut latest = observed.lock().unwrap();
                if latest.is_none_or(|previous| previous.version < snapshot.version) {
                    *latest = Some(snapshot);
                }
            })),
            ..Default::default()
        })
        .unwrap();
    let members = runtime
        .lane(SWExecutionClass::High)
        .try_spawn_batch(
            SWBatchSpawnOptions {
                prerequisites: &[input.completion()],
                ..Default::default()
            },
            vec![|| 79usize; 2],
        )
        .unwrap();
    let first = members[0].0.completion().demand(urgent).unwrap();
    let second = members[1].0.completion().demand(normal).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    std::thread::scope(|scope| {
        let start = Arc::clone(&barrier);
        let producer = &members[0].1;
        scope.spawn(move || {
            start.wait();
            producer.cancel();
        });
        let start = Arc::clone(&barrier);
        let second = &second;
        scope.spawn(move || {
            start.wait();
            second.refresh(urgent).unwrap();
        });
        barrier.wait();
    });
    service_demand(&runtime);
    let snapshot = latest.lock().unwrap().unwrap();
    assert!(snapshot.active);
    assert_eq!(snapshot.priority, Some(urgent));
    assert_eq!(members[0].0.status(), Some(SWTaskStatus::Cancelled));
    assert_eq!(members[1].0.status(), None);
    assert_eq!(input.status(), None);
    drop(first);
    second.defer().unwrap();
    service_demand(&runtime);
    let snapshot = latest.lock().unwrap().unwrap();
    assert!(!snapshot.active);
    assert_eq!(snapshot.priority, Some(urgent));
    second.refresh(normal).unwrap();
    provider.complete(()).unwrap();
    assert_eq!(
        members[1].0.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    runtime.shutdown().unwrap();
}

fn service_demand(runtime: &SWRuntime) {
    for _ in 0..64 {
        if !runtime.service_demand(8) {
            return;
        }
    }
    panic!("bounded demand graph did not settle");
}

#[test]
fn delivery_during_attachment_records_status_without_publishing_readiness() {
    let mut runtime = runtime();
    let scheduler = scheduler(&runtime);
    let lane = runtime.lane(SWExecutionClass::High);
    let (input, sink) = SWTask::<()>::pending_pair();
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let release_recv = Mutex::new(release_recv);
    let first = std::sync::atomic::AtomicBool::new(true);
    *scheduler.range_install_hook.lock().unwrap() = Some(Arc::new(move || {
        if first.swap(false, Ordering::SeqCst) {
            entered_send.send(()).unwrap();
            release_recv.lock().unwrap().recv_timeout(TIMEOUT).unwrap();
        }
    }));
    let submitter = std::thread::spawn(move || {
        lane.try_spawn_batch(
            SWBatchSpawnOptions {
                prerequisites: &[input.completion()],
                ..Default::default()
            },
            vec![|| 83usize; 2],
        )
        .unwrap()
    });
    entered_recv.recv_timeout(TIMEOUT).unwrap();
    sink.finish(SWOutcome::Success(()), false);
    let state = scheduler.class_lock(SWExecutionClass::High);
    assert_eq!(state.ready.runnable, 0);
    assert_eq!(state.records.len(), 2);
    for job in state.records.values() {
        let record = job.record_lock();
        let record = record.as_ref().unwrap();
        assert!(record.attaching);
        assert_eq!(record.pending, 0);
        assert!(record.stage == super::super::Stage::Waiting);
    }
    drop(state);
    release_send.send(()).unwrap();
    let members = submitter.join().unwrap();
    for (task, _) in members {
        assert_eq!(
            task.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Succeeded)
        );
    }
    runtime.shutdown().unwrap();
}

#[test]
fn multiple_and_duplicate_prerequisites_keep_independent_subscriptions() {
    for duplicate in [false, true] {
        for policy in [
            SWDependencyPolicy::SuccessOnly,
            SWDependencyPolicy::OutcomeAware,
        ] {
            let mut runtime = runtime();
            let scheduler = scheduler(&runtime);
            let (first, first_sink) = SWTask::<()>::pending_pair();
            let (second, second_sink) = SWTask::<()>::pending_pair();
            let inputs = [
                first.completion(),
                if duplicate {
                    first.completion()
                } else {
                    second.completion()
                },
            ];
            let registrations = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&registrations);
            *scheduler.range_install_hook.lock().unwrap() = Some(Arc::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
            }));
            let members = runtime
                .lane(SWExecutionClass::High)
                .try_spawn_batch(
                    SWBatchSpawnOptions {
                        prerequisites: &inputs,
                        dependency_policy: policy,
                        ..Default::default()
                    },
                    vec![|| 89usize; 3],
                )
                .unwrap();
            {
                let state = scheduler.class_lock(SWExecutionClass::High);
                for job in state.records.values() {
                    assert_eq!(job.record_lock().as_ref().unwrap().subscriptions.len(), 2);
                }
            }
            let start = Arc::new(std::sync::Barrier::new(3));
            std::thread::scope(|scope| {
                let barrier = Arc::clone(&start);
                scope.spawn(move || {
                    barrier.wait();
                    first_sink.finish(SWOutcome::Cancelled, false);
                });
                let barrier = Arc::clone(&start);
                scope.spawn(move || {
                    barrier.wait();
                    second_sink.finish(SWOutcome::Success(()), false);
                });
                start.wait();
            });
            let expected = if policy == SWDependencyPolicy::SuccessOnly {
                SWTaskStatus::PrerequisiteFailed
            } else {
                SWTaskStatus::Succeeded
            };
            for (task, _) in members {
                assert_eq!(
                    task.completion().wait_timeout(TIMEOUT).unwrap(),
                    Some(expected)
                );
            }
            runtime.shutdown().unwrap();
            assert_eq!(registrations.load(Ordering::SeqCst), 0);
            assert_eq!(scheduler.accounting.edges.load(Ordering::Acquire), 0);
        }
    }
}
