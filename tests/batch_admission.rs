use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;

use solworker::{
    SWBatchSpawnOptions, SWCallerEligibility, SWCost, SWDependencyPolicy, SWExecutionClass,
    SWExternalOptions, SWGroup, SWLimits, SWNotifyLimits, SWOutcome, SWOwnedLimits, SWRuntime,
    SWRuntimeConfig, SWSpawnError, SWSpawnOptions, SWTaskStatus, SWWorkerConfig,
};

const TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn partial_submission_flushes_progress_and_dropping_the_receipt_keeps_members_live() {
    let mut runtime = SWRuntime::builder(config())
        .with_owned_limits(SWOwnedLimits::new(4, 0, [3; 3], [1; 3]).unwrap())
        .with_notification_limits(SWNotifyLimits {
            routes: 2,
            bindings: 2,
        })
        .build()
        .unwrap();
    let high = runtime.lane(SWExecutionClass::High);
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let release = Release(release_send);
    let (blocker, _) = high
        .try_spawn(SWSpawnOptions::default(), move || {
            entered_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    entered_recv.recv_timeout(TIMEOUT).unwrap();
    let group = high.group().unwrap();
    let (progress_send, progress_recv) = mpsc::channel();
    let (terminal_send, terminal_recv) = mpsc::channel();
    let progress_host = std::thread::current();
    let terminal_host = progress_host.clone();
    let mut progress_route = runtime
        .notification_route(move || {
            progress_host.unpark();
            progress_send.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    let mut terminal_route = runtime
        .notification_route(move || {
            terminal_host.unpark();
            terminal_send.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    let _progress_binding = progress_route.watch_progress().unwrap();
    let _terminal_binding = terminal_route
        .watch_completion(&group.completion())
        .unwrap();
    let progress_stamp = progress_route.prepare_wait().unwrap();
    let terminal_stamp = terminal_route.prepare_wait().unwrap();
    for _ in progress_recv.try_iter() {}
    for _ in terminal_recv.try_iter() {}
    let (executed_send, executed_recv) = mpsc::channel();
    let operations = (0..5)
        .map(|index| {
            let executed = executed_send.clone();
            move || {
                executed.send(index).unwrap();
            }
        })
        .collect();
    let rejected = high
        .try_spawn_batch(
            SWBatchSpawnOptions {
                group: Some(&group),
                ..Default::default()
            },
            operations,
        )
        .err()
        .unwrap();
    assert_eq!(rejected.reason, SWSpawnError::Full);
    assert_eq!(rejected.accepted.len(), 3);
    assert_eq!(rejected.remaining.len(), 2);
    assert!(progress_route.changed_since(progress_stamp).unwrap());
    progress_recv.recv_timeout(TIMEOUT).unwrap();
    assert!(!terminal_route.changed_since(terminal_stamp).unwrap());
    drop(rejected);
    assert_eq!(group.completion().status(), None);
    group.seal();
    drop(release);
    assert_eq!(
        group.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    terminal_recv.recv_timeout(TIMEOUT).unwrap();
    assert!(terminal_route.changed_since(terminal_stamp).unwrap());
    let observed: Vec<_> = (0..3)
        .map(|_| executed_recv.recv_timeout(TIMEOUT).unwrap())
        .collect();
    assert_eq!(observed, [0, 1, 2]);
    assert_eq!(
        blocker.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    assert_eq!(progress_route.fault(), None);
    assert_eq!(terminal_route.fault(), None);
    progress_route.close().unwrap();
    terminal_route.close().unwrap();
    runtime.shutdown().unwrap();
}

fn config() -> SWRuntimeConfig {
    SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap()
}

struct Release(mpsc::Sender<()>);

impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

struct CountDrop(Arc<AtomicUsize>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct SealOnDrop<'a>(&'a SWGroup);

impl Drop for SealOnDrop<'_> {
    fn drop(&mut self) {
        self.0.seal();
    }
}

#[test]
fn simultaneous_lane_batches_share_global_record_and_edge_ceilings() {
    let mut runtime = SWRuntime::builder(config())
        .with_owned_limits(SWOwnedLimits::new(17, 17, [32; 3], [2; 3]).unwrap())
        .build()
        .unwrap();
    // An unsealed empty group is a real prerequisite without consuming a record.
    let gate = runtime.lane(SWExecutionClass::Low).group().unwrap();
    let release = SealOnDrop(&gate);
    let start = Arc::new(Barrier::new(4));
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let (submitted_send, submitted_recv) = mpsc::channel();
    let mut producers = Vec::new();
    for (lane_index, class) in [
        SWExecutionClass::Low,
        SWExecutionClass::Mid,
        SWExecutionClass::High,
    ]
    .into_iter()
    .enumerate()
    {
        let lane = runtime.lane(class);
        let prerequisite = gate.completion();
        let start = Arc::clone(&start);
        let calls = Arc::clone(&calls);
        let drops = Arc::clone(&drops);
        let submitted = submitted_send.clone();
        producers.push(std::thread::spawn(move || {
            let operations = (0..16)
                .map(|index| {
                    let invoked = Arc::clone(&calls);
                    let capture = CountDrop(Arc::clone(&drops));
                    move || {
                        let _capture = capture;
                        invoked.fetch_add(1, Ordering::SeqCst);
                        (lane_index, index)
                    }
                })
                .collect();
            let prerequisites = [prerequisite];
            start.wait();
            let result = lane.try_spawn_batch(
                SWBatchSpawnOptions {
                    prerequisites: &prerequisites,
                    ..Default::default()
                },
                operations,
            );
            // The borrowed options cannot leave this producer, but the actual
            // accepted handles and untouched operations can.
            let (accepted, remaining, reason) = match result {
                Ok(accepted) => (accepted, Vec::new(), None),
                Err(rejected) => (rejected.accepted, rejected.remaining, Some(rejected.reason)),
            };
            submitted
                .send((lane_index, accepted, remaining, reason))
                .unwrap();
        }));
    }
    drop(submitted_send);
    start.wait();
    let receipts = (0..3)
        .map(|_| submitted_recv.recv_timeout(TIMEOUT))
        .collect::<Result<Vec<_>, _>>();
    let calls_before_release = calls.load(Ordering::SeqCst);
    let drops_before_release = drops.load(Ordering::SeqCst);
    let pending_before_release = receipts.as_ref().is_ok_and(|receipts| {
        receipts
            .iter()
            .flat_map(|(_, accepted, _, _)| accepted)
            .all(|(task, _)| task.status().is_none())
    });
    // Release the dependency before any assertion or producer join, including
    // on a submission timeout.
    drop(release);
    for producer in producers {
        producer.join().unwrap();
    }
    let receipts = receipts.unwrap();
    assert_eq!(calls_before_release, 0);
    assert_eq!(drops_before_release, 0);
    assert!(pending_before_release);
    assert_eq!(
        receipts
            .iter()
            .map(|(_, accepted, _, _)| accepted.len())
            .sum::<usize>(),
        17
    );
    let mut suffixes = Vec::new();
    for (lane_index, mut accepted, remaining, reason) in receipts {
        let prefix_len = accepted.len();
        assert_eq!(prefix_len + remaining.len(), 16);
        assert_eq!(reason, (prefix_len < 16).then_some(SWSpawnError::Full));
        for (index, (task, _)) in accepted.iter_mut().enumerate() {
            assert_eq!(
                task.completion().wait_timeout(TIMEOUT).unwrap(),
                Some(SWTaskStatus::Succeeded)
            );
            assert_eq!(
                task.try_take(),
                Some(SWOutcome::Success((lane_index, index)))
            );
        }
        suffixes.push((lane_index, prefix_len, remaining));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 17);
    assert_eq!(drops.load(Ordering::SeqCst), 17);
    for (lane_index, prefix_len, remaining) in suffixes {
        assert_eq!(
            remaining
                .into_iter()
                .map(|operation| operation())
                .collect::<Vec<_>>(),
            (prefix_len..16)
                .map(|index| (lane_index, index))
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 48);
    assert_eq!(drops.load(Ordering::SeqCst), 48);

    let deadline = std::time::Instant::now() + TIMEOUT;
    while runtime.progress().active_leases != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "accepted work did not drain"
        );
        std::thread::yield_now();
    }
    let lane = runtime.lane(SWExecutionClass::High);
    let next_gate = lane.group().unwrap();
    let next_release = SealOnDrop(&next_gate);
    let prerequisites = [next_gate.completion()];
    let mut reused = lane
        .try_spawn_batch(
            SWBatchSpawnOptions {
                prerequisites: &prerequisites,
                ..Default::default()
            },
            (0..17).map(|index| move || index).collect(),
        )
        .unwrap();
    assert!(reused.iter().all(|(task, _)| task.status().is_none()));
    drop(next_release);
    for (index, (task, _)) in reused.iter_mut().enumerate() {
        assert_eq!(
            task.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Succeeded)
        );
        assert_eq!(task.try_take(), Some(SWOutcome::Success(index)));
    }
    runtime.shutdown().unwrap();
}

#[test]
fn capacity_rejection_recovers_the_exact_uninvoked_suffix_and_keeps_a_live_prefix() {
    for ceiling in ["records", "edges", "runnable", "ordinary_capacity"] {
        let records = if ceiling == "records" { 7 } else { 24 };
        let edges = if ceiling == "edges" { 5 } else { 24 };
        let runnable = if ceiling == "runnable" { 3 } else { 8 };
        let mut builder = SWRuntime::builder(config())
            .with_owned_limits(SWOwnedLimits::new(records, edges, [runnable; 3], [1; 3]).unwrap());
        if ceiling == "ordinary_capacity" {
            builder = builder.with_capacity_limits(
                SWLimits::new(SWCost::new(7, 24, 0, 0), SWCost::new(1, 0, 0, 0), 1, None).unwrap(),
            );
        }
        let mut runtime = builder.build().unwrap();
        let low = runtime.lane(SWExecutionClass::Low);
        let high = runtime.lane(SWExecutionClass::High);
        let (provider, source, _) = runtime
            .external::<()>(SWExternalOptions::default())
            .unwrap();
        let prerequisites = [source.completion()];
        // Another execution class already owns two records and two edges.
        let other_class = low
            .try_spawn_batch(
                SWBatchSpawnOptions {
                    prerequisites: &prerequisites,
                    ..Default::default()
                },
                (0..2).map(|_| || ()).collect(),
            )
            .unwrap();
        let (entered_send, entered_recv) = mpsc::channel();
        let (release_send, release_recv) = mpsc::channel();
        let release = Release(release_send);
        let (blocker, _) = high
            .try_spawn(SWSpawnOptions::default(), move || {
                entered_send.send(()).unwrap();
                release_recv.recv_timeout(TIMEOUT).unwrap();
            })
            .unwrap();
        entered_recv.recv_timeout(TIMEOUT).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let operations = (0..6)
            .map(|index| {
                let capture = CountDrop(Arc::clone(&drops));
                let invoked = Arc::clone(&calls);
                move || {
                    let _capture = capture;
                    invoked.fetch_add(1, Ordering::SeqCst);
                    index
                }
            })
            .collect();
        let rejected = high
            .try_spawn_batch(
                SWBatchSpawnOptions {
                    spawn: SWSpawnOptions {
                        eligibility: SWCallerEligibility::CallerEligible,
                    },
                    prerequisites: if ceiling == "edges" {
                        &prerequisites
                    } else {
                        &[]
                    },
                    ..Default::default()
                },
                operations,
            )
            .err()
            .expect("the next member must exhaust the selected capacity");
        assert_eq!(rejected.reason, SWSpawnError::Full, "{ceiling}");
        assert_eq!(rejected.accepted.len(), 3, "{ceiling}");
        assert_eq!(rejected.remaining.len(), 3, "{ceiling}");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "{ceiling}");
        assert_eq!(drops.load(Ordering::SeqCst), 0, "{ceiling}");
        assert!(
            rejected
                .accepted
                .iter()
                .all(|(task, _)| task.status().is_none())
        );
        let recovered: Vec<_> = rejected
            .remaining
            .into_iter()
            .map(|operation| operation())
            .collect();
        assert_eq!(recovered, [3, 4, 5], "{ceiling}");
        let mut accepted = rejected.accepted;
        accepted[2].1.cancel();
        assert_eq!(accepted[2].0.status(), Some(SWTaskStatus::Cancelled));
        assert_eq!(drops.load(Ordering::SeqCst), 4);
        provider.complete(()).unwrap();
        drop(release);
        for (index, (task, _)) in accepted.iter_mut().enumerate() {
            assert_eq!(
                task.completion().wait_timeout(TIMEOUT).unwrap(),
                Some(if index == 2 {
                    SWTaskStatus::Cancelled
                } else {
                    SWTaskStatus::Succeeded
                }),
                "{ceiling}: {index}"
            );
            assert_eq!(
                task.try_take(),
                Some(if index == 2 {
                    SWOutcome::Cancelled
                } else {
                    SWOutcome::Success(index)
                })
            );
        }
        for (task, _) in other_class {
            assert_eq!(
                task.completion().wait_timeout(TIMEOUT).unwrap(),
                Some(SWTaskStatus::Succeeded)
            );
        }
        assert_eq!(
            blocker.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Succeeded)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 5);
        assert_eq!(drops.load(Ordering::SeqCst), 6);
        runtime.shutdown().unwrap();
    }
}

#[test]
fn empty_batches_are_noops_and_structural_rejections_return_every_operation() {
    for state in ["running", "closed", "disabled"] {
        let mut builder = SWRuntime::builder(config());
        if state != "disabled" {
            builder = builder.with_owned_limits(SWOwnedLimits::new(8, 8, [4; 3], [1; 3]).unwrap());
        }
        let mut runtime = builder.build().unwrap();
        let lane = runtime.lane(SWExecutionClass::High);
        if state == "closed" {
            runtime.begin_shutdown();
        }
        assert!(
            lane.try_spawn_batch(SWBatchSpawnOptions::default(), Vec::<fn()>::new())
                .unwrap()
                .is_empty()
        );
        if state != "running" {
            struct Opaque(usize);
            let rejected = lane
                .try_spawn_batch(SWBatchSpawnOptions::default(), vec![|| Opaque(7)])
                .err()
                .unwrap();
            let debug = format!("{rejected:?}");
            assert!(debug.contains("accepted"));
            assert_eq!(
                rejected.reason,
                if state == "closed" {
                    SWSpawnError::Closed
                } else {
                    SWSpawnError::Disabled
                }
            );
            assert!(rejected.accepted.is_empty());
            assert_eq!(rejected.remaining.into_iter().next().unwrap()().0, 7);
        }
        runtime.shutdown().unwrap();
    }
    let mut runtime = SWRuntime::builder(config())
        .with_owned_limits(SWOwnedLimits::new(8, 8, [4; 3], [1; 3]).unwrap())
        .build()
        .unwrap();
    let mut foreign = SWRuntime::builder(config())
        .with_owned_limits(SWOwnedLimits::new(8, 8, [4; 3], [1; 3]).unwrap())
        .build()
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::High);
    let foreign_group = foreign.lane(SWExecutionClass::High).group().unwrap();
    let wrong_class = runtime.lane(SWExecutionClass::Low).group().unwrap();
    let sealed = lane.group().unwrap();
    sealed.seal();
    let self_dependent = lane.group().unwrap();
    for (group, self_dependency) in [
        (&foreign_group, false),
        (&wrong_class, false),
        (&sealed, false),
        (&self_dependent, true),
    ] {
        let prerequisites = if self_dependency {
            vec![group.completion()]
        } else {
            Vec::new()
        };
        let options = SWBatchSpawnOptions {
            group: Some(group),
            prerequisites: &prerequisites,
            ..Default::default()
        };
        assert!(
            lane.try_spawn_batch(options, Vec::<fn()>::new())
                .unwrap()
                .is_empty()
        );
        let rejected = lane
            .try_spawn_batch(options, (0..3).map(|index| move || index).collect())
            .err()
            .unwrap();
        assert_eq!(rejected.reason, SWSpawnError::InvalidGroup);
        assert!(rejected.accepted.is_empty());
        assert_eq!(
            rejected
                .remaining
                .into_iter()
                .map(|operation| operation())
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(rejected.options.group.unwrap().class(), group.class());
    }
    foreign_group.seal();
    wrong_class.seal();
    self_dependent.seal();
    runtime.shutdown().unwrap();
    foreign.shutdown().unwrap();
}

#[test]
fn ordinary_and_fallible_batches_preserve_typed_results_and_dependency_policy() {
    let mut runtime = SWRuntime::builder(config())
        .with_owned_limits(SWOwnedLimits::new(16, 16, [8; 3], [4; 3]).unwrap())
        .build()
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::High);
    for fallible in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let operations: Vec<_> = (0..3)
            .map(|index| {
                let capture = CountDrop(Arc::clone(&drops));
                move || {
                    let _capture = capture;
                    assert_ne!(index, 2, "batch member panic");
                    if index == 1 {
                        Err("typed error")
                    } else {
                        Ok(index)
                    }
                }
            })
            .collect();
        let mut accepted = if fallible {
            lane.try_spawn_batch_fallible(SWBatchSpawnOptions::default(), operations)
        } else {
            lane.try_spawn_batch(SWBatchSpawnOptions::default(), operations)
        }
        .unwrap();
        let failed = accepted[1].0.completion();
        let (success_only, _) = lane
            .try_spawn_after(
                SWSpawnOptions::default(),
                std::slice::from_ref(&failed),
                SWDependencyPolicy::SuccessOnly,
                || 11,
            )
            .unwrap();
        let mut aware = lane
            .try_spawn_batch(
                SWBatchSpawnOptions {
                    prerequisites: &[failed],
                    dependency_policy: SWDependencyPolicy::OutcomeAware,
                    ..Default::default()
                },
                (0..2).map(|index| move || 13 + index).collect(),
            )
            .unwrap();
        for (index, (task, _)) in accepted.iter_mut().enumerate() {
            let expected = if index == 2 {
                SWTaskStatus::Panicked
            } else if index == 1 && fallible {
                SWTaskStatus::ApplicationFailed
            } else {
                SWTaskStatus::Succeeded
            };
            assert_eq!(
                task.completion().wait_timeout(TIMEOUT).unwrap(),
                Some(expected)
            );
            assert_eq!(
                task.try_take(),
                Some(if index == 2 {
                    SWOutcome::Panicked
                } else {
                    SWOutcome::Success(if index == 1 {
                        Err("typed error")
                    } else {
                        Ok(index)
                    })
                })
            );
        }
        assert_eq!(
            success_only.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(if fallible {
                SWTaskStatus::PrerequisiteFailed
            } else {
                SWTaskStatus::Succeeded
            })
        );
        for (index, (task, _)) in aware.iter_mut().enumerate() {
            assert_eq!(
                task.completion().wait_timeout(TIMEOUT).unwrap(),
                Some(SWTaskStatus::Succeeded)
            );
            assert_eq!(task.try_take(), Some(SWOutcome::Success(13 + index)));
        }
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }
    runtime.shutdown().unwrap();
}
