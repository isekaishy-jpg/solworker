use super::*;
use crate::{
    SWCallerEligibility, SWNotifyLimits, SWOutcome, SWOwnedLimits, SWRuntime, SWRuntimeConfig,
    SWSpawnOptions, SWWorkerConfig,
};
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);

fn build_runtime(records: usize, edges: usize, runnable: usize) -> SWRuntime {
    SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
        .with_owned_limits(SWOwnedLimits::new(records, edges, [runnable; 3], [1; 3]).unwrap())
        .build()
        .unwrap()
}

struct CountDrop(Arc<AtomicUsize>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Release(mpsc::Sender<()>);

impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn singleton_results_preserve_groups_and_all_prerequisite_shapes() {
    for fallible in [false, true] {
        for grouped in [false, true] {
            for shape in [
                "ready",
                "terminal",
                "pending",
                "duplicates",
                "cross_runtime",
            ] {
                let mut runtime = build_runtime(16, 16, 8);
                let mut foreign = build_runtime(16, 16, 8);
                let lane = runtime.lane(SWExecutionClass::High);
                let group = lane.group().unwrap();
                let gate = runtime.lane(SWExecutionClass::Low).group().unwrap();
                let foreign_gate = foreign.lane(SWExecutionClass::Low).group().unwrap();
                if shape == "terminal" {
                    gate.seal();
                }
                let prerequisites = match shape {
                    "ready" => Vec::new(),
                    "duplicates" => vec![gate.completion(), gate.completion()],
                    "cross_runtime" => vec![gate.completion(), foreign_gate.completion()],
                    _ => vec![gate.completion()],
                };
                let drops = Arc::new(AtomicUsize::new(0));
                let capture = CountDrop(Arc::clone(&drops));
                let operation = move || {
                    let _capture = capture;
                    Err::<usize, _>("typed singleton error")
                };
                let options = SWBatchSpawnOptions {
                    group: grouped.then_some(&group),
                    prerequisites: &prerequisites,
                    ..Default::default()
                };
                let mut receipts = if fallible {
                    lane.try_spawn_batch_fallible(options, vec![operation])
                } else {
                    lane.try_spawn_batch(options, vec![operation])
                }
                .unwrap();
                assert_eq!(receipts.len(), 1, "{shape}");
                if !matches!(shape, "ready" | "terminal") {
                    assert_eq!(receipts[0].0.status(), None, "{shape}");
                    assert_eq!(drops.load(Ordering::SeqCst), 0, "{shape}");
                }
                gate.seal();
                foreign_gate.seal();
                let expected = if fallible {
                    SWTaskStatus::ApplicationFailed
                } else {
                    SWTaskStatus::Succeeded
                };
                assert_eq!(
                    receipts[0].0.completion().wait_timeout(TIMEOUT).unwrap(),
                    Some(expected),
                    "{shape}"
                );
                assert_eq!(
                    receipts[0].0.try_take(),
                    Some(SWOutcome::Success(Err("typed singleton error")))
                );
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                assert_eq!(group.completion().status(), None);
                group.seal();
                assert_eq!(
                    group.completion().wait_timeout(TIMEOUT).unwrap(),
                    Some(if grouped && fallible {
                        SWTaskStatus::PrerequisiteFailed
                    } else {
                        SWTaskStatus::Succeeded
                    })
                );
                runtime.shutdown().unwrap();
                foreign.shutdown().unwrap();
            }
        }
    }
}

#[test]
fn singleton_failed_prerequisite_preserves_policy_and_exact_cleanup() {
    for policy in [
        SWDependencyPolicy::SuccessOnly,
        SWDependencyPolicy::OutcomeAware,
    ] {
        let mut runtime = build_runtime(8, 8, 4);
        let lane = runtime.lane(SWExecutionClass::High);
        let (source, _) = lane
            .try_spawn_fallible(Default::default(), || Err::<(), _>("source failure"))
            .unwrap();
        let prerequisites = [source.completion(), source.completion()];
        let drops = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let capture = CountDrop(Arc::clone(&drops));
        let invoked = Arc::clone(&calls);
        let mut receipts = lane
            .try_spawn_batch(
                SWBatchSpawnOptions {
                    prerequisites: &prerequisites,
                    dependency_policy: policy,
                    ..Default::default()
                },
                vec![move || {
                    let _capture = capture;
                    invoked.fetch_add(1, Ordering::SeqCst);
                    37usize
                }],
            )
            .unwrap();
        let aware = policy == SWDependencyPolicy::OutcomeAware;
        assert_eq!(
            receipts[0].0.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(if aware {
                SWTaskStatus::Succeeded
            } else {
                SWTaskStatus::PrerequisiteFailed
            })
        );
        assert_eq!(
            receipts[0].0.try_take(),
            Some(if aware {
                SWOutcome::Success(37)
            } else {
                SWOutcome::PrerequisiteFailed
            })
        );
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(aware));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        runtime.shutdown().unwrap();
    }
}

#[test]
fn singleton_structural_rejections_precede_full_capacity_and_preserve_capture() {
    for cause in [
        "full",
        "too_large",
        "self_dependency",
        "wrong_class",
        "sealed",
        "closed",
    ] {
        let mut runtime = build_runtime(1, 1, 1);
        let lane = runtime.lane(SWExecutionClass::High);
        let group = if cause == "wrong_class" {
            runtime.lane(SWExecutionClass::Low).group().unwrap()
        } else {
            lane.group().unwrap()
        };
        let gate = runtime.lane(SWExecutionClass::Low).group().unwrap();
        let (blocker, blocker_control) = lane
            .try_spawn_after(
                Default::default(),
                &[gate.completion()],
                SWDependencyPolicy::SuccessOnly,
                || (),
            )
            .unwrap();
        if cause == "sealed" {
            group.seal();
        }
        if cause == "closed" {
            runtime.begin_shutdown();
        }
        let prerequisites = match cause {
            "too_large" => vec![gate.completion(), gate.completion()],
            "self_dependency" => vec![group.completion(), gate.completion()],
            _ => Vec::new(),
        };
        let options = SWBatchSpawnOptions {
            spawn: SWSpawnOptions {
                eligibility: SWCallerEligibility::CallerEligible,
            },
            group: (!matches!(cause, "full" | "too_large" | "closed")).then_some(&group),
            prerequisites: &prerequisites,
            dependency_policy: SWDependencyPolicy::OutcomeAware,
        };
        let drops = Arc::new(AtomicUsize::new(0));
        let capture = CountDrop(Arc::clone(&drops));
        let identity = Box::new(113usize);
        let address = &*identity as *const usize as usize;
        let rejected = lane
            .try_spawn_batch(
                options,
                vec![move || {
                    let _capture = capture;
                    (&*identity as *const usize as usize, *identity)
                }],
            )
            .err()
            .unwrap();
        assert_eq!(
            rejected.reason,
            match cause {
                "full" => SWSpawnError::Full,
                "too_large" => SWSpawnError::TooLarge,
                "closed" => SWSpawnError::Closed,
                _ => SWSpawnError::InvalidGroup,
            },
            "{cause}"
        );
        assert!(rejected.accepted.is_empty());
        assert_eq!(rejected.remaining.len(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(rejected.options.spawn, options.spawn);
        assert_eq!(
            rejected.options.dependency_policy,
            options.dependency_policy
        );
        assert!(std::ptr::eq(
            rejected.options.prerequisites,
            options.prerequisites
        ));
        assert_eq!(
            rejected.remaining.into_iter().next().unwrap()(),
            (address, 113)
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        blocker_control.cancel();
        gate.seal();
        group.seal();
        assert!(
            blocker
                .completion()
                .wait_timeout(TIMEOUT)
                .unwrap()
                .is_some()
        );
        runtime.shutdown().unwrap();
    }
}

#[test]
fn singleton_attachment_reserves_runnable_capacity_and_cancel_restores_progress() {
    let mut runtime = build_runtime(8, 8, 1);
    let lane = runtime.lane(SWExecutionClass::High);
    let gate = runtime.lane(SWExecutionClass::Low).group().unwrap();
    let (unrelated, _) = lane
        .try_spawn_after(
            Default::default(),
            &[gate.completion()],
            SWDependencyPolicy::SuccessOnly,
            || 19usize,
        )
        .unwrap();
    let scheduler = gate.scheduler.upgrade().unwrap();
    let (attached_send, attached_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let release = Release(release_send);
    let first = AtomicBool::new(true);
    let release_recv = Mutex::new(release_recv);
    *scheduler.attachment_hook.lock().unwrap() = Some(Arc::new(move |job| {
        if first.swap(false, Ordering::SeqCst) {
            attached_send.send(job).unwrap();
            release_recv.lock().unwrap().recv_timeout(TIMEOUT).unwrap();
        }
    }));
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&calls);
    let submit_lane = lane.clone();
    let submitter = thread::spawn(move || {
        submit_lane
            .try_spawn_batch(
                Default::default(),
                vec![move || {
                    captured.fetch_add(1, Ordering::SeqCst);
                }],
            )
            .unwrap()
    });
    let attaching = attached_recv.recv_timeout(TIMEOUT).unwrap();
    gate.seal();
    let rejected = lane
        .try_spawn_batch(
            SWBatchSpawnOptions {
                spawn: SWSpawnOptions {
                    eligibility: SWCallerEligibility::CallerEligible,
                },
                ..Default::default()
            },
            vec![|| 41usize],
        )
        .err()
        .unwrap();
    assert_eq!(rejected.reason, SWSpawnError::Full);
    let state = scheduler.class_lock(SWExecutionClass::High);
    assert_eq!(state.attaching_runnable, 1);
    assert_eq!(state.deferred.len(), 1);
    drop(state);
    scheduler.suppress(&attaching, SWTaskStatus::Cancelled);
    drop(release);
    let receipts = submitter.join().unwrap();
    *scheduler.attachment_hook.lock().unwrap() = None;
    assert_eq!(
        receipts[0].0.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Cancelled)
    );
    assert_eq!(
        unrelated.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        scheduler
            .class_lock(SWExecutionClass::High)
            .attaching_runnable,
        0
    );
    runtime.shutdown().unwrap();
}

#[test]
fn singleton_runnable_arbitration_precedes_a_concurrent_group_seal() {
    let mut runtime = build_runtime(8, 8, 1);
    let lane = runtime.lane(SWExecutionClass::High);
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let release = Release(release_send);
    let (blocker, _) = lane
        .try_spawn(Default::default(), move || {
            entered_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    entered_recv.recv_timeout(TIMEOUT).unwrap();
    let (queued, _) = lane.try_spawn(Default::default(), || 23usize).unwrap();
    let group = lane.group().unwrap();
    let scheduler = group.scheduler.upgrade().unwrap();
    let state = scheduler.class_lock(SWExecutionClass::High);
    assert_eq!(state.ready.runnable, 1);
    let previous = scheduler.accounting.records.load(Ordering::Acquire);
    let submit_group = group.clone();
    let submitter = thread::spawn(move || {
        lane.try_spawn_batch(
            SWBatchSpawnOptions {
                group: Some(&submit_group),
                ..Default::default()
            },
            vec![|| 29usize],
        )
        .err()
        .unwrap()
        .reason
    });
    let deadline = Instant::now() + TIMEOUT;
    while scheduler.accounting.records.load(Ordering::Acquire) == previous {
        assert!(
            Instant::now() < deadline,
            "singleton did not stage admission"
        );
        thread::yield_now();
    }
    group.seal();
    drop(state);
    let reason = submitter.join().unwrap();
    drop(release);
    assert_eq!(reason, SWSpawnError::Full);
    assert_eq!(group.completion().status(), Some(SWTaskStatus::Succeeded));
    assert_eq!(
        blocker.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    assert_eq!(
        queued.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    runtime.shutdown().unwrap();
}

#[test]
fn singleton_notification_context_precedes_structural_errors_and_retains_capture() {
    let mut runtime =
        SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
            .with_owned_limits(SWOwnedLimits::new(1, 1, [1; 3], [1; 3]).unwrap())
            .with_notification_limits(SWNotifyLimits {
                routes: 1,
                bindings: 1,
            })
            .build()
            .unwrap();
    let lane = runtime.lane(SWExecutionClass::High);
    let group = lane.group().unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&drops);
    let (reason_send, reason_recv) = mpsc::channel();
    let mut route = runtime
        .notification_route(move || {
            let prerequisites = [group.completion(), group.completion()];
            let capture = CountDrop(Arc::clone(&observed));
            let identity = Box::new(127usize);
            let address = &*identity as *const usize as usize;
            let rejected = lane
                .try_spawn_batch(
                    SWBatchSpawnOptions {
                        group: Some(&group),
                        prerequisites: &prerequisites,
                        ..Default::default()
                    },
                    vec![move || {
                        let _capture = capture;
                        (&*identity as *const usize as usize, *identity)
                    }],
                )
                .err()
                .unwrap();
            reason_send
                .send((
                    rejected.reason,
                    rejected.accepted.len(),
                    rejected.remaining,
                    address,
                ))
                .unwrap();
            Ok(())
        })
        .unwrap();
    let binding = route.watch_progress().unwrap();
    route.prepare_wait().unwrap();
    let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let (reason, accepted, remaining, address) = reason_recv.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(reason, SWSpawnError::InvalidContext);
    assert_eq!(accepted, 0);
    assert_eq!(remaining.len(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    route.close().unwrap();
    drop(binding);
    assert_eq!(remaining.into_iter().next().unwrap()(), (address, 127));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    set.seal();
    drop(set);
    runtime.shutdown().unwrap();
}
