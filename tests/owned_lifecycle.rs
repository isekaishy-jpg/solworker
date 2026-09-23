use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use solworker::{
    SWCallerEligibility, SWDependencyPolicy, SWExecutionClass, SWExecutionError, SWOutcome,
    SWOwnedLimits, SWRuntime, SWRuntimeConfig, SWRuntimeState, SWShutdownError, SWSpawnError,
    SWSpawnOptions, SWTaskStatus, SWWorkerConfig,
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn runtime() -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(32, 32, [16; 3], [4; 3]).unwrap();
    SWRuntime::builder(config)
        .with_owned_limits(limits)
        .build()
        .unwrap()
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn cancelling_queued_and_waiting_jobs_settles_their_captures() {
    let mut runtime = runtime();
    let low = runtime.lane(SWExecutionClass::Low);
    let high = runtime.lane(SWExecutionClass::High);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (prerequisite, _) = low
        .try_spawn(SWSpawnOptions::default(), move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(TIMEOUT).unwrap();
            7usize
        })
        .unwrap();
    entered_rx.recv_timeout(TIMEOUT).unwrap();

    let queued_dropped = Arc::new(AtomicBool::new(false));
    let queued_flag = DropFlag(Arc::clone(&queued_dropped));
    let (queued, queued_control) = low
        .try_spawn(SWSpawnOptions::default(), move || {
            let _capture = queued_flag;
            11usize
        })
        .unwrap();
    let waiting_dropped = Arc::new(AtomicBool::new(false));
    let waiting_flag = DropFlag(Arc::clone(&waiting_dropped));
    let group = high.group().unwrap();
    let (waiting, waiting_control) = high
        .try_spawn_after_in(
            &group,
            SWSpawnOptions::default(),
            &[prerequisite.completion()],
            solworker::SWDependencyPolicy::SuccessOnly,
            move || {
                let _capture = waiting_flag;
                13usize
            },
        )
        .unwrap();
    group.seal();

    queued_control.cancel();
    waiting_control.cancel();
    let queued_status = queued.completion().wait_timeout(TIMEOUT).unwrap();
    let waiting_status = waiting.completion().wait_timeout(TIMEOUT).unwrap();
    let captures_dropped =
        queued_dropped.load(Ordering::SeqCst) && waiting_dropped.load(Ordering::SeqCst);
    release_tx.send(()).unwrap();
    assert_eq!(queued_status, Some(SWTaskStatus::Cancelled));
    assert_eq!(waiting_status, Some(SWTaskStatus::Cancelled));
    assert!(captures_dropped, "completion preceded capture cleanup");
    group.wait_helping().unwrap();
    assert!(group.is_complete());
    assert_eq!(
        prerequisite.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    runtime.shutdown().unwrap();
}

#[test]
fn graceful_shutdown_keeps_other_lanes_alive_for_accepted_descendants() {
    let runtime = runtime();
    let low = runtime.lane(SWExecutionClass::Low);
    let high = runtime.lane(SWExecutionClass::High);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (child_tx, child_rx) = mpsc::channel();
    let (parent, _) = low
        .try_spawn(SWSpawnOptions::default(), move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(TIMEOUT).unwrap();
            let (child, _) = high
                .try_spawn(SWSpawnOptions::default(), || 29usize)
                .expect("accepted parent may submit a cross-lane descendant during drain");
            child_tx.send(child).unwrap();
        })
        .unwrap();
    entered_rx.recv_timeout(TIMEOUT).unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let host = thread::spawn(move || {
        let mut runtime = runtime;
        runtime.shutdown().unwrap();
        done_tx.send(runtime.state()).unwrap();
    });
    let deadline = Instant::now() + TIMEOUT;
    let closed = loop {
        match low.group() {
            Err(_) => break true,
            Ok(group) => {
                group.seal();
                if Instant::now() >= deadline {
                    break false;
                }
                thread::yield_now();
            }
        }
    };
    let _ = release_tx.send(());
    let child = child_rx.recv_timeout(TIMEOUT);
    let done = done_rx.recv_timeout(TIMEOUT);
    if done.is_ok() {
        host.join().unwrap();
    }
    assert!(closed, "shutdown did not close root group admission");
    let mut child = child.expect("accepted parent did not submit its descendant");
    assert_eq!(parent.completion().status(), Some(SWTaskStatus::Succeeded));
    assert_eq!(child.completion().status(), Some(SWTaskStatus::Succeeded));
    assert_eq!(child.try_result().as_deref(), Some(&SWOutcome::Success(29)));
    assert_eq!(done.unwrap(), SWRuntimeState::Stopped);
}

#[test]
fn abandonment_settles_unclaimed_work_without_waiting_for_running_work() {
    let mut runtime = runtime();
    let low = runtime.lane(SWExecutionClass::Low);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (running, _) = low
        .try_spawn(SWSpawnOptions::default(), move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(TIMEOUT).unwrap();
            17usize
        })
        .unwrap();
    entered_rx.recv_timeout(TIMEOUT).unwrap();

    let dropped = Arc::new(AtomicBool::new(false));
    let flag = DropFlag(Arc::clone(&dropped));
    let (queued, _) = low
        .try_spawn(SWSpawnOptions::default(), move || {
            let _capture = flag;
            19usize
        })
        .unwrap();
    runtime.abandon();
    let queued_status = queued.completion().wait_timeout(TIMEOUT).unwrap();
    let dropped_before_release = dropped.load(Ordering::SeqCst);
    let _ = release_tx.send(());
    assert_eq!(runtime.state(), SWRuntimeState::Abandoned);
    assert_eq!(queued_status, Some(SWTaskStatus::Abandoned));
    assert!(
        dropped_before_release,
        "abandoned capture was not cleaned up"
    );
    assert_eq!(
        running.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    drop(runtime);
    let rejected = low
        .try_spawn(SWSpawnOptions::default(), || 23usize)
        .err()
        .expect("stale lane must reject work");
    assert_eq!(rejected.reason, SWSpawnError::Closed);
    assert_eq!((rejected.operation)(), 23);
}

#[test]
fn group_helper_claims_handed_off_members_of_only_its_group() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::Mid);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (blocker, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    entered_rx.recv_timeout(TIMEOUT).unwrap();

    let target = lane.group().unwrap();
    let unrelated = lane.group().unwrap();
    let caller = SWSpawnOptions {
        eligibility: SWCallerEligibility::CallerEligible,
    };
    let current_thread = thread::current().id();
    let (mut target_task, _) = lane
        .try_spawn_in(&target, caller, || thread::current().id())
        .unwrap();
    let unrelated_ran = Arc::new(AtomicBool::new(false));
    let unrelated_flag = Arc::clone(&unrelated_ran);
    let (unrelated_task, _) = lane
        .try_spawn_in(&unrelated, caller, move || {
            unrelated_flag.store(true, Ordering::SeqCst);
        })
        .unwrap();
    target.seal();
    unrelated.seal();

    let helped = target.help_ready();
    let target_status = target_task.completion().wait_timeout(TIMEOUT).unwrap();
    let unrelated_before_release = unrelated_ran.load(Ordering::SeqCst);
    let _ = release_tx.send(());
    assert!(helped.unwrap());
    assert_eq!(target_status, Some(SWTaskStatus::Succeeded));
    assert_eq!(
        target_task.try_result().as_deref(),
        Some(&SWOutcome::Success(current_thread))
    );
    assert!(
        !unrelated_before_release,
        "target help executed unrelated work"
    );
    assert_eq!(
        blocker.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    assert_eq!(
        unrelated_task.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    target.wait_helping().unwrap();
    unrelated.wait_helping().unwrap();
    assert!(target.is_complete());
    assert!(unrelated.is_complete());
    runtime.shutdown().unwrap();
}

#[test]
fn group_member_cannot_wait_for_its_own_completion() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::High);
    let group = lane.group().unwrap();
    let nested_lane = lane.clone();
    let member_group = group.clone();
    let (member, _) = lane
        .try_spawn_in(&group, SWSpawnOptions::default(), move || {
            let direct = member_group.wait_helping();
            let worker_group = member_group.clone();
            let owner_group = member_group.clone();
            let (worker, owner) = nested_lane
                .join_with_owner(
                    move || worker_group.wait_helping(),
                    move || owner_group.wait_helping(),
                )
                .unwrap();
            (direct, worker.unwrap(), owner.unwrap())
        })
        .unwrap();
    group.seal();
    assert_eq!(
        member.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    let mut member = member;
    assert_eq!(
        member.try_take(),
        Some(SWOutcome::Success((
            Err(SWExecutionError::InvalidContext),
            Err(SWExecutionError::InvalidContext),
            Err(SWExecutionError::InvalidContext),
        )))
    );
    group.wait_helping().unwrap();
    runtime.shutdown().unwrap();
}

#[test]
fn visible_outcome_retains_admission_until_dependency_cleanup_settles() {
    struct BlockingDrop {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(TIMEOUT).unwrap();
        }
    }

    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(2, 1, [2; 3], [1; 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(limits)
        .build()
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::Mid);
    let group = lane.group().unwrap();
    let (start_tx, start_rx) = mpsc::channel();
    let (run_tx, run_rx) = mpsc::channel();
    let (predecessor, _) = lane
        .try_spawn_fallible(SWSpawnOptions::default(), move || {
            start_tx.send(()).unwrap();
            run_rx.recv_timeout(TIMEOUT).unwrap();
            Err::<usize, &'static str>("failed")
        })
        .unwrap();
    start_rx.recv_timeout(TIMEOUT).unwrap();
    let (drop_tx, drop_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let capture = BlockingDrop {
        entered: drop_tx,
        release: release_rx,
    };
    let (successor, _) = lane
        .try_spawn_after_in(
            &group,
            SWSpawnOptions::default(),
            &[predecessor.completion()],
            SWDependencyPolicy::SuccessOnly,
            move || {
                let _capture = capture;
                1usize
            },
        )
        .unwrap();
    group.seal();
    run_tx.send(()).unwrap();
    drop_rx.recv_timeout(TIMEOUT).unwrap();

    assert_eq!(predecessor.status(), Some(SWTaskStatus::ApplicationFailed));
    assert_eq!(successor.status(), None);
    assert!(!group.is_complete());
    assert_eq!(
        lane.try_spawn(SWSpawnOptions::default(), || 3usize)
            .err()
            .unwrap()
            .reason,
        SWSpawnError::Full
    );

    release_tx.send(()).unwrap();
    assert_eq!(
        successor.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::PrerequisiteFailed)
    );
    group.wait_helping().unwrap();
    runtime.shutdown().unwrap();
}

#[test]
fn capture_cleanup_cannot_join_the_runtime_it_is_settling() {
    struct ShutdownOnDrop {
        runtime: Arc<Mutex<Option<SWRuntime>>>,
        group: solworker::SWGroup,
        result: mpsc::Sender<(Result<(), SWShutdownError>, Result<(), SWExecutionError>)>,
    }

    impl Drop for ShutdownOnDrop {
        fn drop(&mut self) {
            let mut runtime = self.runtime.lock().unwrap();
            let shutdown = runtime.as_mut().unwrap().shutdown();
            drop(runtime);
            self.result
                .send((shutdown, self.group.wait_helping()))
                .unwrap();
        }
    }

    let runtime = Arc::new(Mutex::new(Some(runtime())));
    let lane = runtime
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .lane(SWExecutionClass::Low);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (running, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    entered_rx.recv_timeout(TIMEOUT).unwrap();
    let group = lane.group().unwrap();
    let (result_tx, result_rx) = mpsc::channel();
    let capture = ShutdownOnDrop {
        runtime: Arc::clone(&runtime),
        group: group.clone(),
        result: result_tx,
    };
    let (queued, control) = lane
        .try_spawn_in(&group, SWSpawnOptions::default(), move || {
            let _capture = capture;
        })
        .unwrap();
    group.seal();
    let cancellation = thread::spawn(move || control.cancel());
    let shutdown_result = result_rx.recv_timeout(TIMEOUT);
    let _ = release_tx.send(());
    assert_eq!(
        shutdown_result.unwrap(),
        (
            Err(SWShutdownError::ExecutionContext),
            Err(SWExecutionError::InvalidContext),
        )
    );
    cancellation.join().unwrap();
    assert_eq!(
        queued.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Cancelled)
    );
    group.wait_helping().unwrap();
    assert_eq!(
        running.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    runtime
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .shutdown()
        .unwrap();
}
