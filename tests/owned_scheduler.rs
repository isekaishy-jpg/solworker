use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use solworker::{
    SWCallerEligibility, SWDependencyPolicy, SWExecutionClass, SWOutcome, SWOwnedLimits, SWRuntime,
    SWRuntimeConfig, SWSpawnError, SWSpawnOptions, SWTaskStatus, SWWorkerConfig,
};

fn runtime(runnable: usize, handoff: usize) -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(16, 16, [runnable; 3], [handoff; 3]).unwrap();
    SWRuntime::builder(config)
        .with_owned_limits(limits)
        .build()
        .unwrap()
}

fn caller_eligible() -> SWSpawnOptions {
    SWSpawnOptions {
        eligibility: SWCallerEligibility::CallerEligible,
    }
}

const TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn runnable_saturation_inline_is_explicit_and_try_spawn_never_runs_inline() {
    let mut runtime = runtime(1, 1);
    let lane = runtime.lane(SWExecutionClass::High);
    let (started_send, started_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let (mut first, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            started_send.send(()).unwrap();
            release_recv.recv().unwrap();
            1
        })
        .unwrap();
    started_recv.recv().unwrap();

    let (mut queued, _) = lane.try_spawn(caller_eligible(), || 2).unwrap();
    let rejected = lane
        .try_spawn(caller_eligible(), || 3)
        .err()
        .expect("runnable capacity should reject");
    assert_eq!(rejected.reason, SWSpawnError::Full);
    let caller = thread::current().id();
    let (mut inline, _) = lane
        .submit_or_run(caller_eligible(), move || thread::current().id())
        .unwrap();
    assert_eq!(inline.try_take(), Some(SWOutcome::Success(caller)));
    let group = lane.group().unwrap();
    let (mut fallible, _) = lane
        .submit_or_run_fallible_in(&group, caller_eligible(), move || {
            assert_eq!(thread::current().id(), caller);
            Err::<usize, _>("inline failure")
        })
        .unwrap();
    group.seal();
    assert_eq!(fallible.status(), Some(SWTaskStatus::ApplicationFailed));
    assert_eq!(
        fallible.try_take(),
        Some(SWOutcome::Success(Err("inline failure")))
    );
    assert!(group.is_complete());
    let (mut succeeded, _) = lane
        .submit_or_run_fallible(caller_eligible(), || Ok::<_, ()>(4))
        .unwrap();
    assert_eq!(succeeded.status(), Some(SWTaskStatus::Succeeded));
    assert_eq!(succeeded.try_take(), Some(SWOutcome::Success(Ok(4))));
    release_send.send(()).unwrap();
    assert_eq!(first.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    assert_eq!(queued.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    assert_eq!(first.try_take(), Some(SWOutcome::Success(1)));
    assert_eq!(queued.try_take(), Some(SWOutcome::Success(2)));
    runtime.shutdown().unwrap();
}

#[test]
fn full_handoff_window_does_not_mean_runnable_saturation() {
    let mut runtime = runtime(2, 1);
    let lane = runtime.lane(SWExecutionClass::Mid);
    let (started_send, started_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let (running, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            started_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    started_recv.recv_timeout(TIMEOUT).unwrap();
    let (pending, _) = lane.try_spawn(caller_eligible(), || 5).unwrap();
    let (more, _) = lane.try_spawn(caller_eligible(), || 6).unwrap();
    assert_eq!(pending.status(), None);
    assert_eq!(more.status(), None);
    release_send.send(()).unwrap();
    assert_eq!(
        running.completion().wait().unwrap(),
        SWTaskStatus::Succeeded
    );
    assert_eq!(
        pending.completion().wait().unwrap(),
        SWTaskStatus::Succeeded
    );
    assert_eq!(more.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    runtime.shutdown().unwrap();
}

#[test]
fn repeated_cancelled_ready_jobs_leave_no_queue_entries() {
    let mut runtime = runtime(1, 1);
    let lane = runtime.lane(SWExecutionClass::Mid);
    let (started_send, started_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let (running, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            started_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    started_recv.recv_timeout(TIMEOUT).unwrap();
    for _ in 0..2048 {
        let (task, control) = lane.try_spawn(SWSpawnOptions::default(), || ()).unwrap();
        control.cancel();
        assert_eq!(
            task.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Cancelled)
        );
    }
    release_send.send(()).unwrap();
    assert_eq!(
        running.completion().wait().unwrap(),
        SWTaskStatus::Succeeded
    );
    runtime.shutdown().unwrap();
}

#[test]
fn prerequisite_failure_suppresses_only_success_only_successors() {
    for form in ["standalone", "group", "dependent", "grouped_dependent"] {
        for fail in [false, true] {
            let mut runtime = runtime(8, 2);
            let lane = runtime.lane(SWExecutionClass::Low);
            let group = lane.group().unwrap();
            let options = SWSpawnOptions::default();
            let ready = solworker::SWTask::ready(()).completion();
            let cancelled =
                solworker::SWTask::<()>::ready_outcome(SWOutcome::Cancelled).completion();
            let operation = move || {
                if fail {
                    Err(String::from("decode failed"))
                } else {
                    Ok(7usize)
                }
            };
            let (mut source, _) = match form {
                "standalone" => lane.try_spawn_fallible(options, operation),
                "group" => lane.try_spawn_fallible_in(&group, options, operation),
                "dependent" => lane.try_spawn_after_fallible(
                    options,
                    &[ready],
                    SWDependencyPolicy::SuccessOnly,
                    operation,
                ),
                "grouped_dependent" => lane.try_spawn_after_fallible_in(
                    &group,
                    options,
                    &[cancelled],
                    SWDependencyPolicy::OutcomeAware,
                    operation,
                ),
                _ => unreachable!(),
            }
            .unwrap();
            group.seal();
            let prerequisite = source.completion();
            let invoked = Arc::new(AtomicUsize::new(0));
            let calls = Arc::clone(&invoked);
            let (mut success_only, _) = lane
                .try_spawn_after(
                    options,
                    std::slice::from_ref(&prerequisite),
                    SWDependencyPolicy::SuccessOnly,
                    move || {
                        calls.fetch_add(1, Ordering::Relaxed);
                        1
                    },
                )
                .unwrap();
            let (mut aware, _) = lane
                .try_spawn_after(
                    options,
                    &[prerequisite],
                    SWDependencyPolicy::OutcomeAware,
                    || 2,
                )
                .unwrap();
            runtime.shutdown().unwrap();
            assert!(group.is_complete(), "{form}");
            assert_eq!(
                source.status(),
                Some(if fail {
                    SWTaskStatus::ApplicationFailed
                } else {
                    SWTaskStatus::Succeeded
                }),
                "{form}"
            );
            assert_eq!(
                source.try_take(),
                Some(SWOutcome::Success(if fail {
                    Err(String::from("decode failed"))
                } else {
                    Ok(7)
                })),
                "{form}"
            );
            assert_eq!(
                success_only.status(),
                Some(if fail {
                    SWTaskStatus::PrerequisiteFailed
                } else {
                    SWTaskStatus::Succeeded
                }),
                "{form}"
            );
            assert_eq!(
                success_only.try_take(),
                Some(if fail {
                    SWOutcome::PrerequisiteFailed
                } else {
                    SWOutcome::Success(1)
                }),
                "{form}"
            );
            assert_eq!(aware.try_take(), Some(SWOutcome::Success(2)), "{form}");
            assert_eq!(
                invoked.load(Ordering::Relaxed),
                usize::from(!fail),
                "{form}"
            );
        }
    }
}

#[test]
fn completed_dependency_defers_when_runnable_queue_is_full() {
    let mut runtime = runtime(1, 1);
    let lane = runtime.lane(SWExecutionClass::Low);
    let (started_send, started_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let (running, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            started_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    started_recv.recv_timeout(TIMEOUT).unwrap();
    let (queued, _) = lane.try_spawn(SWSpawnOptions::default(), || 1).unwrap();
    let ready = solworker::SWTask::ready(());
    let (mut deferred, _) = lane
        .try_spawn_after_fallible(
            SWSpawnOptions::default(),
            &[ready.completion()],
            SWDependencyPolicy::SuccessOnly,
            || Err::<usize, _>("deferred failure"),
        )
        .unwrap();
    assert_eq!(deferred.status(), None);
    release_send.send(()).unwrap();
    assert_eq!(
        running.completion().wait().unwrap(),
        SWTaskStatus::Succeeded
    );
    assert_eq!(queued.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    assert_eq!(
        deferred.completion().wait().unwrap(),
        SWTaskStatus::ApplicationFailed
    );
    assert_eq!(
        deferred.try_take(),
        Some(SWOutcome::Success(Err("deferred failure")))
    );
    runtime.shutdown().unwrap();
}

#[test]
fn rejected_submission_returns_uninvoked_capture() {
    let mut runtime = runtime(1, 1);
    let lane = runtime.lane(SWExecutionClass::High);
    let (started_send, started_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let (running, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            started_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    started_recv.recv_timeout(TIMEOUT).unwrap();
    let (queued, _) = lane.try_spawn(SWSpawnOptions::default(), || ()).unwrap();
    let input = String::from("returned capture");
    let rejected = lane
        .try_spawn(SWSpawnOptions::default(), move || input)
        .err()
        .expect("full runnable queue rejects");
    assert_eq!(rejected.reason, SWSpawnError::Full);
    assert_eq!((rejected.operation)(), "returned capture");
    release_send.send(()).unwrap();
    assert_eq!(
        running.completion().wait().unwrap(),
        SWTaskStatus::Succeeded
    );
    assert_eq!(queued.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    runtime.shutdown().unwrap();
}

#[test]
fn saturated_inline_rejects_a_different_class_context() {
    let mut runtime = runtime(1, 1);
    let high = runtime.lane(SWExecutionClass::High);
    let low = runtime.lane(SWExecutionClass::Low);
    let (started_send, started_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let (running, _) = high
        .try_spawn(SWSpawnOptions::default(), move || {
            started_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    started_recv.recv_timeout(TIMEOUT).unwrap();
    let (queued, _) = high.try_spawn(SWSpawnOptions::default(), || ()).unwrap();
    let (result, _) = low
        .try_spawn(SWSpawnOptions::default(), move || {
            high.submit_or_run(caller_eligible(), || 3)
                .err()
                .unwrap()
                .reason
        })
        .unwrap();
    assert_eq!(result.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    let mut result = result;
    assert_eq!(
        result.try_take(),
        Some(SWOutcome::Success(SWSpawnError::InvalidContext))
    );
    release_send.send(()).unwrap();
    assert_eq!(
        running.completion().wait().unwrap(),
        SWTaskStatus::Succeeded
    );
    assert_eq!(queued.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    runtime.shutdown().unwrap();
}

#[test]
fn long_cancelled_successor_chain_drains_without_recursion() {
    const LENGTH: usize = 4096;
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(LENGTH + 2, LENGTH + 2, [1; 3], [1; 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(limits)
        .build()
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::Low);
    let (started_send, started_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let (predecessor, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            started_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    started_recv.recv_timeout(TIMEOUT).unwrap();
    let mut previous = predecessor.completion();
    let mut tasks = Vec::with_capacity(LENGTH);
    let mut first_control = None;
    for _ in 0..LENGTH {
        let (task, control) = lane
            .try_spawn_after(
                SWSpawnOptions::default(),
                &[previous],
                SWDependencyPolicy::SuccessOnly,
                || (),
            )
            .unwrap();
        previous = task.completion();
        if first_control.is_none() {
            first_control = Some(control);
        }
        tasks.push(task);
    }
    first_control.unwrap().cancel();
    assert_eq!(
        tasks
            .last()
            .unwrap()
            .completion()
            .wait_timeout(TIMEOUT)
            .unwrap(),
        Some(SWTaskStatus::PrerequisiteFailed)
    );
    release_send.send(()).unwrap();
    assert_eq!(
        predecessor.completion().wait().unwrap(),
        SWTaskStatus::Succeeded
    );
    runtime.shutdown().unwrap();
}
