use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use solworker::{
    SWExternalOptions, SWOwnedLimits, SWOwnerError, SWPhase, SWPriority, SWProgressWait,
    SWProgressWaitError, SWPumpBudget, SWRuntime, SWRuntimeConfig, SWRuntimeState, SWTask,
    SWWaitError, SWWorkerConfig,
};

fn runtime() -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    SWRuntime::builder(config).build().unwrap()
}

#[test]
fn owner_phase_blocker_and_wake_are_passive() {
    let mut runtime = runtime();
    let phase = SWPhase(11);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(2).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let sender = owner.sender();

    let before = runtime.progress();
    assert_eq!(before.owner.routes, 1);
    assert_eq!(before.owner.ready, 0);
    assert_eq!(
        before.wait_for_change(Instant::now()),
        Err(SWProgressWaitError::InvalidContext)
    );

    // Posting after sampling but before entering the wait must be observable.
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    sender
        .try_post(phase, move |state| {
            *state += 1;
            entered_send.send(()).unwrap();
            release_recv.recv_timeout(Duration::from_secs(5)).unwrap();
        })
        .unwrap();
    let observed = thread::spawn(move || {
        before
            .wait_for_change(Instant::now() + Duration::from_secs(2))
            .unwrap()
    })
    .join()
    .unwrap();
    assert!(matches!(observed, SWProgressWait::Changed(_)));
    let ready = runtime.progress();
    assert_eq!(ready.owner.ready, 1);
    assert_eq!(*owner.state(), 0);

    // Observe the claim while the callback is still running, so settlement
    // cannot hide a missing claim notification.
    let claimed = thread::spawn(move || {
        entered_recv.recv_timeout(Duration::from_secs(5)).unwrap();
        let wake = ready.wait_for_change(Instant::now());
        release_send.send(()).unwrap();
        wake.unwrap()
    });
    owner.pump(phase, SWPumpBudget::new(1)).unwrap();
    let SWProgressWait::Changed(claim_generation) = claimed.join().unwrap() else {
        panic!("claim did not wake the progress observer");
    };
    assert_eq!(*owner.state(), 1);
    let settled = runtime.progress();
    assert_eq!(settled.owner.ready, 0);
    assert_eq!(settled.owner.claimed, 0);
    assert!(settled.wake_generation > claim_generation);

    runtime.begin_shutdown();
    assert_eq!(
        sender.try_post(phase, |_| {}).err().unwrap().reason,
        SWOwnerError::Closed
    );
    assert_eq!(
        owner.try_post(phase, |_| {}).err().unwrap().reason,
        SWOwnerError::Closed
    );
    owner.close();
    assert!(runtime.try_shutdown().unwrap());
}

#[test]
fn deadline_timeout_does_not_advance_owner_work() {
    let mut runtime = runtime();
    let phase = SWPhase(12);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(1).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let prepared = owner.prepare_delivery(phase, |state| *state += 1).unwrap();
    let snapshot = runtime.progress();
    assert_eq!(snapshot.owner.waiting, 1);

    let result = thread::spawn(move || snapshot.wait_for_change(Instant::now()).unwrap())
        .join()
        .unwrap();
    assert!(matches!(result, SWProgressWait::TimedOut(_)));
    assert_eq!(*owner.state(), 0);
    assert_eq!(runtime.progress().owner.waiting, 1);

    drop(prepared.ticket);
    owner.close();
    runtime.shutdown().unwrap();
}

#[test]
fn provider_callback_retains_control_work_after_logical_completion() {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(SWOwnedLimits::new(2, 0, [1; 3], [1; 3]).unwrap())
        .with_demand_limits(vec![SWPriority::new(0)], 1)
        .build()
        .unwrap();
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let release_recv = Mutex::new(release_recv);
    let (producer, task, _) = runtime
        .external::<()>(SWExternalOptions {
            provider_demand: Some(Arc::new(move |snapshot| {
                if snapshot.active {
                    let result = SWTask::ready(()).completion().wait();
                    entered_send.send(result).unwrap();
                    release_recv
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                }
            })),
            ..Default::default()
        })
        .unwrap();
    let completion = task.completion();
    let notifier = thread::spawn(move || completion.demand(SWPriority::new(0)).unwrap());
    assert_eq!(
        entered_recv.recv_timeout(Duration::from_secs(5)).unwrap(),
        Err(SWWaitError::ExecutionContext)
    );
    producer.complete(()).unwrap();
    let progress = runtime.progress();
    assert_eq!(progress.external.logical, 0);
    assert_eq!(progress.scheduler.provider_callbacks, 1);
    assert!(!runtime.try_shutdown().unwrap());
    assert_eq!(runtime.state(), SWRuntimeState::Closing);
    release_send.send(()).unwrap();
    drop(notifier.join().unwrap());
    assert_eq!(runtime.progress().scheduler.provider_callbacks, 0);
    assert!(runtime.try_shutdown().unwrap());
}
