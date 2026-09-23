use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use solworker::{
    SWBatchError, SWDeliveryStatus, SWDependencyPolicy, SWExecutionClass, SWOutcome, SWOwnedLimits,
    SWPhase, SWPumpBudget, SWRuntime, SWRuntimeConfig, SWSpawnError, SWSpawnOptions, SWTaskStatus,
    SWWorkerConfig,
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

#[test]
fn reusable_batch_resets_failure_and_exposes_each_wave_to_other_clients() {
    let mut runtime = runtime();
    let low = runtime.lane(SWExecutionClass::Low);
    let high = runtime.lane(SWExecutionClass::High);
    let mut batch = low.batch();
    assert_eq!(batch.class(), SWExecutionClass::Low);
    assert!(batch.current().is_none());

    let first = batch.begin().unwrap().clone();
    let first_done = first.completion();
    let (failed, _) = low
        .try_spawn_fallible_in(&first, SWSpawnOptions::default(), || {
            Err::<(), _>("first wave failed")
        })
        .unwrap();
    assert_eq!(batch.begin().err(), Some(SWBatchError::Busy));
    assert_eq!(batch.current().unwrap().completion().status(), None);
    first.seal();
    first.wait_helping().unwrap();
    assert_eq!(failed.status(), Some(SWTaskStatus::ApplicationFailed));
    assert_eq!(first_done.status(), Some(SWTaskStatus::PrerequisiteFailed));

    let second = batch.begin().unwrap().clone();
    let second_done = second.completion();
    assert_eq!(second_done.status(), None);
    assert_eq!(first_done.status(), Some(SWTaskStatus::PrerequisiteFailed));
    assert_eq!(
        first.completion().status(),
        Some(SWTaskStatus::PrerequisiteFailed)
    );
    assert_eq!(
        low.try_spawn_in(&first, SWSpawnOptions::default(), || ())
            .err()
            .unwrap()
            .reason,
        SWSpawnError::InvalidGroup
    );

    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(2).unwrap())
        .unwrap();
    let phase = SWPhase(1);
    owner.set_phase(phase).unwrap();
    let (delivery, _) = owner
        .on_ready(&second_done, phase, |state, status| {
            assert_eq!(status, SWTaskStatus::Succeeded);
            *state += 1;
        })
        .unwrap();
    let (mut dependent, _) = high
        .try_spawn_after(
            SWSpawnOptions::default(),
            std::slice::from_ref(&second_done),
            SWDependencyPolicy::SuccessOnly,
            || 31usize,
        )
        .unwrap();
    let (member, _) = low
        .try_spawn_in(&second, SWSpawnOptions::default(), || 29usize)
        .unwrap();
    assert_eq!(batch.begin().err(), Some(SWBatchError::Busy));
    second.seal();
    second.wait_helping().unwrap();
    assert_eq!(second_done.status(), Some(SWTaskStatus::Succeeded));
    assert_eq!(first_done.status(), Some(SWTaskStatus::PrerequisiteFailed));
    assert_eq!(member.status(), Some(SWTaskStatus::Succeeded));
    // Group completion precedes downstream activation; owner delivery has its
    // own completion boundary and may need a later pump.
    let deadline = Instant::now() + TIMEOUT;
    let mut invoked = 0;
    while delivery.status() != SWDeliveryStatus::Published {
        assert!(Instant::now() < deadline, "owner delivery did not complete");
        invoked += owner.pump(phase, SWPumpBudget::new(2)).unwrap().invoked;
        std::thread::yield_now();
    }
    assert_eq!(invoked, 1);
    assert_eq!(*owner.state(), 1);
    assert_eq!(delivery.status(), SWDeliveryStatus::Published);
    owner.close();
    assert_eq!(
        dependent.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    assert_eq!(dependent.try_take(), Some(SWOutcome::Success(31)));

    let third = batch.begin().unwrap().clone();
    assert_eq!(third.completion().status(), None);
    third.seal();
    assert_eq!(third.completion().status(), Some(SWTaskStatus::Succeeded));
    assert_eq!(first_done.status(), Some(SWTaskStatus::PrerequisiteFailed));
    runtime.shutdown().unwrap();
}

#[test]
fn batch_begin_waits_for_member_cleanup_and_rejects_closed_runtime() {
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

    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::Mid);
    let mut batch = lane.batch();
    let first = batch.begin().unwrap().clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let capture = BlockingDrop {
        entered: entered_tx,
        release: release_rx,
    };
    let (member, _) = lane
        .try_spawn_in(&first, SWSpawnOptions::default(), move || {
            let _capture = capture;
            7usize
        })
        .unwrap();
    first.seal();
    entered_rx.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(batch.begin().err(), Some(SWBatchError::Busy));
    assert_eq!(batch.current().unwrap().completion().status(), None);
    assert!(!first.is_complete());
    release_tx.send(()).unwrap();
    first.wait_helping().unwrap();
    assert_eq!(member.status(), Some(SWTaskStatus::Succeeded));

    let second = batch.begin().unwrap().clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let (accepted, _) = lane
        .try_spawn_in(&second, SWSpawnOptions::default(), move || {
            started_tx.send(()).unwrap();
            finish_rx.recv_timeout(TIMEOUT).unwrap();
            11usize
        })
        .unwrap();
    started_rx.recv_timeout(TIMEOUT).unwrap();
    runtime.begin_shutdown();
    second.seal();
    finish_tx.send(()).unwrap();
    second.wait_helping().unwrap();
    assert_eq!(accepted.status(), Some(SWTaskStatus::Succeeded));
    assert_eq!(
        batch.begin().err(),
        Some(SWBatchError::Admission(SWSpawnError::Closed))
    );
    assert_eq!(
        batch.current().unwrap().completion().status(),
        Some(SWTaskStatus::Succeeded)
    );
    runtime.shutdown().unwrap();
}

#[test]
fn dropping_batch_owner_leaves_accepted_members_running() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::Low);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (blocker, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    entered_rx.recv_timeout(TIMEOUT).unwrap();

    let mut batch = lane.batch();
    let retained = batch.begin().unwrap().clone();
    let ran = Arc::new(AtomicBool::new(false));
    let ran_by_member = Arc::clone(&ran);
    let (member, _) = lane
        .try_spawn_in(&retained, SWSpawnOptions::default(), move || {
            ran_by_member.store(true, Ordering::SeqCst);
        })
        .unwrap();
    drop(batch);
    assert_eq!(member.status(), None);
    retained.seal();
    release_tx.send(()).unwrap();
    assert_eq!(
        member.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    retained.wait_helping().unwrap();
    assert!(ran.load(Ordering::SeqCst));
    assert_eq!(
        retained.completion().status(),
        Some(SWTaskStatus::Succeeded)
    );
    assert_eq!(
        blocker.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    runtime.shutdown().unwrap();
}
