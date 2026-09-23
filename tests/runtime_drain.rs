use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use solworker::{
    SWExecutionClass, SWExternalAccessOptions, SWExternalOptions, SWOwnedLimits, SWRuntime,
    SWRuntimeConfig, SWRuntimeState, SWSpawnError, SWSpawnOptions, SWTaskStatus, SWWorkerConfig,
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn runtime() -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(16, 16, [8; 3], [8; 3]).unwrap();
    SWRuntime::builder(config)
        .with_owned_limits(limits)
        .with_external_capacity(NonZeroUsize::new(4).unwrap())
        .build()
        .unwrap()
}

fn finish_shutdown(runtime: &mut SWRuntime) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let progress = runtime.progress();
        if runtime.try_shutdown().unwrap() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "runtime did not drain: {progress:?}"
        );
        progress.wait_for_change(deadline).unwrap();
    }
}

#[test]
fn closing_seals_roots_but_keeps_discovery_and_prepared_access_alive() {
    let mut runtime = runtime();
    let lane = runtime.lane(SWExecutionClass::Low);
    let set = runtime.work_set(NonZeroUsize::new(2).unwrap()).unwrap();
    let permit = set.discovery().unwrap();
    let prepared = runtime
        .prepare_external(
            vec![1_u8; 8],
            SWExternalAccessOptions {
                discovery: Some(&permit),
                ..Default::default()
            },
        )
        .ok()
        .unwrap();

    runtime.begin_shutdown();
    assert_eq!(runtime.state(), SWRuntimeState::Closing);
    assert!(set.progress().sealed);
    assert_eq!(
        set.try_spawn(&lane, SWSpawnOptions::default(), || 1)
            .err()
            .unwrap()
            .reason,
        SWSpawnError::Closed
    );
    assert!(runtime.work_set(NonZeroUsize::new(1).unwrap()).is_err());

    let (child, _) = permit
        .try_spawn(&lane, SWSpawnOptions::default(), || 7_u32)
        .unwrap();
    let (producer, external, _) = runtime
        .external::<u32>(SWExternalOptions {
            discovery: Some(&permit),
            ..Default::default()
        })
        .unwrap();
    let active = prepared.activate().ok().unwrap();
    assert!(!runtime.try_shutdown().unwrap());
    assert_eq!(runtime.external_progress().active, 1);

    assert_eq!(
        child.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    producer.complete(9).unwrap();
    assert_eq!(
        external.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    let retained = unsafe { active.acknowledge_release() };
    drop(retained);
    drop(permit);
    // The child's visible outcome can precede its work-set retirement. The
    // runtime drain, not result readiness, supplies the settlement barrier.
    finish_shutdown(&mut runtime);
    assert!(set.is_drained());
    assert_eq!(runtime.state(), SWRuntimeState::Stopped);

    let mut replacement = self::runtime();
    assert_eq!(
        lane.try_spawn(SWSpawnOptions::default(), || 3)
            .err()
            .unwrap()
            .reason,
        SWSpawnError::Closed
    );
    replacement.shutdown().unwrap();
}

#[test]
fn execution_callback_can_request_close_and_host_joins_after_return() {
    let runtime = Arc::new(runtime());
    let lane = runtime.lane(SWExecutionClass::Mid);
    let for_callback = Arc::clone(&runtime);
    let (closed_send, closed_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let (task, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            for_callback.begin_shutdown();
            closed_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    closed_recv.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(runtime.state(), SWRuntimeState::Closing);
    assert!(runtime.progress().active_leases > 0);
    release_send.send(()).unwrap();
    assert_eq!(
        task.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    let mut runtime = Arc::try_unwrap(runtime).ok().unwrap();
    finish_shutdown(&mut runtime);
}
