use std::time::Duration;

use solworker::{
    SWDependencyPolicy, SWExecutionClass, SWExternalOptions, SWNotifyLimits, SWOwnedLimits,
    SWRuntime, SWRuntimeConfig, SWRuntimeState, SWSpawnOptions, SWTaskStatus, SWWorkerConfig,
};

fn closing_notification_context_preserves_scheduler(drop_route: bool) {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(SWOwnedLimits::new(16, 16, [8; 3], [2; 3]).unwrap())
        .with_notification_limits(SWNotifyLimits {
            routes: 1,
            bindings: 1,
        })
        .build()
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::Low);
    let (producer, source, _) = runtime
        .external::<()>(SWExternalOptions::default())
        .unwrap();
    let (successor, _) = lane
        .try_spawn_after(
            SWSpawnOptions::default(),
            &[source.completion()],
            SWDependencyPolicy::OutcomeAware,
            || 42,
        )
        .unwrap();

    // These records have no dependency on the route or its captured producer.
    let (other_producer, other_source, _) = runtime
        .external::<()>(SWExternalOptions::default())
        .unwrap();
    let (other_successor, _) = lane
        .try_spawn_after(
            SWSpawnOptions::default(),
            &[other_source.completion()],
            SWDependencyPolicy::OutcomeAware,
            || 99,
        )
        .unwrap();

    let host_thread = std::thread::current();
    let mut route = runtime
        .notification_route(move || {
            // Retain provider authority with the host context. The actual
            // signal obeys the adapter contract and only sets a durable wake.
            let _keep_context_alive = &producer;
            host_thread.unpark();
            Ok(())
        })
        .unwrap();
    // No signal has to run: only ordinary capture destruction triggers the bug.
    if drop_route {
        drop(route);
    } else {
        route.close().unwrap();
        assert!(route.is_quiescent());
        drop(route);
    }

    let observed = successor
        .completion()
        .wait_timeout(Duration::from_secs(3))
        .unwrap();
    let next = lane.try_spawn(SWSpawnOptions::default(), || 7);
    eprintln!(
        "drop_route={drop_route}; source={:?}; successor={observed:?}; \
         unrelated_source={:?}; unrelated_successor={:?}; runtime={:?}; next_error={:?}",
        source.status(),
        other_source.status(),
        other_successor.status(),
        runtime.state(),
        next.as_ref().err().map(|rejection| rejection.reason),
    );

    assert_eq!(source.status(), Some(SWTaskStatus::Abandoned));
    assert_eq!(runtime.state(), SWRuntimeState::Running);
    assert_eq!(
        observed,
        Some(SWTaskStatus::Succeeded),
        "an OutcomeAware successor must run when its provider authority is dropped",
    );
    assert_eq!(
        other_source.status(),
        None,
        "closing a route must preserve unrelated producers"
    );
    assert_eq!(other_successor.status(), None);
    let (next, _) = next.expect("closing a notification route must preserve ordinary admission");
    other_producer.complete(()).unwrap();
    assert_eq!(
        other_successor.completion().wait().unwrap(),
        SWTaskStatus::Succeeded
    );
    assert_eq!(next.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    runtime.shutdown().unwrap();
}

#[test]
fn explicit_route_close_keeps_outcome_aware_successor_and_unrelated_work_live() {
    closing_notification_context_preserves_scheduler(false);
}

#[test]
fn route_drop_keeps_outcome_aware_successor_and_unrelated_work_live() {
    closing_notification_context_preserves_scheduler(true);
}

#[test]
fn final_claim_retirement_can_dispatch_captured_producer_successor() {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(SWOwnedLimits::new(16, 16, [8; 3], [2; 3]).unwrap())
        .with_notification_limits(SWNotifyLimits {
            routes: 1,
            bindings: 1,
        })
        .build()
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::Low);
    let (captured, source, _) = runtime
        .external::<()>(SWExternalOptions::default())
        .unwrap();
    let (successor, _) = lane
        .try_spawn_after(
            SWSpawnOptions::default(),
            &[source.completion()],
            SWDependencyPolicy::OutcomeAware,
            || 42,
        )
        .unwrap();
    let (trigger, observed, _) = runtime
        .external::<()>(SWExternalOptions::default())
        .unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let armed = Arc::new(AtomicBool::new(false));
    let signal_armed = Arc::clone(&armed);
    let mut route = runtime
        .notification_route(move || {
            let _keep_captured = &captured;
            if signal_armed.load(Ordering::Acquire) {
                entered_tx.send(()).unwrap();
                // Test-only gate exposes the already-claimed close race. Real host
                // signals must not block. A timeout prevents a broken test hanging.
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
            Ok(())
        })
        .unwrap();
    let binding = route.watch_completion(&observed.completion()).unwrap();
    route.prepare_wait().unwrap();
    armed.store(true, Ordering::Release);
    let publisher = std::thread::spawn(move || trigger.complete(()).unwrap());
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    route.close().unwrap();
    assert!(!route.is_quiescent());
    assert_eq!(source.status(), None); // Final capture still belongs to the claim.
    release_tx.send(()).unwrap();
    publisher.join().unwrap();
    assert!(route.is_quiescent());
    assert_eq!(source.status(), Some(SWTaskStatus::Abandoned));
    assert_eq!(
        successor
            .completion()
            .wait_timeout(Duration::from_secs(3))
            .unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    let (next, _) = lane.try_spawn(SWSpawnOptions::default(), || 7).unwrap();
    assert_eq!(
        next.completion()
            .wait_timeout(Duration::from_secs(3))
            .unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    drop(binding);
    drop(route);
    runtime.shutdown().unwrap();
}
