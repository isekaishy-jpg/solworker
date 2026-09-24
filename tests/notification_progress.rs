use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use solworker::{
    SWCost, SWExternalAccessOptions, SWExternalOptions, SWLimits, SWNotifyError, SWNotifyLimits,
    SWOwnedLimits, SWProgressWait, SWRuntime, SWRuntimeConfig, SWRuntimeState, SWWorkerConfig,
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn runtime(notifications: bool) -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let owned = SWOwnedLimits::new(8, 8, [8; 3], [2; 3]).unwrap();
    let capacity = SWLimits::new(
        SWCost::new(4, 4, 4, 80),
        SWCost::new(4, 4, 4, 0),
        2,
        Some(80),
    )
    .unwrap();
    let mut builder = SWRuntime::builder(config)
        .with_owned_limits(owned)
        .with_capacity_limits(capacity)
        .with_external_capacity(NonZeroUsize::new(2).unwrap());
    if notifications {
        builder = builder.with_notification_limits(SWNotifyLimits {
            routes: 1,
            bindings: 1,
        });
    }
    builder.build().unwrap()
}

#[test]
fn broad_route_covers_workset_capacity_and_external_accounting_outside_locks() {
    let runtime = Arc::new(runtime(true));
    let observed = Arc::clone(&runtime);
    let sibling = Arc::new(Mutex::new(None::<solworker::SWReservation>));
    let sibling_in_signal = Arc::clone(&sibling);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let mut route = runtime
        .notification_route(move || {
            // If publication still holds the control or scheduler lock, this
            // bounded cross-thread snapshot cannot finish until the callback
            // returns and the route faults.
            let (tx, rx) = mpsc::channel();
            let observed = Arc::clone(&observed);
            let sibling = Arc::clone(&sibling_in_signal);
            let probe = thread::spawn(move || {
                let snapshot = observed.progress();
                if let Some(reservation) = sibling.lock().unwrap().as_ref() {
                    // This also takes the pipeline total lock. A signal from
                    // try_grow must run after that outer lock is released.
                    let lease = reservation.retain_bytes(0).unwrap();
                    drop(lease);
                }
                let _ = tx.send(snapshot);
            });
            rx.recv_timeout(TIMEOUT)
                .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?;
            // Sending the snapshot can precede releasing the probe's runtime
            // clone. Join on success so final runtime ownership is deterministic.
            probe.join().unwrap();
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let binding = route.watch_progress().unwrap();

    let mut stamp = route.prepare_wait().unwrap();
    let set = runtime.work_set(NonZeroUsize::new(2).unwrap()).unwrap();
    assert!(route.changed_since(stamp).unwrap());
    assert_eq!(runtime.progress().work_sets.open, 1);

    stamp = route.prepare_wait().unwrap();
    let permit = set.discovery().unwrap();
    assert!(route.changed_since(stamp).unwrap());
    assert_eq!(runtime.progress().work_sets.discovery_permits, 1);

    stamp = route.prepare_wait().unwrap();
    set.seal();
    assert!(route.changed_since(stamp).unwrap());
    stamp = route.prepare_wait().unwrap();
    drop(permit);
    assert!(route.changed_since(stamp).unwrap());
    assert!(set.is_drained());

    stamp = route.prepare_wait().unwrap();
    let mut reserve = runtime.reserve_ordinary(SWCost::new(1, 0, 0, 0)).unwrap();
    assert!(route.changed_since(stamp).unwrap());
    *sibling.lock().unwrap() = Some(reserve.stage(SWCost::new(0, 0, 0, 0)).unwrap());
    stamp = route.prepare_wait().unwrap();
    reserve.try_grow(SWCost::new(1, 0, 0, 0)).unwrap();
    assert!(route.changed_since(stamp).unwrap());
    stamp = route.prepare_wait().unwrap();
    drop(reserve);
    assert!(route.changed_since(stamp).unwrap());
    let retired_sibling = sibling.lock().unwrap().take();
    drop(retired_sibling);

    stamp = route.prepare_wait().unwrap();
    let (producer, task, _) = runtime
        .external::<u32>(SWExternalOptions::default())
        .unwrap();
    assert!(route.changed_since(stamp).unwrap());
    assert_eq!(runtime.progress().external.logical, 1);
    stamp = route.prepare_wait().unwrap();
    producer.complete(7).unwrap();
    assert!(route.changed_since(stamp).unwrap());
    assert!(task.status().is_some());

    stamp = route.prepare_wait().unwrap();
    let prepared = runtime
        .prepare_external(vec![1_u8], SWExternalAccessOptions::default())
        .unwrap();
    assert!(route.changed_since(stamp).unwrap());
    assert_eq!(runtime.progress().external.prepared, 1);
    stamp = route.prepare_wait().unwrap();
    drop(prepared);
    assert!(route.changed_since(stamp).unwrap());
    assert_eq!(runtime.progress().external.prepared, 0);

    assert_eq!(route.fault(), None);
    assert!(calls.load(Ordering::SeqCst) >= 8);
    route.close().unwrap();
    assert!(route.is_quiescent());
    drop(binding);
    drop(route);
    drop(set);
    let mut runtime = Arc::try_unwrap(runtime).unwrap_or_else(|_| panic!("route retained runtime"));
    runtime.shutdown().unwrap();
}

#[test]
fn fixed_progress_wait_wakes_before_a_blocked_host_signal() {
    let runtime = Arc::new(runtime(true));
    let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let block = Arc::new(AtomicBool::new(false));
    let should_block = Arc::clone(&block);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let mut route = runtime
        .notification_route(move || {
            if should_block.load(Ordering::SeqCst) {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv_timeout(TIMEOUT).unwrap();
            }
            Ok(())
        })
        .unwrap();
    let _binding = route.watch_progress().unwrap();
    let progress = runtime.progress();
    route.prepare_wait().unwrap();
    block.store(true, Ordering::SeqCst);
    let publisher = thread::spawn(move || {
        // Seal publishes after the set lock, without an enclosing control
        // scope that could mask incorrect CV-versus-host notification ordering.
        set.seal();
    });
    entered_rx.recv_timeout(TIMEOUT).unwrap();
    assert!(matches!(
        progress.wait_for_change(Instant::now() + TIMEOUT).unwrap(),
        SWProgressWait::Changed(_)
    ));
    release_tx.send(()).unwrap();
    publisher.join().unwrap();
    route.close().unwrap();
    assert!(route.is_quiescent());
    drop(route);
    let mut runtime = Arc::try_unwrap(runtime).unwrap_or_else(|_| panic!("runtime retained"));
    runtime.shutdown().unwrap();
}

#[test]
fn route_survives_runtime_drop_and_disabled_configuration_rejects_route() {
    let disabled = runtime(false);
    assert_eq!(
        disabled.notification_route(|| Ok(())).err().unwrap().reason,
        SWNotifyError::Disabled
    );
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let zero_limits = SWRuntime::builder(config)
        .with_notification_limits(SWNotifyLimits {
            routes: 0,
            bindings: 1,
        })
        .build()
        .unwrap();
    assert_eq!(
        zero_limits
            .notification_route(|| Ok(()))
            .err()
            .unwrap()
            .reason,
        SWNotifyError::Disabled
    );

    let runtime = runtime(true);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let mut route = runtime
        .notification_route(move || {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let _binding = route.watch_progress().unwrap();
    let stamp = route.prepare_wait().unwrap();
    runtime.begin_shutdown();
    assert_eq!(runtime.state(), SWRuntimeState::Closing);
    assert!(route.changed_since(stamp).unwrap());
    drop(runtime);
    let _ = route.prepare_wait().unwrap();
    route.close().unwrap();
    assert!(route.is_quiescent());
    assert!(calls.load(Ordering::SeqCst) > 0);
}
