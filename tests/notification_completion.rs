use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use solworker::{
    SWExecutionClass, SWExternalOptions, SWNotifyError, SWNotifyLimits, SWOwnedLimits, SWRuntime,
    SWRuntimeConfig, SWSpawnOptions, SWTask, SWTaskStatus, SWWorkerConfig,
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn runtime(notifications: bool) -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(8, 8, [8; 3], [2; 3]).unwrap();
    let mut builder = SWRuntime::builder(config).with_owned_limits(limits);
    if notifications {
        builder = builder.with_notification_limits(SWNotifyLimits {
            routes: 4,
            bindings: 8,
        });
    }
    builder.build().unwrap()
}

#[test]
fn task_and_external_completion_signal_after_terminal_state_is_visible() {
    let mut runtime = runtime(true);
    let observed = Arc::new(Mutex::new(None));
    let observed_in_signal = Arc::clone(&observed);
    let (status_tx, status_rx) = mpsc::channel();
    let mut route = runtime
        .notification_route(move || {
            if let Some(status) = observed_in_signal
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|completion: &solworker::SWCompletion| completion.status())
            {
                status_tx.send(status).unwrap();
            }
            Ok(())
        })
        .unwrap();

    let (producer, external, _) = runtime
        .external::<u32>(SWExternalOptions::default())
        .unwrap();
    let external_completion = external.completion();
    *observed.lock().unwrap() = Some(external_completion.clone());
    let _external_binding = route.watch_completion(&external_completion).unwrap();
    let before = route.prepare_wait().unwrap();
    producer.complete(7).unwrap();
    assert_eq!(external_completion.status(), Some(SWTaskStatus::Succeeded));
    assert!(route.changed_since(before).unwrap());
    assert_eq!(
        status_rx.recv_timeout(TIMEOUT).unwrap(),
        SWTaskStatus::Succeeded
    );

    let lane = runtime.lane(SWExecutionClass::Low);
    let (release_tx, release_rx) = mpsc::channel();
    let (task, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            release_rx.recv_timeout(TIMEOUT).unwrap();
            11_u32
        })
        .unwrap();
    let completion = task.completion();
    *observed.lock().unwrap() = Some(completion.clone());
    let _task_binding = route.watch_completion(&completion).unwrap();
    let before = route.prepare_wait().unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(
        completion.wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    assert_eq!(
        status_rx.recv_timeout(TIMEOUT).unwrap(),
        SWTaskStatus::Succeeded
    );
    assert!(route.changed_since(before).unwrap());
    runtime.shutdown().unwrap();
}

#[test]
fn reusable_group_waves_keep_old_completion_bindings_isolated() {
    let mut runtime = runtime(true);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let mut old_route = runtime
        .notification_route(move || {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::Low);
    let mut batch = lane.batch();
    let first = batch.begin().unwrap();
    let old_completion = first.completion();
    let _old_binding = old_route.watch_completion(&old_completion).unwrap();
    let stamp = old_route.prepare_wait().unwrap();
    first.seal();
    assert_eq!(old_completion.status(), Some(SWTaskStatus::Succeeded));
    assert!(old_route.changed_since(stamp).unwrap());
    // The binding is now the only retained observer of the old wave. Renewal
    // must not retarget it even if the batch can reuse its group allocation.
    drop(old_completion);

    let stamp = old_route.prepare_wait().unwrap();
    let second = batch.begin().unwrap();
    let new_completion = second.completion();
    assert_eq!(new_completion.status(), None);
    second.seal();
    assert_eq!(new_completion.status(), Some(SWTaskStatus::Succeeded));
    assert!(!old_route.changed_since(stamp).unwrap());

    let mut new_route = runtime.notification_route(|| Ok(())).unwrap();
    let _new_binding = new_route.watch_completion(&new_completion).unwrap();
    assert!(calls.load(Ordering::SeqCst) > 0);
    runtime.shutdown().unwrap();
}

#[test]
fn standalone_ready_is_inert_but_runtime_tokens_keep_their_identity() {
    let mut first = runtime(true);
    let mut second = runtime(true);
    let disabled = runtime(false);
    let mut route = first.notification_route(|| Ok(())).unwrap();

    let ready = SWTask::ready(9_u32).completion();
    let stamp = route.prepare_wait().unwrap();
    let _ready_binding = route.watch_completion(&ready).unwrap();
    assert!(route.changed_since(stamp).unwrap());

    let (producer, task, _) = second
        .external::<u32>(SWExternalOptions::default())
        .unwrap();
    let foreign = task.completion();
    assert_eq!(
        route.watch_completion(&foreign).err(),
        Some(SWNotifyError::ForeignSource)
    );
    producer.complete(2).unwrap();
    assert_eq!(
        route.watch_completion(&foreign).err(),
        Some(SWNotifyError::ForeignSource)
    );

    let pending_group = disabled.lane(SWExecutionClass::Low).group().unwrap();
    assert_eq!(
        route.watch_completion(&pending_group.completion()).err(),
        Some(SWNotifyError::ForeignSource)
    );
    pending_group.seal();
    assert_eq!(
        route.watch_completion(&pending_group.completion()).err(),
        Some(SWNotifyError::ForeignSource)
    );

    first.shutdown().unwrap();
    second.shutdown().unwrap();
}
