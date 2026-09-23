use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::time::Duration;

use solworker::{
    SWDeliveryOptions, SWDeliveryStatus, SWExecutionClass, SWOwnedLimits, SWPhase, SWPumpBudget,
    SWRuntime, SWRuntimeConfig, SWSpawnError, SWSpawnOptions, SWTaskStatus, SWWorkerConfig,
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn runtime(records: usize, runnable: usize) -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(records, records, [runnable; 3], [1; 3]).unwrap();
    SWRuntime::builder(config)
        .with_owned_limits(limits)
        .build()
        .unwrap()
}

#[test]
fn full_cpu_admission_returns_usable_delivery_ticket_and_operation() {
    let mut runtime = runtime(2, 1);
    let lane = runtime.lane(SWExecutionClass::Low);
    let group = lane.group().unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    lane.try_spawn_in(&group, SWSpawnOptions::default(), move || {
        entered_tx.send(()).unwrap();
        release_rx.recv_timeout(TIMEOUT).unwrap();
    })
    .unwrap();
    entered_rx.recv_timeout(TIMEOUT).unwrap();
    lane.try_spawn_in(&group, SWSpawnOptions::default(), || 1usize)
        .unwrap();

    let phase = SWPhase(1);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(1).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let prepared = owner.prepare_delivery(phase, |state| *state += 1).unwrap();
    let rejected = lane
        .try_spawn_delivering(SWDeliveryOptions::default(), prepared.ticket, || 7usize)
        .err()
        .unwrap();
    assert_eq!(rejected.reason, SWSpawnError::Full);
    assert_eq!(prepared.delivery.status(), SWDeliveryStatus::Waiting);

    release_tx.send(()).unwrap();
    group.seal();
    group.wait_helping().unwrap();
    let delivery_group = lane.group().unwrap();
    let options = SWDeliveryOptions {
        group: Some(&delivery_group),
        ..rejected.options
    };
    let (task, _) = lane
        .try_spawn_delivering(options, rejected.ticket, rejected.operation)
        .unwrap();
    delivery_group.seal();
    delivery_group.wait_helping().unwrap();
    assert_eq!(task.completion().status(), Some(SWTaskStatus::Succeeded));
    assert_eq!(prepared.delivery.status(), SWDeliveryStatus::Ready);
    assert_eq!(owner.pump(phase, SWPumpBudget::new(1)).unwrap().invoked, 1);
    assert_eq!(*owner.state(), 1);
    assert_eq!(prepared.delivery.status(), SWDeliveryStatus::Published);
    owner.close();
    runtime.shutdown().unwrap();
}

#[test]
fn ticket_rejects_a_different_runtime_without_losing_its_reservation() {
    let mut first = runtime(4, 2);
    let mut second = runtime(4, 2);
    let phase = SWPhase(2);
    let mut owner = first.owner(0usize, NonZeroUsize::new(1).unwrap()).unwrap();
    owner.set_phase(phase).unwrap();
    let prepared = owner.prepare_delivery(phase, |state| *state += 1).unwrap();
    let rejected = second
        .lane(SWExecutionClass::Low)
        .try_spawn_delivering(SWDeliveryOptions::default(), prepared.ticket, || 3usize)
        .err()
        .unwrap();
    assert_eq!(rejected.reason, SWSpawnError::InvalidDelivery);
    assert_eq!(prepared.delivery.status(), SWDeliveryStatus::Waiting);

    let lane = first.lane(SWExecutionClass::Low);
    let group = lane.group().unwrap();
    let options = SWDeliveryOptions {
        group: Some(&group),
        ..rejected.options
    };
    let (task, _) = lane
        .try_spawn_delivering(options, rejected.ticket, rejected.operation)
        .unwrap();
    group.seal();
    group.wait_helping().unwrap();
    assert_eq!(task.completion().status(), Some(SWTaskStatus::Succeeded));
    owner.pump(phase, SWPumpBudget::new(1)).unwrap();
    assert_eq!(*owner.state(), 1);
    owner.close();
    first.shutdown().unwrap();
    second.shutdown().unwrap();
}

#[test]
fn closed_owner_rejects_cpu_admission_and_suppresses_delivery() {
    let mut runtime = runtime(4, 2);
    let phase = SWPhase(3);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(1).unwrap())
        .unwrap();
    let prepared = owner.prepare_delivery(phase, |state| *state += 1).unwrap();
    owner.close();
    assert_eq!(prepared.delivery.status(), SWDeliveryStatus::Suppressed);

    let rejected = runtime
        .lane(SWExecutionClass::Low)
        .try_spawn_delivering(SWDeliveryOptions::default(), prepared.ticket, || 5usize)
        .err()
        .unwrap();
    assert_eq!(rejected.reason, SWSpawnError::Closed);
    assert_eq!((rejected.operation)(), 5);
    drop(rejected.ticket);
    runtime.shutdown().unwrap();
}

#[test]
fn application_failure_still_delivers_one_terminal_owner_notification() {
    let mut runtime = runtime(4, 2);
    let phase = SWPhase(4);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(1).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let prepared = owner.prepare_delivery(phase, |state| *state += 1).unwrap();
    let lane = runtime.lane(SWExecutionClass::Low);
    let group = lane.group().unwrap();
    let options = SWDeliveryOptions {
        group: Some(&group),
        ..SWDeliveryOptions::default()
    };
    let (task, _) = lane
        .try_spawn_fallible_delivering(options, prepared.ticket, || {
            Err::<usize, &'static str>("decode failed")
        })
        .unwrap();
    group.seal();
    group.wait_helping().unwrap();
    assert_eq!(
        task.completion().status(),
        Some(SWTaskStatus::ApplicationFailed)
    );
    assert_eq!(prepared.delivery.status(), SWDeliveryStatus::Ready);
    assert_eq!(owner.pump(phase, SWPumpBudget::new(1)).unwrap().invoked, 1);
    assert_eq!(*owner.state(), 1);
    assert_eq!(prepared.delivery.status(), SWDeliveryStatus::Published);
    owner.close();
    runtime.shutdown().unwrap();
}
