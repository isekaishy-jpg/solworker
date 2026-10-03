use std::num::NonZeroUsize;

use solworker::{
    SWDeliveryStatus, SWPhase, SWPumpBudget, SWPumpMode, SWRuntime, SWRuntimeConfig, SWWorkerConfig,
};

#[test]
fn batch_preserves_entry_eligible_frontier_when_callback_requests_close() {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let mut runtime = SWRuntime::builder(config).build().unwrap();
    let mut owner = runtime.owner((), NonZeroUsize::new(3).unwrap()).unwrap();
    let current_phase = SWPhase(1);
    let other_phase = SWPhase(2);
    owner.set_phase(current_phase).unwrap();

    // Queue order is [other phase, current phase #1, current phase #2].
    // Only the latter two belong to the Batch frontier at pump entry.
    let (outside_frontier, _) = owner.try_post(other_phase, |_| {}).unwrap();
    let close = owner.control();
    let (first, _) = owner
        .try_post(current_phase, move |_| close.request_close())
        .unwrap();
    let (second, _) = owner.try_post(current_phase, |_| {}).unwrap();

    let report = owner
        .pump(
            current_phase,
            SWPumpBudget::new(3).with_mode(SWPumpMode::Batch),
        )
        .unwrap();
    let after_pump = (first.status(), second.status(), outside_frontier.status());

    // Complete cleanup before asserting so a failing regression does not
    // leave worker threads or outstanding owner obligations behind.
    owner.close();
    runtime.shutdown().unwrap();

    assert_eq!(report.invoked, 1);
    assert_eq!(report.suppressed, 1);
    assert_eq!(
        after_pump,
        (
            SWDeliveryStatus::Published,
            SWDeliveryStatus::Suppressed,
            SWDeliveryStatus::Ready,
        ),
        "Batch must retain the identities eligible at entry; a close request \
         must not replace an entry-frontier delivery with another phase's delivery"
    );
}

#[test]
fn batch_preserves_entry_frontier_when_callback_panics() {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let mut runtime = SWRuntime::builder(config).build().unwrap();
    let mut owner = runtime.owner((), NonZeroUsize::new(3).unwrap()).unwrap();
    let phase = SWPhase(1);
    owner.set_phase(phase).unwrap();
    let (outside, _) = owner.try_post(SWPhase(2), |_| {}).unwrap();
    let (first, _) = owner
        .try_post(phase, |_| panic!("fault current batch"))
        .unwrap();
    let (second, _) = owner
        .try_post(phase, |_| panic!("must be suppressed"))
        .unwrap();
    let report = owner
        .pump(phase, SWPumpBudget::new(3).with_mode(SWPumpMode::Batch))
        .unwrap();
    assert_eq!(report.invoked, 0);
    assert_eq!(report.suppressed, 2);
    assert_eq!(first.status(), SWDeliveryStatus::Panicked);
    assert_eq!(second.status(), SWDeliveryStatus::Suppressed);
    assert_eq!(outside.status(), SWDeliveryStatus::Ready);
    owner.close();
    assert_eq!(outside.status(), SWDeliveryStatus::Suppressed);
    runtime.shutdown().unwrap();
}

#[test]
fn next_batch_after_close_can_clean_other_phases_within_its_budget() {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let mut runtime = SWRuntime::builder(config).build().unwrap();
    let mut owner = runtime.owner((), NonZeroUsize::new(3).unwrap()).unwrap();
    let phase = SWPhase(1);
    owner.set_phase(phase).unwrap();
    let (outside, _) = owner.try_post(SWPhase(2), |_| {}).unwrap();
    let close = owner.control();
    let (first, _) = owner
        .try_post(phase, move |_| close.request_close())
        .unwrap();
    let (second, _) = owner.try_post(phase, |_| {}).unwrap();
    let one = SWPumpBudget::new(1).with_mode(SWPumpMode::Batch);
    assert_eq!(owner.pump(phase, one).unwrap().invoked, 1);
    assert_eq!(first.status(), SWDeliveryStatus::Published);
    assert_eq!(outside.status(), SWDeliveryStatus::Ready);
    assert_eq!(second.status(), SWDeliveryStatus::Ready);
    assert_eq!(owner.pump(phase, one).unwrap().suppressed, 1);
    assert_eq!(outside.status(), SWDeliveryStatus::Suppressed);
    assert_eq!(second.status(), SWDeliveryStatus::Ready);
    assert_eq!(owner.pump(phase, one).unwrap().suppressed, 1);
    assert_eq!(second.status(), SWDeliveryStatus::Suppressed);
    owner.close();
    runtime.shutdown().unwrap();
}
