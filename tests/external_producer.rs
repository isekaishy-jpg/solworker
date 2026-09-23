use std::num::NonZeroUsize;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use solworker::{
    SWDemandSnapshot, SWExecutionClass, SWExternalOptions, SWOutcome, SWOwnedLimits, SWPriority,
    SWRuntime, SWRuntimeConfig, SWShutdownError, SWSpawnError, SWSpawnOptions, SWTaskStatus,
    SWWorkerConfig,
};

fn runtime(records: usize) -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let limits = SWOwnedLimits::new(records, 8, [8; 3], [1; 3]).unwrap();
    SWRuntime::builder(config)
        .with_owned_limits(limits)
        .with_demand_limits(vec![SWPriority::new(0), SWPriority::new(5)], 8)
        .build()
        .unwrap()
}

#[test]
fn external_outcome_occupies_metadata_without_using_worker_handoff() {
    let mut runtime = runtime(1);
    let (producer, mut task, _) = runtime
        .external::<u32>(SWExternalOptions::default())
        .unwrap();
    let rejected = runtime
        .lane(SWExecutionClass::Low)
        .try_spawn(SWSpawnOptions::default(), || 9_u32)
        .err()
        .unwrap();
    assert_eq!(rejected.reason, SWSpawnError::Full);
    assert!(producer.complete(7).is_ok());
    assert_eq!(task.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    assert!(matches!(task.try_take(), Some(SWOutcome::Success(value)) if *value == 7));
    let (cpu, _) = runtime
        .lane(SWExecutionClass::Low)
        .try_spawn(SWSpawnOptions::default(), || 9_u32)
        .unwrap();
    assert_eq!(cpu.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    runtime.shutdown().unwrap();
}

#[test]
fn cancellation_and_drop_publish_one_terminal_outcome_and_return_late_value() {
    let mut runtime = runtime(2);
    let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let (producer, mut task, control) = runtime
        .external::<String>(SWExternalOptions {
            work_set: Some(&set),
            ..Default::default()
        })
        .unwrap();
    set.cancel();
    assert_eq!(task.completion().wait().unwrap(), SWTaskStatus::Cancelled);
    let late = String::from("provider still owns this");
    assert_eq!(
        producer.complete(late).unwrap_err(),
        "provider still owns this"
    );
    control.cancel();
    drop(producer);
    assert!(matches!(task.try_take(), Some(SWOutcome::Cancelled)));
    assert!(set.progress().is_drained());

    let (producer, mut task, _) = runtime
        .external::<u8>(SWExternalOptions::default())
        .unwrap();
    drop(producer);
    assert_eq!(task.completion().wait().unwrap(), SWTaskStatus::Abandoned);
    assert!(matches!(task.try_take(), Some(SWOutcome::Abandoned)));

    let pending_set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let (set_producer, mut set_task, _) = runtime
        .external::<u32>(SWExternalOptions {
            work_set: Some(&pending_set),
            ..Default::default()
        })
        .unwrap();
    let (ordinary_producer, mut ordinary_task, _) = runtime
        .external::<u32>(SWExternalOptions::default())
        .unwrap();
    assert_eq!(runtime.shutdown(), Err(SWShutdownError::LiveExternal));
    runtime.abandon();
    for task in [&mut set_task, &mut ordinary_task] {
        assert_eq!(task.completion().wait().unwrap(), SWTaskStatus::Abandoned);
        assert!(matches!(task.try_take(), Some(SWOutcome::Abandoned)));
    }
    assert_eq!(set_producer.complete(17), Err(17));
    assert_eq!(ordinary_producer.complete(19), Err(19));
    assert!(pending_set.is_drained());
}

#[test]
fn concurrent_completion_and_cancel_publish_exactly_once() {
    let mut runtime = runtime(1);
    let (producer, mut task, control) = runtime
        .external::<u32>(SWExternalOptions::default())
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let worker_barrier = Arc::clone(&barrier);
    let completion = thread::spawn(move || {
        worker_barrier.wait();
        producer.complete(11)
    });
    barrier.wait();
    control.cancel();
    let completion = completion.join().unwrap();
    match completion {
        Ok(()) => {
            assert_eq!(task.completion().wait().unwrap(), SWTaskStatus::Succeeded);
            assert!(matches!(task.try_take(), Some(SWOutcome::Success(value)) if *value == 11));
        }
        Err(value) => {
            assert_eq!(value, 11);
            assert_eq!(task.completion().wait().unwrap(), SWTaskStatus::Cancelled);
            assert!(matches!(task.try_take(), Some(SWOutcome::Cancelled)));
        }
    }
    assert!(task.try_take().is_none());
    runtime.shutdown().unwrap();
}

#[test]
fn fallible_external_result_preserves_error_and_provider_demand_versions() {
    for baseline in [None, Some(SWPriority::new(5))] {
        let mut runtime = runtime(2);
        let observed = Arc::new(Mutex::new(Vec::<SWDemandSnapshot>::new()));
        let sent = Arc::clone(&observed);
        let (producer, mut task, _) = runtime
            .external::<Result<u32, &'static str>>(SWExternalOptions {
                priority: baseline,
                provider_demand: Some(Arc::new(move |snapshot| {
                    sent.lock().unwrap().push(snapshot);
                })),
                ..Default::default()
            })
            .unwrap();
        let interest = task.completion().demand(SWPriority::new(0)).unwrap();
        while runtime.service_demand(1) {}
        interest.defer().unwrap();
        while runtime.service_demand(1) {}
        interest.refresh(SWPriority::new(0)).unwrap();
        while runtime.service_demand(1) {}
        drop(interest);
        while runtime.service_demand(1) {}
        assert_eq!(task.completion().status(), None);
        let snapshots = observed.lock().unwrap();
        assert_eq!(
            snapshots
                .iter()
                .map(|snapshot| (snapshot.priority, snapshot.active))
                .collect::<Vec<_>>(),
            vec![
                (baseline, baseline.is_some()),
                (Some(SWPriority::new(0)), true),
                (Some(SWPriority::new(0)), false),
                (Some(SWPriority::new(0)), true),
                (baseline, baseline.is_some()),
            ],
            "baseline: {baseline:?}",
        );
        assert!(
            snapshots
                .windows(2)
                .all(|pair| pair[0].version < pair[1].version)
        );
        drop(snapshots);
        assert!(producer.complete_fallible(Err("failed")).is_ok());
        assert_eq!(
            task.completion().wait().unwrap(),
            SWTaskStatus::ApplicationFailed
        );
        assert!(
            matches!(task.try_take(), Some(SWOutcome::Success(value)) if *value == Err("failed"))
        );
        runtime.shutdown().unwrap();
    }
}

#[test]
fn panicking_provider_demand_does_not_lose_committed_result() {
    let mut runtime = runtime(1);
    let (producer, task, _) = runtime
        .external::<u32>(SWExternalOptions {
            priority: Some(SWPriority::new(5)),
            provider_demand: Some(Arc::new(|_| panic!("provider demand failed"))),
            ..Default::default()
        })
        .unwrap();
    let interest = task.completion().demand(SWPriority::new(0)).unwrap();
    while runtime.service_demand(1) {}
    assert!(producer.complete(42).is_ok());
    assert_eq!(task.completion().wait().unwrap(), SWTaskStatus::Succeeded);
    drop(interest);
    runtime.shutdown().unwrap();
}
