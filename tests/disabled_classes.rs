use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use solworker::{
    SWBatchSpawnOptions, SWCallerEligibility, SWDeliveryOptions, SWDeliveryStatus,
    SWDependencyPolicy, SWExecutionClass, SWExecutionError, SWExternalOptions, SWOutcome,
    SWOwnedLimits, SWPhase, SWPumpBudget, SWRuntime, SWRuntimeConfig, SWSpawnError, SWSpawnOptions,
    SWStageOptions, SWTask, SWTaskStatus, SWWorkerConfig,
};

fn runtime(counts: [usize; 3]) -> SWRuntime {
    SWRuntime::builder(
        SWRuntimeConfig::new(counts.iter().sum(), counts.map(SWWorkerConfig::new)).unwrap(),
    )
    .with_owned_limits(SWOwnedLimits::new(16, 16, [8; 3], [4; 3]).unwrap())
    .build()
    .unwrap()
}

#[test]
fn every_disabled_class_combination_starts_only_requested_workers_and_closes() {
    for mask in 0..8 {
        let counts = std::array::from_fn(|index| usize::from(mask & (1 << index) != 0));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let setup = Arc::clone(&observed);
        let mut runtime = SWRuntime::builder(
            SWRuntimeConfig::new(counts.iter().sum(), counts.map(SWWorkerConfig::new)).unwrap(),
        )
        .with_owned_limits(SWOwnedLimits::new(8, 0, [4; 3], [2; 3]).unwrap())
        .with_worker_setup(move |class, worker| {
            setup.lock().unwrap().push((class, worker));
            Ok(())
        })
        .build()
        .unwrap();
        assert_eq!(observed.lock().unwrap().len(), counts.iter().sum::<usize>());
        for (index, class) in SWExecutionClass::ALL.into_iter().enumerate() {
            let lane = runtime.lane(class);
            if counts[index] == 0 {
                assert!(
                    !observed
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|(seen, _)| *seen == class)
                );
                assert!(
                    matches!(lane.group(), Err(SWSpawnError::ClassDisabled(seen)) if seen == class)
                );
                let rejected = lane.join(|| 3, || 4).err().unwrap();
                assert_eq!(rejected.reason, SWExecutionError::ClassDisabled(class));
                assert_eq!((rejected.left)() + (rejected.right)(), 7);
                let rejected = lane
                    .try_spawn(SWSpawnOptions::default(), || 9)
                    .err()
                    .unwrap();
                assert_eq!(rejected.reason, SWSpawnError::ClassDisabled(class));
                assert_eq!((rejected.operation)(), 9);
            } else {
                let (task, _) = lane.try_spawn(SWSpawnOptions::default(), || 7).unwrap();
                assert_eq!(
                    task.completion()
                        .wait_timeout(Duration::from_secs(5))
                        .unwrap(),
                    Some(SWTaskStatus::Succeeded)
                );
                let (left, right) = lane.join(|| 1, || 2).unwrap();
                assert_eq!(left.unwrap() + right.unwrap(), 3);
            }
        }
        runtime.shutdown().unwrap();
    }
}

#[test]
fn disabled_admission_preserves_batches_prerequisites_and_explicit_inline_work() {
    let mut runtime = runtime([0, 1, 0]);
    let lane = runtime.lane(SWExecutionClass::Low);
    let options = SWSpawnOptions {
        eligibility: SWCallerEligibility::CallerEligible,
    };
    let rejected = lane.submit_or_run(options, || 12).err().unwrap();
    assert_eq!(
        rejected.reason,
        SWSpawnError::ClassDisabled(SWExecutionClass::Low)
    );
    assert_eq!((rejected.operation)(), 12);
    let ready = SWTask::ready(1);
    let rejected = lane
        .try_spawn_after(
            options,
            &[ready.completion()],
            SWDependencyPolicy::SuccessOnly,
            || 13,
        )
        .err()
        .unwrap();
    assert_eq!(
        rejected.reason,
        SWSpawnError::ClassDisabled(SWExecutionClass::Low)
    );
    assert_eq!((rejected.operation)(), 13);
    let operations: Vec<_> = (0..40).map(|value| move || value).collect();
    let rejected = lane
        .try_spawn_batch(SWBatchSpawnOptions::default(), operations)
        .err()
        .unwrap();
    assert_eq!(
        rejected.reason,
        SWSpawnError::ClassDisabled(SWExecutionClass::Low)
    );
    assert!(rejected.accepted.is_empty());
    assert_eq!(
        rejected.remaining.into_iter().map(|f| f()).sum::<i32>(),
        780
    );
    assert!(
        lane.try_spawn_batch::<fn() -> (), ()>(SWBatchSpawnOptions::default(), Vec::new())
            .unwrap()
            .is_empty()
    );
    assert!(runtime.try_shutdown().unwrap());
}

#[test]
fn disabled_stage_and_discovery_do_not_consume_accounting_or_call_operations() {
    let mut runtime = runtime([0, 1, 0]);
    let lane = runtime.lane(SWExecutionClass::High);
    let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let permit = set.discovery().unwrap();
    let rejected = set
        .try_spawn(&lane, SWSpawnOptions::default(), || 21)
        .err()
        .unwrap();
    assert_eq!(
        rejected.reason,
        SWSpawnError::ClassDisabled(SWExecutionClass::High)
    );
    assert_eq!((rejected.operation)(), 21);
    set.seal();
    let rejected = permit
        .try_spawn(&lane, SWSpawnOptions::default(), || 22)
        .err()
        .unwrap();
    assert_eq!(
        rejected.reason,
        SWSpawnError::ClassDisabled(SWExecutionClass::High)
    );
    assert_eq!((rejected.operation)(), 22);
    let rejected = lane
        .try_spawn_stage(SWStageOptions::default(), || 23)
        .err()
        .unwrap();
    assert_eq!(
        rejected.reason,
        SWSpawnError::ClassDisabled(SWExecutionClass::High)
    );
    assert_eq!((rejected.operation)(), 23);
    assert_eq!(set.progress().active_work, 0);
    assert_eq!(set.progress().discovery_permits, 1);
    drop(permit);
    assert!(set.is_drained());
    assert!(runtime.try_shutdown().unwrap());
}

#[test]
fn disabled_scoped_work_does_not_run_an_owner_branch_or_mutate_borrowed_data() {
    let mut runtime = runtime([0; 3]);
    let lane = runtime.lane(SWExecutionClass::Mid);
    let mut owner_value = 0;
    {
        let rejected = lane
            .join_with_owner(|| panic!("disabled worker branch ran"), || owner_value = 1)
            .err()
            .unwrap();
        assert_eq!(
            rejected.reason,
            SWExecutionError::ClassDisabled(SWExecutionClass::Mid)
        );
    }
    assert_eq!(owner_value, 0);
    let mut data = [1, 2];
    let rejected = lane
        .for_each_chunk(&mut data, NonZeroUsize::new(1).unwrap(), |_, chunk| {
            chunk[0] = 0
        })
        .err()
        .unwrap();
    assert_eq!(
        rejected.reason,
        SWExecutionError::ClassDisabled(SWExecutionClass::Mid)
    );
    assert_eq!(data, [1, 2]);
    assert!(runtime.try_shutdown().unwrap());
}

#[test]
fn disabled_admission_returns_a_delivery_ticket_usable_on_an_enabled_class() {
    let mut runtime = runtime([0, 1, 0]);
    let phase = SWPhase(1);
    let mut owner = runtime.owner(0, NonZeroUsize::new(1).unwrap()).unwrap();
    owner.set_phase(phase).unwrap();
    let prepared = owner.prepare_delivery(phase, |state| *state += 1).unwrap();
    let rejected = runtime
        .lane(SWExecutionClass::Low)
        .try_spawn_delivering(SWDeliveryOptions::default(), prepared.ticket, || 7)
        .err()
        .unwrap();
    assert_eq!(
        rejected.reason,
        SWSpawnError::ClassDisabled(SWExecutionClass::Low)
    );
    assert_eq!(prepared.delivery.status(), SWDeliveryStatus::Waiting);
    let lane = runtime.lane(SWExecutionClass::Mid);
    let group = lane.group().unwrap();
    let (mut task, _) = lane
        .try_spawn_delivering(
            SWDeliveryOptions {
                group: Some(&group),
                ..rejected.options
            },
            rejected.ticket,
            rejected.operation,
        )
        .unwrap();
    group.seal();
    group.wait_helping().unwrap();
    assert!(matches!(task.try_take(), Some(SWOutcome::Success(value)) if value == 7));
    assert_eq!(owner.pump(phase, SWPumpBudget::new(1)).unwrap().invoked, 1);
    assert_eq!(*owner.state(), 1);
    assert_eq!(prepared.delivery.status(), SWDeliveryStatus::Published);
    owner.close();
    runtime.shutdown().unwrap();
}

#[test]
fn all_disabled_runtime_still_services_external_results_and_owner_posts() {
    let mut runtime = runtime([0; 3]);
    let (producer, mut task, _) = runtime
        .external::<u32>(SWExternalOptions::default())
        .unwrap();
    assert!(producer.complete(19).is_ok());
    assert!(matches!(task.try_take(), Some(SWOutcome::Success(value)) if *value == 19));
    let phase = SWPhase(1);
    let mut owner = runtime.owner(0, NonZeroUsize::new(1).unwrap()).unwrap();
    owner.set_phase(phase).unwrap();
    owner.try_post(phase, |state| *state = 2).unwrap();
    assert_eq!(owner.pump(phase, SWPumpBudget::new(1)).unwrap().invoked, 1);
    assert_eq!(*owner.state(), 2);
    owner.close();
    runtime.shutdown().unwrap();
}
