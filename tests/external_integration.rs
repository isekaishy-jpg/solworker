use std::num::NonZeroUsize;

use solworker::{
    SWCost, SWExternalAccessOptions, SWExternalOptions, SWLimits, SWOutcome, SWOwnedLimits,
    SWPhase, SWPumpBudget, SWRuntime, SWRuntimeConfig, SWShutdownError, SWSpawnError, SWTaskStatus,
    SWWorkerConfig,
};

fn runtime() -> SWRuntime {
    SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
        .with_owned_limits(SWOwnedLimits::new(8, 8, [4; 3], [1; 3]).unwrap())
        .with_external_capacity(NonZeroUsize::new(1).unwrap())
        .with_capacity_limits(
            SWLimits::new(
                SWCost::new(4, 4, 1, 32),
                SWCost::new(2, 0, 1, 0),
                1,
                Some(64),
            )
            .unwrap(),
        )
        .build()
        .unwrap()
}

#[test]
fn physical_release_and_logical_publication_keep_independent_sets_and_one_byte_charge() {
    for cancel in [false, true] {
        let mut runtime = runtime();
        let producer_set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
        let physical_set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
        let consumer_set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
        let mut owner = runtime
            .owner(Vec::new(), NonZeroUsize::new(1).unwrap())
            .unwrap();
        let phase = SWPhase(1);
        owner.set_phase(phase).unwrap();
        owner.try_post(phase, |values| values.push(1)).unwrap();
        let reserve = runtime.reserve_required(SWCost::new(2, 0, 1, 16)).unwrap();
        let delivery = owner
            .prepare_delivery_reserved_in(&consumer_set, &reserve, phase, |values| values.push(2))
            .unwrap();
        let (producer, task, _) = runtime
            .external::<Vec<u8>>(SWExternalOptions {
                work_set: Some(&producer_set),
                reservation: Some(&reserve),
                delivery: Some(delivery.ticket),
                cost: SWCost::new(1, 0, 1, 0),
                ..Default::default()
            })
            .unwrap();
        let shared = task.into_shared();
        let access = runtime
            .prepare_external(
                vec![7u8; 16],
                SWExternalAccessOptions {
                    work_set: Some(&physical_set),
                    reservation: Some(&reserve),
                    cost: SWCost::new(1, 0, 0, 16),
                    ..Default::default()
                },
            )
            .unwrap()
            .activate()
            .unwrap();
        producer_set.seal();
        physical_set.seal();
        consumer_set.seal();
        if cancel {
            producer_set.cancel();
            assert_eq!(shared.status(), Some(SWTaskStatus::Cancelled));
            assert!(producer_set.is_drained());
        }
        assert!(!physical_set.is_drained());
        assert_eq!(runtime.external_progress().active, 1);
        // This simulated provider is the only accessor. All pointer use ends
        // synchronously here, before acknowledgement and safe publication.
        let retained = unsafe {
            let resource = access.as_mut_ptr();
            (&mut *resource)[0] = 9;
            access.acknowledge_release()
        };
        assert!(physical_set.is_drained());
        assert_eq!(runtime.external_progress().active, 0);
        assert_eq!(runtime.capacity_usage().unwrap().required.bytes, 16);
        if cancel {
            let returned = producer.complete_retained(retained).unwrap_err();
            assert_eq!(returned[0], 9);
            drop(returned);
        } else {
            producer.complete_retained(retained).unwrap();
        }
        assert!(producer_set.is_drained());
        assert_eq!(runtime.external_progress().logical, 0);
        assert!(!consumer_set.is_drained());
        assert_eq!(owner.pump(phase, SWPumpBudget::new(2)).unwrap().invoked, 2);
        assert_eq!(owner.state().as_slice(), &[1, 2]);
        assert!(consumer_set.is_drained());
        owner.close();
        drop(reserve);
        let outcome = shared.try_result().unwrap();
        drop(shared);
        if cancel {
            assert!(matches!(&*outcome, SWOutcome::Cancelled));
            assert_eq!(runtime.capacity_usage().unwrap().required.bytes, 0);
        } else {
            assert!(matches!(&*outcome, SWOutcome::Success(value) if value[0] == 9));
            assert_eq!(runtime.capacity_usage().unwrap().required.bytes, 16);
        }
        assert_eq!(runtime.capacity_usage().unwrap().required_pipelines, 0);
        runtime.shutdown().unwrap();
        drop(outcome);
        assert_eq!(runtime.capacity_usage().unwrap().required.bytes, 0);
    }
}

#[test]
fn physical_pressure_returns_inputs_and_protects_required_slots_until_release() {
    let mut runtime = runtime();
    let ordinary = runtime
        .prepare_external(
            vec![1u8; 8],
            SWExternalAccessOptions {
                cost: SWCost::new(1, 0, 0, 8),
                ..Default::default()
            },
        )
        .unwrap();
    let before = runtime.capacity_usage().unwrap();
    let rejected = runtime
        .prepare_external(
            vec![2u8; 8],
            SWExternalAccessOptions {
                cost: SWCost::new(1, 0, 0, 8),
                ..Default::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(rejected.reason, SWSpawnError::Full);
    assert_eq!(rejected.resource, vec![2u8; 8]);
    assert_eq!(runtime.capacity_usage().unwrap(), before);
    let reserve = runtime.reserve_required(SWCost::new(1, 0, 0, 8)).unwrap();
    let required = runtime
        .prepare_external(
            vec![3u8; 8],
            SWExternalAccessOptions {
                reservation: Some(&reserve),
                cost: SWCost::new(1, 0, 0, 8),
                ..Default::default()
            },
        )
        .unwrap()
        .activate()
        .unwrap();
    assert_eq!(runtime.shutdown(), Err(SWShutdownError::LiveExternal));
    drop(ordinary);
    assert_eq!(runtime.external_progress().prepared, 0);
    runtime.abandon();
    // No provider accesses were started in this case; absence of foreign users
    // supplies the acknowledgement even though runtime abandonment alone cannot.
    unsafe {
        required.release();
    }
    assert_eq!(runtime.external_progress().active, 0);
    assert_eq!(reserve.available(), SWCost::new(1, 0, 0, 8));
    drop(reserve);
    assert_eq!(runtime.capacity_usage().unwrap().required.bytes, 0);
}
