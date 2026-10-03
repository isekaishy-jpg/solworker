use crate::{
    SWCallerEligibility, SWDependencyPolicy, SWExecutionClass, SWExternalOptions, SWGroup,
    SWOutcome, SWOwnedLimits, SWProducer, SWProducerControl, SWRetained, SWRuntime,
    SWRuntimeConfig, SWSpawnOptions, SWTask, SWTaskStatus, SWWorkerConfig,
};

struct PendingMembers {
    runtime: SWRuntime,
    group: SWGroup,
    producer: SWProducer<()>,
    _gate_task: SWTask<SWRetained<()>>,
    tasks: Vec<(SWTask<usize>, SWProducerControl)>,
}

impl PendingMembers {
    fn new(count: usize) -> Self {
        let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
        let runtime = SWRuntime::builder(config)
            .with_owned_limits(SWOwnedLimits::new(count + 1, count, [count; 3], [1; 3]).unwrap())
            .build()
            .unwrap();
        let lane = runtime.lane(SWExecutionClass::High);
        let group = lane.group().unwrap();
        let (producer, gate_task, _) = runtime
            .external::<()>(SWExternalOptions::default())
            .unwrap();
        let prerequisites = [gate_task.completion()];
        let tasks = (0..count)
            .map(|id| {
                lane.try_spawn_after_in(
                    &group,
                    SWSpawnOptions {
                        eligibility: SWCallerEligibility::CallerEligible,
                    },
                    &prerequisites,
                    SWDependencyPolicy::SuccessOnly,
                    move || id,
                )
                .unwrap()
            })
            .collect();
        Self {
            runtime,
            group,
            producer,
            _gate_task: gate_task,
            tasks,
        }
    }

    fn cancel_and_drain(mut self) {
        self.group.seal();
        for (task, control) in &mut self.tasks {
            control.cancel();
            assert_eq!(task.try_take(), Some(SWOutcome::Cancelled));
        }
        self.group.wait_helping().unwrap();
        assert!(self.group.inner.members.lock().unwrap().is_empty());
        self.producer.complete(()).unwrap();
        self.runtime.shutdown().unwrap();
    }
}

#[test]
fn traversal_keeps_stable_id_order_after_shuffled_registration_and_retirement() {
    let fixture = PendingMembers::new(64);
    let entries = std::mem::take(&mut *fixture.group.inner.members.lock().unwrap());
    let ids: Vec<_> = entries.keys().copied().collect();
    let mut reversed: Vec<_> = entries.into_values().rev().collect();
    fixture.group.inner.register_member(reversed.pop().unwrap());
    fixture.group.inner.register_members(reversed);

    for index in (1..ids.len()).step_by(3) {
        fixture.tasks[index].1.cancel();
    }
    let live_ids: Vec<_> = fixture
        .group
        .inner
        .members
        .lock()
        .unwrap()
        .keys()
        .copied()
        .collect();
    assert_eq!(
        live_ids,
        ids.iter()
            .enumerate()
            .filter_map(|(index, &id)| (index % 3 != 1).then_some(id))
            .collect::<Vec<_>>()
    );
    let mut cursor = 0;
    for &id in &live_ids {
        let next = fixture.group.inner.member_after(cursor).unwrap();
        let expected = fixture
            .group
            .inner
            .members
            .lock()
            .unwrap()
            .get(&id)
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(std::ptr::eq(&*next, &*expected));
        // A returned control must not keep membership locked across a claim.
        assert!(fixture.group.inner.members.try_lock().is_ok());
        cursor = id;
    }
    assert!(fixture.group.inner.member_after(cursor).is_none());
    assert!(fixture.group.inner.member_after(u64::MAX).is_none());
    assert!(!fixture.group.help_ready().unwrap());
    fixture.cancel_and_drain();
}

#[test]
fn expired_membership_does_not_retarget_recycled_controls_or_hide_later_members() {
    let mut fixture = PendingMembers::new(3);
    let ids: Vec<_> = fixture
        .group
        .inner
        .members
        .lock()
        .unwrap()
        .keys()
        .copied()
        .collect();
    let stale = fixture
        .group
        .inner
        .members
        .lock()
        .unwrap()
        .remove(&ids[1])
        .unwrap();
    fixture.tasks[1].1.cancel();
    assert!(stale.upgrade().is_none());
    let replacement = fixture
        .runtime
        .lane(SWExecutionClass::High)
        .try_spawn_after_in(
            &fixture.group,
            SWSpawnOptions {
                eligibility: SWCallerEligibility::CallerEligible,
            },
            &[fixture._gate_task.completion()],
            SWDependencyPolicy::SuccessOnly,
            || 99,
        )
        .unwrap();
    assert!(stale.upgrade().is_none());
    fixture.tasks.push(replacement);
    fixture.group.inner.register_member(stale);
    let later = fixture.group.inner.member_after(ids[0]).unwrap();
    let expected = fixture
        .group
        .inner
        .members
        .lock()
        .unwrap()
        .get(&ids[2])
        .unwrap()
        .upgrade()
        .unwrap();
    assert!(std::ptr::eq(&*later, &*expected));
    drop((later, expected));
    fixture.group.inner.retire_member(ids[1]);
    fixture.cancel_and_drain();
}

#[test]
fn retired_members_leave_reset_group_empty_and_old_completion_terminal() {
    let mut fixture = PendingMembers::new(8);
    let old = fixture.group.completion();
    fixture.group.seal();
    for (_, control) in &fixture.tasks {
        control.cancel();
    }
    fixture.group.wait_helping().unwrap();
    assert_eq!(old.status(), Some(SWTaskStatus::PrerequisiteFailed));
    assert!(fixture.group.inner.members.lock().unwrap().is_empty());
    assert!(fixture.group.try_reset(100));
    assert_eq!(fixture.group.completion().status(), None);
    assert_eq!(old.status(), Some(SWTaskStatus::PrerequisiteFailed));
    assert!(fixture.group.inner.member_after(0).is_none());
    assert!(fixture.group.inner.members.lock().unwrap().is_empty());
    fixture.group.seal();
    assert_eq!(
        fixture.group.completion().status(),
        Some(SWTaskStatus::Succeeded)
    );
    fixture.producer.complete(()).unwrap();
    fixture.runtime.shutdown().unwrap();
}
