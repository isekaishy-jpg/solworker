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

fn keys(group: &SWGroup) -> Vec<super::MembershipKey> {
    let members = group.inner.members.lock().unwrap();
    let mut keys = Vec::new();
    for (page_index, entry) in members.pages.iter().enumerate() {
        if let Some(page) = &entry.page {
            for (offset, slot) in page.slots.iter().enumerate() {
                if let Some(job) = &slot.job {
                    keys.push(super::MembershipKey {
                        group: group.inner.id,
                        incarnation: entry.incarnation,
                        slot: page_index * super::PAGE_SLOTS + offset,
                        generation: slot.generation,
                        job: job.id(),
                    });
                }
            }
        }
    }
    keys
}
#[test]
fn waiting_members_never_enter_helper_index_and_removed_keys_cannot_retarget() {
    let mut fixture = PendingMembers::new(65);
    assert!(fixture.group.inner.ready_member().is_none());
    assert!(!fixture.group.help_ready().unwrap());
    let original = keys(&fixture.group);
    let removed = original
        .iter()
        .find(|key| key.job == original[0].job)
        .copied()
        .unwrap();
    let removed_task = keys(&fixture.group)
        .iter()
        .map(|key| key.job)
        .min()
        .unwrap();
    let stale = original
        .iter()
        .find(|key| key.job == removed_task)
        .copied()
        .unwrap();
    fixture.tasks[0].1.cancel();
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
    fixture.tasks.push(replacement);
    fixture.group.inner.mark_ready(stale);
    fixture.group.inner.retire_member(stale);
    assert!(fixture.group.inner.ready_member().is_none());
    assert_eq!(keys(&fixture.group).len(), 65);
    assert!(keys(&fixture.group).iter().all(|key| key.job != stale.job));
    // A still-live key remains independently unlinkable after replacement.
    if removed != stale {
        fixture.group.inner.mark_ready(removed);
        assert!(fixture.group.inner.ready_member().is_some());
        fixture.group.inner.mark_unready(removed);
    }
    fixture.cancel_and_drain();
}

#[test]
fn expired_weak_association_never_upgrades_to_a_new_job_control() {
    let mut fixture = PendingMembers::new(1);
    let key = keys(&fixture.group)[0];
    let (stale, page) = fixture
        .group
        .inner
        .members
        .lock()
        .unwrap()
        .retire(fixture.group.inner.id, key);
    drop(page);
    let stale = stale.unwrap();
    fixture.tasks[0].1.cancel();
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
    fixture.tasks.push(replacement);
    assert!(stale.upgrade().is_none());
    drop(stale);
    fixture.cancel_and_drain();
}

#[test]
fn readiness_revocation_exposes_later_member_without_consuming_candidates() {
    let fixture = PendingMembers::new(128);
    let keys = keys(&fixture.group);
    let first = keys[4];
    let later = keys[127];
    fixture.group.inner.mark_ready(first);
    fixture.group.inner.mark_ready(later);
    fixture.group.inner.mark_ready(first);
    assert_candidate(&fixture.group, first);
    assert_candidate(&fixture.group, first);
    fixture.group.inner.mark_unready(first);
    assert_candidate(&fixture.group, later);
    fixture.group.inner.mark_unready(later);
    assert!(fixture.group.inner.ready_member().is_none());
    fixture.cancel_and_drain();
}

#[test]
fn provisional_reservation_survives_last_settlement_and_excess_pages_return() {
    let mut fixture = PendingMembers::new(65);
    let stale = keys(&fixture.group);
    let reserved = fixture.group.inner.reserve_members(64);
    let reserved_page = reserved.head.unwrap() / super::PAGE_SLOTS;
    let reserved_incarnation =
        fixture.group.inner.members.lock().unwrap().pages[reserved_page].incarnation;
    for (_, control) in &fixture.tasks {
        control.cancel();
    }
    {
        let members = fixture.group.inner.members.lock().unwrap();
        assert_eq!(members.reserved, 64);
        let pinned = members.pages[reserved_page]
            .page
            .as_ref()
            .expect("reservation pins its actual page");
        assert_eq!(pinned.in_use, 64);
        assert_eq!(
            members.pages[reserved_page].incarnation,
            reserved_incarnation
        );
        assert!(
            members.pages[1].page.is_none(),
            "unreserved empty page returns independently"
        );
    }
    drop(reserved);
    assert_eq!(fixture.group.inner.members.lock().unwrap().pages.len(), 1);

    let lane = fixture.runtime.lane(SWExecutionClass::High);
    let mut replacement = (0..65)
        .map(|value| {
            lane.try_spawn_after_in(
                &fixture.group,
                SWSpawnOptions {
                    eligibility: SWCallerEligibility::CallerEligible,
                },
                &[fixture._gate_task.completion()],
                SWDependencyPolicy::SuccessOnly,
                move || value,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(fixture.group.inner.members.lock().unwrap().pages.len() > 1);
    let live = keys(&fixture.group);
    let old_page = stale
        .iter()
        .find(|key| key.slot / super::PAGE_SLOTS == 1)
        .unwrap();
    let new_page = live
        .iter()
        .find(|key| key.slot / super::PAGE_SLOTS == 1)
        .unwrap();
    assert_ne!(old_page.incarnation, new_page.incarnation);
    // Replay actual pre-trim keys after the same open group regrows. They must
    // neither publish candidates nor unlink/remove the replacement members.
    for key in stale {
        fixture.group.inner.mark_ready(key);
        fixture.group.inner.mark_unready(key);
        fixture.group.inner.retire_member(key);
    }
    assert_eq!(keys(&fixture.group), live);
    assert!(fixture.group.inner.ready_member().is_none());
    fixture.group.seal();
    fixture.producer.complete(()).unwrap();
    fixture.group.wait_helping().unwrap();
    for (value, (task, _)) in replacement.iter_mut().enumerate() {
        assert_eq!(task.try_take(), Some(SWOutcome::Success(value)));
    }
    assert_eq!(fixture.group.inner.members.lock().unwrap().pages.len(), 1);
    fixture.runtime.shutdown().unwrap();
}

#[test]
fn empty_middle_page_returns_without_invalidating_later_members_or_reused_directory_keys() {
    let mut fixture = PendingMembers::new(129);
    let original = keys(&fixture.group);
    let stale = original
        .iter()
        .find(|key| key.slot / super::PAGE_SLOTS == 1)
        .copied()
        .unwrap();
    let later = original
        .iter()
        .find(|key| key.slot / super::PAGE_SLOTS == 2)
        .copied()
        .unwrap();
    for index in 64..128 {
        fixture.tasks[index].1.cancel();
    }
    let directory = {
        let members = fixture.group.inner.members.lock().unwrap();
        assert!(members.pages[1].page.is_none());
        assert!(members.matches(fixture.group.inner.id, later));
        assert_eq!(members.occupied, 65);
        members.pages.as_ptr()
    };
    let lane = fixture.runtime.lane(SWExecutionClass::High);
    let mut replacement = (0..64)
        .map(|value| {
            lane.try_spawn_after_in(
                &fixture.group,
                SWSpawnOptions {
                    eligibility: SWCallerEligibility::CallerEligible,
                },
                &[fixture._gate_task.completion()],
                SWDependencyPolicy::SuccessOnly,
                move || 1000 + value,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let live = keys(&fixture.group);
    let replaced = live
        .iter()
        .find(|key| key.slot / super::PAGE_SLOTS == 1)
        .unwrap();
    assert_ne!(replaced.incarnation, stale.incarnation);
    // The stale page identity must still fail with current association fields;
    // this isolates incarnation checks from the independent job-ID safeguard.
    {
        let members = fixture.group.inner.members.lock().unwrap();
        assert_eq!(
            members.pages.as_ptr(),
            directory,
            "reusing the vacant position needs no directory growth"
        );
        assert_eq!(members.pages.len(), 3);
        assert!(!members.matches(
            fixture.group.inner.id,
            super::MembershipKey {
                incarnation: stale.incarnation,
                ..*replaced
            }
        ));
        assert!(members.matches(fixture.group.inner.id, later));
    }
    fixture.group.inner.mark_ready(stale);
    fixture.group.inner.mark_unready(stale);
    fixture.group.inner.retire_member(stale);
    assert_eq!(keys(&fixture.group), live);
    assert!(fixture.group.inner.ready_member().is_none());
    fixture.group.seal();
    fixture.producer.complete(()).unwrap();
    fixture.group.wait_helping().unwrap();
    for (index, (task, _)) in fixture.tasks.iter_mut().enumerate() {
        assert_eq!(
            task.try_take(),
            Some(if (64..128).contains(&index) {
                SWOutcome::Cancelled
            } else {
                SWOutcome::Success(index)
            })
        );
    }
    for (index, (task, _)) in replacement.iter_mut().enumerate() {
        assert_eq!(task.try_take(), Some(SWOutcome::Success(1000 + index)));
    }
    assert_eq!(fixture.group.inner.members.lock().unwrap().pages.len(), 1);
    fixture.runtime.shutdown().unwrap();
}

#[test]
fn exhausted_slot_is_retired_and_reset_preserves_old_completion() {
    let mut fixture = PendingMembers::new(1);
    let old = fixture.group.completion();
    let original = keys(&fixture.group)[0];
    // Put the occupied slot at its final generation, updating its retirement
    // key as a real record would have obtained at checkout.
    let final_key = super::MembershipKey {
        generation: u64::MAX,
        ..original
    };
    {
        let mut members = fixture.group.inner.members.lock().unwrap();
        members.slot_mut(original.slot).generation = u64::MAX;
    }
    fixture.group.inner.retire_member(final_key);
    {
        let members = fixture.group.inner.members.lock().unwrap();
        let page = members.pages[original.slot / super::PAGE_SLOTS]
            .page
            .as_ref()
            .unwrap();
        assert_eq!(
            page.free & (1_u64 << (original.slot % super::PAGE_SLOTS)),
            0
        );
    }
    // Logical settlement can still arrive with its stale old key; it cannot
    // remove a replacement member or reintroduce the exhausted slot.
    fixture.group.seal();
    fixture.tasks[0].1.cancel();
    fixture.group.wait_helping().unwrap();
    assert_eq!(old.status(), Some(SWTaskStatus::PrerequisiteFailed));
    assert!(fixture.group.try_reset(100));
    assert_eq!(fixture.group.completion().status(), None);
    assert_eq!(old.status(), Some(SWTaskStatus::PrerequisiteFailed));
    assert!(fixture.group.inner.ready_member().is_none());
    fixture.group.seal();
    fixture.producer.complete(()).unwrap();
    fixture.runtime.shutdown().unwrap();
}
fn assert_candidate(group: &SWGroup, key: super::MembershipKey) {
    let expected = group
        .inner
        .members
        .lock()
        .unwrap()
        .slot(key.slot)
        .job
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    let candidate = group.inner.ready_member().unwrap();
    assert!(std::ptr::eq(&*candidate, &*expected));
    assert!(group.inner.members.try_lock().is_ok());
}
