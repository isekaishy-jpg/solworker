use solworker::{
    SWCallerEligibility, SWDependencyPolicy, SWExecutionClass, SWExternalOptions, SWOwnedLimits,
    SWPriority, SWRuntime, SWRuntimeConfig, SWSpawnOptions, SWStageOptions, SWTaskStatus,
    SWWorkerConfig,
};
use std::sync::mpsc;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn cancelling_deferred_ordinary_head_exposes_earlier_ordinary_work() {
    let mut runtime =
        SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
            .with_owned_limits(SWOwnedLimits::new(16, 16, [1; 3], [1; 3]).unwrap())
            .with_demand_limits(vec![SWPriority::new(5)], 4)
            .build()
            .unwrap();
    let lane = runtime.lane(SWExecutionClass::High);
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let release = Release(release_send);
    let _blocker = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            entered_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    entered_recv.recv_timeout(TIMEOUT).unwrap();
    let group = lane.group().unwrap();
    let mut inputs = Vec::new();
    let mut receipts = Vec::new();
    for index in 0..3 {
        let (producer, input, _) = runtime
            .external::<()>(SWExternalOptions::default())
            .unwrap();
        receipts.push(
            lane.try_spawn_stage(
                SWStageOptions {
                    spawn: SWSpawnOptions {
                        eligibility: SWCallerEligibility::CallerEligible,
                    },
                    prerequisites: &[input.completion()],
                    group: (index == 0).then_some(&group),
                    priority: (index == 1).then_some(SWPriority::new(5)),
                    ..Default::default()
                },
                move || index,
            )
            .unwrap(),
        );
        inputs.push(Some(producer));
    }
    group.seal();
    for index in [1, 2, 0] {
        inputs[index].take().unwrap().complete(()).unwrap();
    }
    assert!(!group.help_ready().unwrap());
    receipts[2].1.cancel();
    assert!(group.help_ready().unwrap());
    assert_eq!(receipts[0].0.status(), Some(SWTaskStatus::Succeeded));
    drop(release);
    runtime.shutdown().unwrap();
    assert_eq!(receipts[1].0.status(), Some(SWTaskStatus::Succeeded));
    assert_eq!(receipts[2].0.status(), Some(SWTaskStatus::Cancelled));
}

#[test]
fn out_of_order_resource_removal_repairs_the_helpable_window() {
    for cancel in [false, true] {
        let mut runtime =
            SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
                .with_owned_limits(SWOwnedLimits::new(16, 16, [3; 3], [1; 3]).unwrap())
                .with_demand_limits(vec![SWPriority::new(0), SWPriority::new(5)], 8)
                .build()
                .unwrap();
        let lane = runtime.lane(SWExecutionClass::High);
        let (entered_send, entered_recv) = mpsc::channel();
        let (release_send, release_recv) = mpsc::channel();
        let release = Release(release_send);
        let _blocker = lane
            .try_spawn(SWSpawnOptions::default(), move || {
                entered_send.send(()).unwrap();
                release_recv.recv_timeout(TIMEOUT).unwrap();
            })
            .unwrap();
        entered_recv.recv_timeout(TIMEOUT).unwrap();
        let second = lane.group().unwrap();
        let urgent = lane.group().unwrap();
        let mut receipts = Vec::new();
        let mut inputs = Vec::new();
        for index in 0..5 {
            let (producer, input, _) = runtime
                .external::<()>(SWExternalOptions::default())
                .unwrap();
            let (task, control) = lane
                .try_spawn_stage(
                    SWStageOptions {
                        spawn: SWSpawnOptions {
                            eligibility: SWCallerEligibility::CallerEligible,
                        },
                        prerequisites: &[input.completion()],
                        group: match index {
                            1 => Some(&second),
                            4 => Some(&urgent),
                            _ => None,
                        },
                        priority: match index {
                            0 | 1 => Some(SWPriority::new(5)),
                            4 => Some(SWPriority::new(0)),
                            _ => None,
                        },
                        ..Default::default()
                    },
                    move || index,
                )
                .unwrap();
            receipts.push((task, control));
            inputs.push(Some(producer));
        }
        second.seal();
        urgent.seal();
        // Resource work fills the first window. Ordinary arrivals must then
        // repair it, even though both initially encounter runnable saturation.
        for index in [4, 0, 1] {
            inputs[index].take().unwrap().complete(()).unwrap();
        }
        // All three released resource jobs, including the sole member of
        // `second`, are Ready while the physical worker remains blocked.
        assert_eq!(runtime.progress().scheduler.ready, 3);
        assert_eq!(runtime.progress().scheduler.deferred, 0);
        for index in [3, 2] {
            inputs[index].take().unwrap().complete(()).unwrap();
        }
        assert_eq!(runtime.progress().scheduler.deferred, 2);
        assert_eq!(receipts[1].0.status(), None);
        // Ordinary arrivals demote the previously Ready grouped resource.
        assert!(!second.help_ready().unwrap());
        if cancel {
            receipts[4].1.cancel();
        } else {
            assert!(urgent.help_ready().unwrap());
        }
        // Removing the high-ID urgent head exposes BOTH lower-ID background
        // resources ahead of ordinary work. Refilling just one vacancy would
        // leave this second resource incorrectly ineligible for helping.
        assert!(second.help_ready().unwrap());
        assert_eq!(receipts[1].0.status(), Some(SWTaskStatus::Succeeded));
        drop(release);
        runtime.shutdown().unwrap();
        for (index, (task, _)) in receipts.iter().enumerate() {
            assert_eq!(
                task.status(),
                Some(if cancel && index == 4 {
                    SWTaskStatus::Cancelled
                } else {
                    SWTaskStatus::Succeeded
                })
            );
        }
    }
}

struct Release(mpsc::Sender<()>);

impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn deferred_fifo_survives_mixed_ranks_refresh_and_cancellation() {
    for window in [1, 4] {
        let mut runtime =
            SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
                .with_owned_limits(SWOwnedLimits::new(64, 64, [window; 3], [1; 3]).unwrap())
                .with_demand_limits(vec![SWPriority::new(0), SWPriority::new(5)], 16)
                .build()
                .unwrap();
        let lane = runtime.lane(SWExecutionClass::High);
        let (entered_send, entered_recv) = mpsc::channel();
        let (release_send, release_recv) = mpsc::channel();
        let release = Release(release_send);
        let (blocker, _) = lane
            .try_spawn(SWSpawnOptions::default(), move || {
                entered_send.send(()).unwrap();
                release_recv.recv_timeout(TIMEOUT).unwrap();
            })
            .unwrap();
        entered_recv.recv_timeout(TIMEOUT).unwrap();
        let (order_send, order_recv) = mpsc::channel();
        let mut inputs = Vec::new();
        let mut completions = Vec::new();
        let mut controls = Vec::new();
        for index in 0..12 {
            let (producer, input, _) = runtime
                .external::<()>(SWExternalOptions::default())
                .unwrap();
            let sender = order_send.clone();
            let operation = move || sender.send(index).unwrap();
            let (completion, control) = if index % 2 == 0 {
                let (task, control) = lane
                    .try_spawn_after(
                        SWSpawnOptions::default(),
                        &[input.completion()],
                        SWDependencyPolicy::SuccessOnly,
                        operation,
                    )
                    .unwrap();
                (task.completion(), control)
            } else {
                let (task, control) = lane
                    .try_spawn_stage(
                        SWStageOptions {
                            prerequisites: &[input.completion()],
                            priority: Some(SWPriority::new(5)),
                            ..Default::default()
                        },
                        operation,
                    )
                    .unwrap();
                (task.completion(), control)
            };
            inputs.push(producer);
            completions.push(completion);
            controls.push(control);
        }
        // Activation FIFO deliberately disagrees with admission identity.
        for input in inputs.into_iter().rev() {
            input.complete(()).unwrap();
        }
        assert_eq!(runtime.progress().scheduler.deferred, 12 - window);
        let demand = completions[9].demand(SWPriority::new(0)).unwrap();
        while runtime.service_demand(32) {}
        controls[7].cancel();
        assert_eq!(completions[7].status(), Some(SWTaskStatus::Cancelled));

        // Independent expected ordering: ordinary activation FIFO; resource
        // demand rank then admission tie; only their heads interleave by ID.
        let mut ordinary = vec![10, 8, 6, 4, 2, 0];
        let mut resource = vec![9, 1, 3, 5, 11];
        let mut expected = Vec::new();
        while !ordinary.is_empty() || !resource.is_empty() {
            if resource.is_empty() || (!ordinary.is_empty() && ordinary[0] < resource[0]) {
                expected.push(ordinary.remove(0));
            } else {
                expected.push(resource.remove(0));
            }
        }
        drop(release);
        let observed = (0..11)
            .map(|_| order_recv.recv_timeout(TIMEOUT).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(observed, expected, "window {window}");
        assert_eq!(
            blocker.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Succeeded)
        );
        runtime.shutdown().unwrap();
        for (index, completion) in completions.iter().enumerate() {
            assert_eq!(
                completion.status(),
                Some(if index == 7 {
                    SWTaskStatus::Cancelled
                } else {
                    SWTaskStatus::Succeeded
                })
            );
        }
        drop(demand);
    }
}
