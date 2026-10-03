use solworker::{
    SWBatchSpawnOptions, SWCallerEligibility, SWExecutionClass, SWExternalOptions, SWOutcome,
    SWOwnedLimits, SWRuntime, SWRuntimeConfig, SWSpawnOptions, SWTaskStatus, SWWorkerConfig,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

struct Release(mpsc::Sender<()>);

impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn simultaneous_helpers_and_cancellation_claim_once_while_worker_only_stays_queued() {
    let mut runtime =
        SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
            .with_owned_limits(SWOwnedLimits::new(128, 128, [128; 3], [2; 3]).unwrap())
            .build()
            .unwrap();
    let lane = runtime.lane(SWExecutionClass::High);
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let release = Release(release_send);
    let (_, _) = lane
        .try_spawn(SWSpawnOptions::default(), move || {
            entered_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    entered_recv.recv_timeout(TIMEOUT).unwrap();
    let group = lane.group().unwrap();
    // This member occupies the second physical wrapper. It must stay Handed
    // and excluded from the caller index while later Ready members run.
    let (worker_only, _) = lane
        .try_spawn_in(&group, SWSpawnOptions::default(), || 1000)
        .unwrap();
    let counts = Arc::new((0..64).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let mut receipts = Vec::new();
    for id in 0..64 {
        let invoked = Arc::clone(&counts);
        receipts.push(
            lane.try_spawn_in(
                &group,
                SWSpawnOptions {
                    eligibility: SWCallerEligibility::CallerEligible,
                },
                move || {
                    invoked[id].fetch_add(1, Ordering::SeqCst);
                    id
                },
            )
            .unwrap(),
        );
    }
    receipts[63].1.cancel();
    group.seal();
    let start = Arc::new(Barrier::new(6));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let group = group.clone();
            let start = Arc::clone(&start);
            scope.spawn(move || {
                start.wait();
                while group.help_ready().unwrap() {}
            });
        }
        let start_cancel = Arc::clone(&start);
        let receipts_ref = &receipts;
        scope.spawn(move || {
            start_cancel.wait();
            for id in (1..64).step_by(2) {
                receipts_ref[id].1.cancel();
            }
        });
        start.wait();
    });
    assert_eq!(worker_only.status(), None);
    assert!(!group.help_ready().unwrap());
    assert!(!group.is_complete());
    assert_eq!(runtime.progress().scheduler.handoff_wrappers[2], 2);
    for (id, (task, _)) in receipts.iter_mut().enumerate() {
        match task.try_take().expect("each eligible member settled") {
            SWOutcome::Success(value) => {
                assert_eq!(value, id);
                assert_eq!(counts[id].load(Ordering::SeqCst), 1);
            }
            SWOutcome::Cancelled => {
                assert_eq!(id % 2, 1);
                assert_eq!(counts[id].load(Ordering::SeqCst), 0);
            }
            outcome => panic!("unexpected outcome: {outcome:?}"),
        }
    }
    drop(release);
    assert_eq!(
        group.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::PrerequisiteFailed)
    );
    runtime.shutdown().unwrap();
    assert_eq!(worker_only.status(), Some(SWTaskStatus::Succeeded));
    assert_eq!(runtime.progress().scheduler.handoff_wrappers[2], 0);
}

#[test]
fn grouped_bulk_ready_publication_and_shared_release_cross_the_portion_boundary() {
    for count in [64, 65] {
        for gated in [false, true] {
            let mut runtime =
                SWRuntime::builder(SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap())
                    .with_owned_limits(SWOwnedLimits::new(128, 128, [128; 3], [2; 3]).unwrap())
                    .build()
                    .unwrap();
            let lane = runtime.lane(SWExecutionClass::High);
            let (entered_send, entered_recv) = mpsc::channel();
            let (release_send, release_recv) = mpsc::channel();
            let release = Release(release_send);
            let (_, _) = lane
                .try_spawn(SWSpawnOptions::default(), move || {
                    entered_send.send(()).unwrap();
                    release_recv.recv_timeout(TIMEOUT).unwrap();
                })
                .unwrap();
            entered_recv.recv_timeout(TIMEOUT).unwrap();
            let (provider, input, _) = runtime
                .external::<()>(SWExternalOptions::default())
                .unwrap();
            let prerequisites = [input.completion()];
            let group = lane.group().unwrap();
            let calls = Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
            let mut members = lane
                .try_spawn_batch(
                    SWBatchSpawnOptions {
                        group: Some(&group),
                        spawn: SWSpawnOptions {
                            eligibility: SWCallerEligibility::CallerEligible,
                        },
                        prerequisites: if gated { &prerequisites } else { &[] },
                        ..Default::default()
                    },
                    (0..count)
                        .map(|id| {
                            let calls = Arc::clone(&calls);
                            move || {
                                calls[id].fetch_add(1, Ordering::SeqCst);
                                id
                            }
                        })
                        .collect(),
                )
                .unwrap();
            members[63].1.cancel();
            group.seal();
            if gated {
                assert!(!group.help_ready().unwrap());
                assert!(
                    members[..63]
                        .iter()
                        .all(|(task, _)| task.status().is_none())
                );
            }
            provider.complete(()).unwrap();
            let mut helped = 0;
            while group.help_ready().unwrap() {
                helped += 1;
            }
            assert_eq!(helped, count - 1, "count={count}, gated={gated}");
            assert!(group.is_complete());
            for (id, (task, _)) in members.iter_mut().enumerate() {
                if id == 63 {
                    assert_eq!(task.try_take(), Some(SWOutcome::Cancelled));
                    assert_eq!(calls[id].load(Ordering::SeqCst), 0);
                } else {
                    assert_eq!(task.try_take(), Some(SWOutcome::Success(id)));
                    assert_eq!(calls[id].load(Ordering::SeqCst), 1);
                }
            }
            // Caller settlement leaves the accepted physical wrappers queued.
            assert_eq!(runtime.progress().scheduler.handoff_wrappers[2], 2);
            drop(release);
            runtime.shutdown().unwrap();
            assert_eq!(runtime.progress().scheduler.handoff_wrappers[2], 0);
        }
    }
}
