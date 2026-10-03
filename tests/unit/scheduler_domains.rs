use super::*;
use crate::runtime::SWRuntime;
use crate::runtime::config::{SWRuntimeConfig, SWWorkerConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);

fn runtime() -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    SWRuntime::builder(config)
        .with_owned_limits(SWOwnedLimits::new(16, 16, [8; 3], [4; 3]).unwrap())
        .build()
        .unwrap()
}

struct CountDrop(Arc<AtomicUsize>);

struct Release(mpsc::Sender<()>);

impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn held_low_class_guard_does_not_block_high_claim_or_retirement() {
    let mut runtime = runtime();
    let high = runtime.lane(SWExecutionClass::High);
    let seed = high.group().unwrap();
    let scheduler = seed.scheduler.upgrade().unwrap();
    seed.seal();
    drop(seed);
    let low_guard = scheduler.class_lock(SWExecutionClass::Low);
    let retiring = Arc::clone(&scheduler);
    let (done_send, done_recv) = mpsc::channel();
    let executor = thread::spawn(move || {
        let group = high.group().unwrap();
        let (task, _) = high
            .try_spawn_in(&group, SWSpawnOptions::default(), || 41usize)
            .unwrap();
        group.seal();
        assert_eq!(
            task.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Succeeded)
        );
        assert_eq!(
            group.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Succeeded)
        );
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let class = retiring.class_lock(SWExecutionClass::High);
            let retired = class.records.is_empty() && class.ready.handed_off == 0;
            drop(class);
            if retired {
                break;
            }
            assert!(Instant::now() < deadline, "High wrapper did not retire");
            thread::yield_now();
        }
        done_send.send(()).unwrap();
    });
    // Always release the actual Low guard before joining or reporting failure.
    let isolated = done_recv.recv_timeout(TIMEOUT);
    drop(low_guard);
    executor.join().unwrap();
    assert!(isolated.is_ok(), "High work waited for the held Low guard");
    runtime.shutdown().unwrap();
}

#[test]
fn pending_low_demand_does_not_block_ordinary_high_claim_or_retirement() {
    for hold_record in [false, true] {
        let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
        let mut runtime = SWRuntime::builder(config)
            .with_owned_limits(SWOwnedLimits::new(16, 16, [8; 3], [1; 3]).unwrap())
            .with_demand_limits(vec![SWPriority::new(0), SWPriority::new(5)], 2)
            .build()
            .unwrap();
        let low = runtime.lane(SWExecutionClass::Low);
        let high = runtime.lane(SWExecutionClass::High);
        let seed = high.group().unwrap();
        let scheduler = seed.scheduler.upgrade().unwrap();
        seed.seal();
        drop(seed);
        let (entered_send, entered_recv) = mpsc::channel();
        let (release_send, release_recv) = mpsc::channel();
        let release = Release(release_send);
        let (blocker, _) = low
            .try_spawn(SWSpawnOptions::default(), move || {
                entered_send.send(()).unwrap();
                release_recv.recv_timeout(TIMEOUT).unwrap();
            })
            .unwrap();
        entered_recv.recv_timeout(TIMEOUT).unwrap();
        let (resource, _) = low
            .try_spawn_stage(
                crate::execution::SWStageOptions {
                    priority: Some(SWPriority::new(5)),
                    ..Default::default()
                },
                || (),
            )
            .unwrap();
        let demand = resource.completion().demand(SWPriority::new(5)).unwrap();
        assert!(!scheduler.demand_updates_pending[0].load(Ordering::Acquire));
        let resource_id = resource.completion().producer_identity().unwrap().1;
        let job = scheduler
            .class_lock(SWExecutionClass::Low)
            .records
            .get(&resource_id)
            .unwrap()
            .clone();
        let low_guard = (!hold_record).then(|| scheduler.class_lock(SWExecutionClass::Low));
        let record_guard = hold_record.then(|| job.record_lock());
        let reprioritize = thread::spawn(move || {
            demand.refresh(SWPriority::new(0)).unwrap();
            demand
        });
        // The actual update producer must publish its Low mailbox before High is
        // tested. Holding Low prevents consumption, so this is a durable predicate.
        let deadline = Instant::now() + TIMEOUT;
        let update_published = loop {
            let staged = if hold_record {
                // The producer has drained the actual mailbox and holds Low while
                // waiting for this record. Its mailbox must already be released.
                !scheduler.demand_updates_pending[0].load(Ordering::Acquire)
                    && scheduler.classes[0].try_lock().is_err()
            } else {
                scheduler.demand_updates_pending[0].load(Ordering::Acquire)
            };
            if staged {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            thread::yield_now();
        };
        let retiring = Arc::clone(&scheduler);
        let (done_send, done_recv) = mpsc::channel();
        let executor = thread::spawn(move || {
            let (task, _) = high.try_spawn(SWSpawnOptions::default(), || ()).unwrap();
            assert_eq!(
                task.completion().wait_timeout(TIMEOUT).unwrap(),
                Some(SWTaskStatus::Succeeded)
            );
            let deadline = Instant::now() + TIMEOUT;
            loop {
                let class = retiring.class_lock(SWExecutionClass::High);
                let retired = class.records.is_empty() && class.ready.handed_off == 0;
                drop(class);
                if retired {
                    break;
                }
                assert!(Instant::now() < deadline, "High wrapper did not retire");
                thread::yield_now();
            }
            done_send.send(()).unwrap();
        });
        let isolated = done_recv.recv_timeout(TIMEOUT);
        drop(low_guard);
        drop(record_guard);
        drop(release);
        executor.join().unwrap();
        let demand = reprioritize.join().unwrap();
        assert!(update_published, "Low demand update was not published");
        assert!(
            isolated.is_ok(),
            "ordinary High waited for the Low demand update"
        );
        for completion in [blocker.completion(), resource.completion()] {
            assert_eq!(
                completion.wait_timeout(TIMEOUT).unwrap(),
                Some(SWTaskStatus::Succeeded)
            );
        }
        drop(demand);
        runtime.shutdown().unwrap();
    }
}

#[test]
fn public_demand_service_preserves_zero_and_one_update_budgets() {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(SWOwnedLimits::new(4, 0, [1; 3], [1; 3]).unwrap())
        .with_demand_limits(vec![SWPriority::new(0), SWPriority::new(5)], 2)
        .build()
        .unwrap();
    let group = runtime.lane(SWExecutionClass::High).group().unwrap();
    let scheduler = group.scheduler.upgrade().unwrap();
    group.seal();
    let changed = Arc::new(AtomicUsize::new(0));
    let mut producers = Vec::new();
    let mut leases = Vec::new();
    for _ in 0..2 {
        let observed = Arc::clone(&changed);
        let (producer, task, _) = runtime
            .external::<()>(SWExternalOptions {
                priority: Some(SWPriority::new(5)),
                provider_demand: Some(Arc::new(move |snapshot| {
                    if snapshot.priority == Some(SWPriority::new(0)) {
                        observed.fetch_add(1, Ordering::SeqCst);
                    }
                })),
                ..Default::default()
            })
            .unwrap();
        producers.push((producer, task));
    }
    // Stage two real consumer changes on accepted external records. No worker
    // can consume these updates; only the public wrapper under test services
    // them. Direct staging isolates its explicit budget from automatic service.
    {
        let mut demand = scheduler.demand.lock().unwrap();
        for (_, task) in &producers {
            let id = task.completion().producer_identity().unwrap().1;
            leases.push(demand.graph.attach(id, SWPriority::new(0)).unwrap());
        }
        scheduler.demand_pending.store(true, Ordering::Release);
    }
    assert!(runtime.service_demand(0));
    assert_eq!(changed.load(Ordering::SeqCst), 0);
    assert!(runtime.service_demand(1));
    assert_eq!(changed.load(Ordering::SeqCst), 1);
    assert!(!runtime.service_demand(1));
    assert_eq!(changed.load(Ordering::SeqCst), 2);
    {
        let mut demand = scheduler.demand.lock().unwrap();
        for lease in leases {
            demand
                .graph
                .change(lease, demand::DemandCommand::Detach)
                .unwrap();
        }
        scheduler.demand_pending.store(true, Ordering::Release);
    }
    assert!(!runtime.service_demand(2));
    for (producer, task) in producers {
        assert_eq!(task.status(), None);
        producer.complete(()).unwrap();
    }
    runtime.shutdown().unwrap();
}

#[derive(Clone, Copy, Debug)]
enum AttachmentExit {
    Success,
    Failure,
    Cancel,
    Abandon,
}

#[test]
fn completion_and_suppression_during_attachment_settle_one_accepted_record() {
    for exit in [
        AttachmentExit::Success,
        AttachmentExit::Failure,
        AttachmentExit::Cancel,
        AttachmentExit::Abandon,
    ] {
        let mut runtime = runtime();
        let high = runtime.lane(SWExecutionClass::High);
        let seed = high.group().unwrap();
        let scheduler = seed.scheduler.upgrade().unwrap();
        seed.seal();
        drop(seed);
        let (producer, predecessor, _) = runtime
            .external::<()>(SWExternalOptions::default())
            .unwrap();
        let prerequisite = predecessor.completion();
        let group = high.group().unwrap();
        let group_submission = group.clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let capture = CountDrop(Arc::clone(&drops));
        let invoked = Arc::clone(&calls);
        let (attached_send, attached_recv) = mpsc::channel();
        let (release_send, release_recv) = mpsc::channel();
        let release = Release(release_send);
        let release_recv = Mutex::new(release_recv);
        *scheduler.attachment_hook.lock().unwrap() = Some(Arc::new(move |job| {
            attached_send.send(job).unwrap();
            release_recv.lock().unwrap().recv_timeout(TIMEOUT).unwrap();
        }));
        let submitter = thread::spawn(move || {
            high.try_spawn_after_in(
                &group_submission,
                SWSpawnOptions::default(),
                &[prerequisite],
                SWDependencyPolicy::SuccessOnly,
                move || {
                    let _capture = capture;
                    invoked.fetch_add(1, Ordering::SeqCst);
                },
            )
        });
        let attaching = attached_recv.recv_timeout(TIMEOUT);
        let attaching_completion = attaching.as_ref().ok().and_then(|job| {
            job.record_lock()
                .as_ref()
                .and_then(|record| record.completion.clone())
        });
        group.seal();
        let mut expected = SWTaskStatus::Succeeded;
        if let Ok(job) = attaching.as_ref() {
            match exit {
                AttachmentExit::Success => producer.complete(()).unwrap(),
                AttachmentExit::Failure => {
                    producer.cancel();
                    expected = SWTaskStatus::PrerequisiteFailed;
                }
                AttachmentExit::Cancel => {
                    scheduler.suppress(job, SWTaskStatus::Cancelled);
                    expected = SWTaskStatus::Cancelled;
                }
                AttachmentExit::Abandon => {
                    runtime.abandon();
                    expected = SWTaskStatus::Abandoned;
                }
            }
        }
        let drops_while_attaching = drops.load(Ordering::SeqCst);
        let group_while_attaching = group.completion().status();
        let task_while_attaching = attaching_completion.as_ref().map(SWCompletion::status);
        let retained_while_attaching = runtime.progress().active_leases;
        // Release and join even when the acceptance hook was never reached.
        drop(release);
        let submitted = submitter.join().unwrap();
        *scheduler.attachment_hook.lock().unwrap() = None;
        assert!(
            attaching.is_ok(),
            "attachment hook was not reached: {exit:?}"
        );
        assert!(drops_while_attaching <= 1, "{exit:?}");
        assert_eq!(group_while_attaching, None, "{exit:?}");
        assert_eq!(task_while_attaching, Some(None), "{exit:?}");
        assert!(retained_while_attaching > 0, "{exit:?}");
        drop(attaching);
        let (task, _) = submitted.unwrap();
        assert_eq!(
            task.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(expected),
            "{exit:?}"
        );
        assert_eq!(
            group.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(if matches!(exit, AttachmentExit::Success) {
                SWTaskStatus::Succeeded
            } else {
                SWTaskStatus::PrerequisiteFailed
            }),
            "{exit:?}"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1, "{exit:?}");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            usize::from(matches!(exit, AttachmentExit::Success)),
            "{exit:?}"
        );
        if matches!(exit, AttachmentExit::Cancel) {
            producer.complete(()).unwrap();
        }
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let progress = runtime.progress();
            if progress.active_leases == 0 && progress.scheduler.handoff_wrappers == [0; 3] {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "attachment did not retire: {exit:?}: {progress:?}"
            );
            progress.wait_for_change(deadline).unwrap();
        }
        if !matches!(exit, AttachmentExit::Abandon) {
            runtime.shutdown().unwrap();
        }
    }
}
