use super::*;
use crate::external::SWExternalOptions;
use crate::notification::SWNotifyLimits;
use crate::runtime::{
    SWRuntime,
    config::{SWRuntimeConfig, SWWorkerConfig},
};
use crate::scheduler::SWOwnedLimits;
use crate::scheduler::reservation::SWLimits;
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize},
};
use std::thread;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

struct Release(mpsc::Sender<()>);

impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn cancelled_attachment_reservations_restore_unrelated_deferred_progress() {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(SWOwnedLimits::new(8, 8, [2; 3], [2; 3]).unwrap())
        .build()
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::High);
    let seed = lane.group().unwrap();
    let scheduler = seed.scheduler.upgrade().unwrap();
    seed.seal();
    drop(seed);
    let (provider, predecessor, _) = runtime
        .external::<()>(SWExternalOptions::default())
        .unwrap();
    let (unrelated, _) = lane
        .try_spawn_after(
            Default::default(),
            &[predecessor.completion()],
            SWDependencyPolicy::SuccessOnly,
            || 19usize,
        )
        .unwrap();
    let (attached_send, attached_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let release = Release(release_send);
    let first = AtomicBool::new(true);
    let release_recv = Mutex::new(release_recv);
    *scheduler.attachment_hook.lock().unwrap() = Some(Arc::new(move |_| {
        if first.swap(false, Ordering::SeqCst) {
            attached_send.send(()).unwrap();
            release_recv.lock().unwrap().recv_timeout(TIMEOUT).unwrap();
        }
    }));
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&calls);
    let submitter = thread::spawn(move || {
        lane.try_spawn_batch(
            Default::default(),
            (0..2)
                .map(|_| {
                    let captured = Arc::clone(&captured);
                    move || {
                        captured.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .collect(),
        )
        .unwrap()
    });
    attached_recv.recv_timeout(TIMEOUT).unwrap();
    provider.complete(()).unwrap();
    let batch = {
        let state = scheduler.class_lock(SWExecutionClass::High);
        assert_eq!(state.attaching_runnable, 2);
        assert_eq!(state.ready.runnable, 0);
        assert_eq!(state.deferred.len(), 1);
        state
            .records
            .values()
            .filter(|job| job.record_lock().as_ref().unwrap().attaching)
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(batch.len(), 2);
    assert!(runtime.progress().scheduler.runnable_full[SWExecutionClass::High.index()]);
    for job in &batch {
        scheduler.suppress(job, SWTaskStatus::Cancelled);
    }
    assert_eq!(
        scheduler
            .class_lock(SWExecutionClass::High)
            .attaching_runnable,
        2
    );
    drop(release);
    let members = submitter.join().unwrap();
    *scheduler.attachment_hook.lock().unwrap() = None;
    for (task, _) in members {
        assert_eq!(
            task.completion().wait_timeout(TIMEOUT).unwrap(),
            Some(SWTaskStatus::Cancelled)
        );
    }
    assert_eq!(
        unrelated.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        scheduler
            .class_lock(SWExecutionClass::High)
            .attaching_runnable,
        0
    );
    drop(batch);
    runtime.shutdown().unwrap();
}

#[test]
fn saturated_notification_callback_rejects_batch_before_touching_capacity() {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(SWOwnedLimits::new(1, 1, [1; 3], [1; 3]).unwrap())
        .with_capacity_limits(
            SWLimits::new(SWCost::new(1, 0, 0, 0), SWCost::new(1, 0, 0, 0), 1, None).unwrap(),
        )
        .with_notification_limits(SWNotifyLimits {
            routes: 1,
            bindings: 1,
        })
        .build()
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::High);
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let release = Release(release_send);
    let (blocker, _) = lane
        .try_spawn(Default::default(), move || {
            entered_send.send(()).unwrap();
            release_recv.recv_timeout(TIMEOUT).unwrap();
        })
        .unwrap();
    entered_recv.recv_timeout(TIMEOUT).unwrap();
    let (reason_send, reason_recv) = mpsc::channel();
    let mut route = runtime
        .notification_route(move || {
            let rejected = lane
                .try_spawn_batch(Default::default(), vec![|| ()])
                .err()
                .unwrap();
            reason_send
                .send((
                    rejected.reason,
                    rejected.accepted.len(),
                    rejected.remaining.len(),
                ))
                .unwrap();
            Ok(())
        })
        .unwrap();
    let binding = route.watch_progress().unwrap();
    route.prepare_wait().unwrap();
    // This real runtime transition publishes while the ordinary and global
    // owned ledgers are both full, without freeing the blocked operation.
    let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    assert_eq!(
        reason_recv.recv_timeout(TIMEOUT).unwrap(),
        (SWSpawnError::InvalidContext, 0, 1)
    );
    route.close().unwrap();
    drop(binding);
    set.seal();
    drop(set);
    drop(release);
    assert_eq!(
        blocker.completion().wait_timeout(TIMEOUT).unwrap(),
        Some(SWTaskStatus::Succeeded)
    );
    runtime.shutdown().unwrap();
}

#[test]
fn provisional_rollback_refunds_accounting_before_the_final_runtime_wake() {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let mut runtime = SWRuntime::builder(config)
        .with_owned_limits(SWOwnedLimits::new(1, 1, [1; 3], [1; 3]).unwrap())
        .with_notification_limits(SWNotifyLimits {
            routes: 1,
            bindings: 1,
        })
        .build()
        .unwrap();
    let lane = runtime.lane(SWExecutionClass::High);
    let seed = lane.group().unwrap();
    let scheduler = seed.scheduler.upgrade().unwrap();
    seed.seal();
    drop(seed);
    let control = scheduler.control.upgrade().unwrap();
    // These are actual independent runtime/global credits, staged exactly as
    // a selected member before class commitment. Capacity is disabled so its
    // refund cannot provide an extra wake that hides a missing final wake.
    let admission = control.admit_owned_many(false, 1).unwrap().pop().unwrap();
    let charge = scheduler
        .accounting
        .acquire_many(0, 1)
        .unwrap()
        .pop()
        .unwrap();
    let job = scheduler
        .jobs
        .prepare_many(
            &[OwnedScheduler::allocate_id(&scheduler.next_id)],
            Arc::downgrade(&scheduler),
        )
        .pop()
        .unwrap();
    let signal = scheduler.signals.acquire_many(1).pop().unwrap();
    let (task, sink) = SWTask::pending_with_signal(signal);
    let prepared = Prepared {
        job,
        envelope: Box::new(PreparedEnvelope {
            payload: 23usize,
            run: |value| value,
            application_failed: |_| false,
            sink,
        }),
        record: Record {
            completion: None,
            group: None,
            pending: 0,
            failed: false,
            policy: SWDependencyPolicy::SuccessOnly,
            stage: Stage::Waiting,
            eligibility: crate::scheduler::SWCallerEligibility::WorkerOnly,
            subscriptions: Vec::new(),
            attaching: true,
            deferred_finish: None,
            admission,
            work_set: None,
            capacity: None,
            resource: false,
            runnable_reserved: true,
            selection: None,
            charge,
        },
        receipt: (task, SWProducerControl::new(Box::new(|| {}))),
    };
    assert_eq!(control.active_leases(), 1);
    assert_eq!(scheduler.accounting.records.load(Ordering::Acquire), 1);
    let (wake_send, wake_recv) = mpsc::channel();
    let observed_control = Arc::clone(&control);
    let observed_scheduler = Arc::clone(&scheduler);
    let mut route = runtime
        .notification_route(move || {
            wake_send
                .send((
                    observed_control.active_leases(),
                    observed_scheduler
                        .accounting
                        .records
                        .load(Ordering::Acquire),
                ))
                .unwrap();
            Ok(())
        })
        .unwrap();
    let binding = route.watch_progress().unwrap();
    runtime.begin_shutdown();
    assert!(!runtime.try_shutdown().unwrap());
    while wake_recv.try_recv().is_ok() {}
    route.prepare_wait().unwrap();
    assert_eq!(prepared.recover(), 23);
    assert_eq!(wake_recv.recv_timeout(TIMEOUT).unwrap(), (0, 0));
    route.close().unwrap();
    drop(binding);
    assert!(runtime.try_shutdown().unwrap());
}
