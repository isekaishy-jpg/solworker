use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use solworker::{
    SWDeliveryStatus, SWNotifyLimits, SWOwnedLimits, SWOwner, SWOwnerError, SWPhase, SWPumpBudget,
    SWRuntime, SWRuntimeConfig, SWTask, SWWorkerConfig,
};

fn runtime() -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    let owned = SWOwnedLimits::new(8, 8, [8; 3], [1; 3]).unwrap();
    SWRuntime::builder(config)
        .with_owned_limits(owned)
        .with_notification_limits(SWNotifyLimits {
            routes: 2,
            bindings: 4,
        })
        .build()
        .unwrap()
}

#[test]
fn owner_binding_tracks_ready_suppressed_settled_and_close() {
    let mut runtime = runtime();
    let phase = SWPhase(1);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(2).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let signals = Arc::new(AtomicUsize::new(0));
    let signal_count = Arc::clone(&signals);
    let mut route = runtime
        .notification_route(move || {
            signal_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let _binding = route.watch_owner(&owner).unwrap();
    assert_eq!(signals.load(Ordering::SeqCst), 1);
    let stamp = route.prepare_wait().unwrap();

    let ready = owner.prepare_delivery(phase, |state| *state += 1).unwrap();
    assert!(!route.changed_since(stamp).unwrap());
    ready.ticket.ready();
    assert!(route.changed_since(stamp).unwrap());
    assert_eq!(ready.delivery.status(), SWDeliveryStatus::Ready);

    let stamp = route.prepare_wait().unwrap();
    assert_eq!(owner.pump(phase, SWPumpBudget::new(1)).unwrap().invoked, 1);
    assert_eq!(ready.delivery.status(), SWDeliveryStatus::Published);
    assert!(route.changed_since(stamp).unwrap());
    assert_eq!(*owner.state(), 1);

    let stamp = route.prepare_wait().unwrap();
    let suppressed = owner.prepare_delivery(phase, |_| {}).unwrap();
    drop(suppressed.ticket);
    assert!(route.changed_since(stamp).unwrap());
    assert_eq!(suppressed.delivery.status(), SWDeliveryStatus::Waiting);
    let stamp = route.prepare_wait().unwrap();
    assert_eq!(
        owner.pump(phase, SWPumpBudget::new(1)).unwrap().suppressed,
        1
    );
    assert_eq!(suppressed.delivery.status(), SWDeliveryStatus::Suppressed);
    assert!(route.changed_since(stamp).unwrap());

    let stamp = route.prepare_wait().unwrap();
    owner.control().request_close();
    assert!(route.changed_since(stamp).unwrap());
    let stamp = route.prepare_wait().unwrap();
    owner.close();
    assert!(route.changed_since(stamp).unwrap());
    route.close().unwrap();
    runtime.shutdown().unwrap();
}

#[test]
fn sender_publication_runs_after_inbox_unlock() {
    let mut runtime = runtime();
    let phase = SWPhase(2);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(2).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let probe = Arc::new(AtomicBool::new(false));
    let signal_probe = Arc::clone(&probe);
    let signal_release = Arc::clone(&release_rx);
    let mut route = runtime
        .notification_route(move || {
            if signal_probe.swap(false, Ordering::AcqRel) {
                entered_tx.send(()).unwrap();
                signal_release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
            Ok(())
        })
        .unwrap();
    let _binding = route.watch_owner(&owner).unwrap();
    route.prepare_wait().unwrap();
    probe.store(true, Ordering::Release);
    let first_sender = owner.sender();
    let (first_tx, first_rx) = mpsc::channel();
    std::thread::spawn(move || {
        first_tx
            .send(first_sender.try_post(phase, |_| {}).unwrap().0)
            .unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let second_sender = owner.sender();
    let (second_tx, second_rx) = mpsc::channel();
    std::thread::spawn(move || {
        second_tx
            .send(second_sender.try_post(phase, |_| {}).unwrap().0)
            .unwrap();
    });
    let second = second_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    release_tx.send(()).unwrap();
    let first = first_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(first.status(), SWDeliveryStatus::Ready);
    assert_eq!(second.status(), SWDeliveryStatus::Ready);
    assert_eq!(owner.pump(phase, SWPumpBudget::new(2)).unwrap().invoked, 2);
    route.close().unwrap();
    owner.close();
    runtime.shutdown().unwrap();
}

#[test]
fn notification_callback_rejects_root_and_accounted_owner_admission() {
    thread_local! {
        static OWNER: RefCell<Option<SWOwner<usize>>> = const { RefCell::new(None) };
    }
    let mut runtime = runtime();
    let phase = SWPhase(3);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(2).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let sender = owner.sender();
    OWNER.with(|slot| *slot.borrow_mut() = Some(owner));
    let set = runtime.work_set(NonZeroUsize::new(1).unwrap()).unwrap();
    let consumer = set.clone();
    let (tx, rx) = mpsc::channel();
    let mut route = runtime
        .notification_route(move || {
            let sender_error = sender.try_post(phase, |_| {}).err().map(|e| e.reason);
            let (root_error, accounted_error) = OWNER.with(|slot| {
                let mut slot = slot.borrow_mut();
                let owner = slot.as_mut().unwrap();
                let root = owner
                    .prepare_delivery(phase, |_| {})
                    .err()
                    .map(|e| e.reason);
                let accounted = owner
                    .prepare_delivery_in(&consumer, phase, |_| {})
                    .err()
                    .map(|e| e.reason);
                (root, accounted)
            });
            tx.send([sender_error, root_error, accounted_error])
                .unwrap();
            Ok(())
        })
        .unwrap();
    let _binding = route
        .watch_completion(&SWTask::ready(()).completion())
        .unwrap();
    let errors = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    route.close().unwrap();
    OWNER.with(|slot| drop(slot.borrow_mut().take()));
    set.seal();
    runtime.shutdown().unwrap();
    assert_eq!(errors, [Some(SWOwnerError::InvalidContext); 3]);
}
