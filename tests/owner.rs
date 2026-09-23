use std::cell::Cell;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use solworker::{
    SWCancelResult, SWDeliveryStatus, SWOwnerError, SWPhase, SWPumpBudget, SWPumpMode,
    SWReadyAccess, SWRuntime, SWRuntimeConfig, SWShared, SWTaskStatus, SWWorkerConfig,
};

fn new_runtime() -> SWRuntime {
    let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
    SWRuntime::builder(config).build().unwrap()
}

struct CloseOnDrop(solworker::SWOwnerControl);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.request_close();
    }
}

#[test]
fn local_callbacks_defer_warm_hits_and_clean_non_send_captures() {
    let mut runtime = new_runtime();
    let phase = SWPhase(7);
    let other = SWPhase(8);
    let calls = Rc::new(Cell::new(0));
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(2).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let ready = SWShared::ready(3usize);
    let immediate = owner.with_ready(&ready, phase, |state, outcome| {
        *state += 1;
        assert!(matches!(&*outcome, solworker::SWOutcome::Success(3)));
        *state
    });
    assert!(matches!(immediate, SWReadyAccess::Ready(1)));

    let calls_in_callback = Rc::clone(&calls);
    let (delivery, _) = owner
        .on_ready(&ready.completion(), phase, move |state, status| {
            assert_eq!(status, SWTaskStatus::Succeeded);
            calls_in_callback.set(calls_in_callback.get() + 1);
            *state += 2;
        })
        .unwrap();
    assert_eq!(calls.get(), 0);
    assert_eq!(delivery.status(), SWDeliveryStatus::Ready);
    assert_eq!(
        owner.pump(other, SWPumpBudget::new(2)),
        Err(SWOwnerError::WrongPhase)
    );
    let report = owner.pump(phase, SWPumpBudget::new(2)).unwrap();
    assert_eq!(report.invoked, 1);
    assert_eq!(calls.get(), 1);
    assert_eq!(*owner.state(), 3);
    assert_eq!(delivery.status(), SWDeliveryStatus::Published);
    owner.close();
    runtime.shutdown().unwrap();
}

#[test]
fn reservation_pressure_cancel_and_close_destroy_locally() {
    let mut runtime = new_runtime();
    let phase = SWPhase(1);
    let dropped_on = Rc::new(Cell::new(false));
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(1).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let capture = Rc::clone(&dropped_on);
    let prepared = owner
        .prepare_delivery(phase, move |_: &mut usize| {
            capture.set(true);
        })
        .unwrap();
    assert_eq!(Rc::strong_count(&dropped_on), 2);
    let rejected = owner.try_post(phase, |_| {}).err().unwrap();
    assert_eq!(rejected.reason, SWOwnerError::Full);
    assert_eq!(prepared.control.cancel(), SWCancelResult::Requested);
    drop(prepared.ticket);
    assert!(!dropped_on.get());
    assert_eq!(
        owner
            .pump(phase, SWPumpBudget::new(1).with_duration(Duration::ZERO))
            .unwrap()
            .processed(),
        0
    );
    let report = owner.pump(phase, SWPumpBudget::new(1)).unwrap();
    assert_eq!(report.suppressed, 1);
    assert!(!dropped_on.get()); // Callback body was suppressed.
    assert_eq!(Rc::strong_count(&dropped_on), 1);
    assert_eq!(prepared.delivery.status(), SWDeliveryStatus::Suppressed);
    owner.close();
    runtime.shutdown().unwrap();
}

#[test]
fn batch_freezes_entry_frontier_while_live_sees_new_arrivals() {
    for mode in [SWPumpMode::Batch, SWPumpMode::Live] {
        let mut runtime = new_runtime();
        let phase = SWPhase(1);
        let mut owner = runtime
            .owner(Vec::<usize>::new(), NonZeroUsize::new(2).unwrap())
            .unwrap();
        owner.set_phase(phase).unwrap();
        let (start_tx, start_rx) = mpsc::channel();
        let (sent_tx, sent_rx) = mpsc::channel();
        let prepared = owner
            .prepare_delivery(phase, |state: &mut Vec<usize>| state.push(2))
            .unwrap();
        let worker = thread::spawn(move || {
            start_rx.recv().unwrap();
            prepared.ticket.ready();
            sent_tx.send(()).unwrap();
        });
        owner
            .try_post(phase, move |state| {
                state.push(1);
                start_tx.send(()).unwrap();
                sent_rx.recv().unwrap();
            })
            .unwrap();
        let report = owner
            .pump(phase, SWPumpBudget::new(2).with_mode(mode))
            .unwrap();
        worker.join().unwrap();
        assert_eq!(report.invoked, if mode == SWPumpMode::Live { 2 } else { 1 });
        if mode == SWPumpMode::Batch {
            assert_eq!(owner.pump(phase, SWPumpBudget::new(2)).unwrap().invoked, 1);
        }
        assert_eq!(owner.state(), &vec![1, 2]);
        owner.close();
        runtime.shutdown().unwrap();
    }
}

#[test]
fn callback_teardown_and_panic_suppress_following_publication() {
    // Suppressed capture cleanup has the same teardown checkpoint as a
    // callback returning normally, even within a single Live pump.
    let mut runtime = new_runtime();
    let phase = SWPhase(1);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(2).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let capture = CloseOnDrop(owner.control());
    let (first, cancel) = owner.try_post(phase, move |_| drop(capture)).unwrap();
    let (second, _) = owner.try_post(phase, |state| *state += 1).unwrap();
    assert_eq!(cancel.cancel(), SWCancelResult::Requested);
    let report = owner.pump(phase, SWPumpBudget::new(2)).unwrap();
    assert_eq!(report.invoked, 0);
    assert_eq!(report.suppressed, 2);
    assert_eq!(*owner.state(), 0);
    assert_eq!(first.status(), SWDeliveryStatus::Suppressed);
    assert_eq!(second.status(), SWDeliveryStatus::Suppressed);
    runtime.shutdown().unwrap();

    let mut runtime = new_runtime();
    let phase = SWPhase(1);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(2).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let control = owner.control();
    owner
        .try_post(phase, move |state| {
            *state += 1;
            control.request_close();
        })
        .unwrap();
    let (second, _) = owner.try_post(phase, |state| *state += 10).unwrap();
    assert_eq!(owner.pump(phase, SWPumpBudget::new(2)).unwrap().invoked, 1);
    assert_eq!(*owner.state(), 1);
    assert_eq!(second.status(), SWDeliveryStatus::Suppressed);
    runtime.shutdown().unwrap();

    let mut runtime = new_runtime();
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(1).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let (pending, _) = owner.try_post(phase, |state| *state += 10).unwrap();
    let sender = owner.sender();
    owner.control().request_close();
    assert_eq!(
        owner.pump(phase, SWPumpBudget::new(0)).unwrap().processed(),
        0
    );
    assert_eq!(
        sender.try_post(phase, |_| {}).err().unwrap().reason,
        SWOwnerError::Closed
    );
    assert_eq!(pending.status(), SWDeliveryStatus::Ready);
    assert_eq!(
        owner.pump(phase, SWPumpBudget::new(1)).unwrap().suppressed,
        1
    );
    assert_eq!(pending.status(), SWDeliveryStatus::Suppressed);
    assert_eq!(*owner.state(), 0);
    runtime.shutdown().unwrap();

    let mut runtime = new_runtime();
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(2).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let (first, _) = owner.try_post(phase, |_| panic!("owner callback")).unwrap();
    let (second, _) = owner.try_post(phase, |state| *state += 1).unwrap();
    assert_eq!(
        owner.pump(phase, SWPumpBudget::new(1)).unwrap().suppressed,
        1
    );
    assert_eq!(first.status(), SWDeliveryStatus::Panicked);
    assert_eq!(second.status(), SWDeliveryStatus::Ready);
    assert_eq!(
        owner.pump(phase, SWPumpBudget::new(1)).unwrap().suppressed,
        1
    );
    assert_eq!(second.status(), SWDeliveryStatus::Suppressed);
    assert_eq!(*owner.state(), 0);
    let rejected = owner.try_post(phase, |_| {}).err().unwrap();
    assert_eq!(rejected.reason, SWOwnerError::Faulted);
    owner.recover().unwrap();
    owner.try_post(phase, |state| *state += 1).unwrap();
    owner.pump(phase, SWPumpBudget::new(1)).unwrap();
    assert_eq!(*owner.state(), 1);
    owner.close();
    runtime.shutdown().unwrap();
}

#[test]
fn transferable_sender_routes_send_callback_to_non_send_owner_state() {
    let mut runtime = new_runtime();
    let phase = SWPhase(4);
    let local_state = Rc::new(Cell::new(0usize));
    let mut owner = runtime
        .owner(Rc::clone(&local_state), NonZeroUsize::new(1).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let sender = owner.sender();
    let after_close = sender.clone();
    let delivery = thread::spawn(move || {
        sender
            .try_post(phase, |state| state.set(state.get() + 1))
            .unwrap()
            .0
    })
    .join()
    .unwrap();
    assert_eq!(local_state.get(), 0);
    assert_eq!(owner.pump(phase, SWPumpBudget::new(1)).unwrap().invoked, 1);
    assert_eq!(local_state.get(), 1);
    assert_eq!(delivery.status(), SWDeliveryStatus::Published);
    owner.close();
    assert_eq!(
        after_close.try_post(phase, |_| {}).err().unwrap().reason,
        SWOwnerError::Closed
    );
    runtime.shutdown().unwrap();
}

#[test]
fn immediate_ready_panic_faults_routes_until_cleanup_and_recovery() {
    let mut runtime = new_runtime();
    let phase = SWPhase(2);
    let mut owner = runtime
        .owner(0usize, NonZeroUsize::new(1).unwrap())
        .unwrap();
    owner.set_phase(phase).unwrap();
    let (waiting, _) = owner.try_post(phase, |state| *state += 10).unwrap();
    let ready = SWShared::ready(5usize);
    let result: SWReadyAccess<(), _> = owner.with_ready(&ready, phase, |state, _| {
        *state += 1;
        panic!("immediate callback");
    });
    assert!(matches!(result, SWReadyAccess::Panicked));
    assert_eq!(*owner.state(), 1);
    assert_eq!(owner.recover(), Err(SWOwnerError::Faulted));
    assert_eq!(
        owner.try_post(phase, |_| {}).err().unwrap().reason,
        SWOwnerError::Faulted
    );
    assert_eq!(
        owner.pump(phase, SWPumpBudget::new(1)).unwrap().suppressed,
        1
    );
    assert_eq!(waiting.status(), SWDeliveryStatus::Suppressed);
    owner.recover().unwrap();
    owner.try_post(phase, |state| *state += 1).unwrap();
    owner.pump(phase, SWPumpBudget::new(1)).unwrap();
    assert_eq!(*owner.state(), 2);
    owner.close();
    runtime.shutdown().unwrap();
}
