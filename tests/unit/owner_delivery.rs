use super::{ClaimResult, SWCancelResult, SWDeliveryStatus, TicketCommitError, Transport};
use std::num::NonZeroUsize;
use std::sync::{Arc, Barrier};

#[test]
fn protected_delivery_survives_ordinary_endpoint_saturation() {
    use crate::scheduler::reservation::SWReservationPool;
    use crate::scheduler::{SWCost, SWLimits};

    let limits = SWLimits::new(SWCost::new(0, 0, 1, 0), SWCost::new(0, 0, 1, 0), 1, None).unwrap();
    let pool = SWReservationPool::new(11, limits);
    let transport =
        Transport::new_with_capacity(NonZeroUsize::new(1).unwrap(), 11, Some(pool.clone()));
    let ordinary = transport.reserve().unwrap().ticket();
    assert!(ordinary.is_accounted());
    assert!(transport.reserve().is_none());
    let required = pool.try_reserve_required(SWCost::new(0, 0, 1, 0)).unwrap();
    let protected = transport.reserve_reserved(&required).unwrap().ticket();
    assert!(protected.is_accounted());
    assert!(transport.reserve_reserved(&required).is_err());
    ordinary.ready();
    protected.ready();
    for _ in 0..2 {
        let notification = transport.try_recv().unwrap();
        assert_eq!(notification.claim(), ClaimResult::Run);
        notification.settle(SWDeliveryStatus::Published);
    }
    assert_eq!(pool.snapshot().ordinary.deliveries, 0);
    assert_eq!(required.available().deliveries, 1);
    drop(required);
    assert_eq!(pool.snapshot().required.deliveries, 0);
}

#[test]
fn ready_ticket_notifies_once_and_settles_after_claim() {
    let transport = Transport::new(NonZeroUsize::new(1).unwrap(), 42);
    let reservation = transport.reserve().unwrap();
    let observer = reservation.observer();
    let control = reservation.control();
    let mut ticket = reservation.ticket();
    assert_eq!(ticket.runtime_identity(), 42);
    assert_eq!(ticket.try_commit(), Ok(()));
    assert_eq!(
        ticket.try_commit(),
        Err(TicketCommitError::AlreadyCommitted)
    );
    ticket.ready();

    let notification = transport.try_recv().unwrap();
    assert_eq!(notification.claim(), ClaimResult::Run);
    assert_eq!(control.cancel(), SWCancelResult::TooLate);
    assert_eq!(observer.status(), SWDeliveryStatus::Claimed);
    notification.settle(SWDeliveryStatus::Published);
    assert_eq!(observer.status(), SWDeliveryStatus::Published);
    assert!(transport.try_recv().is_none());
    assert!(transport.is_empty());
}

#[test]
fn cancel_and_close_retain_owner_cleanup_entitlements() {
    let transport = Transport::new(NonZeroUsize::new(2).unwrap(), 7);
    let wake = crate::progress::SWWake::new();
    transport.install_wake(Arc::clone(&wake));
    let first = transport.reserve().unwrap();
    let second = transport.reserve().unwrap();
    let first_id = first.id();
    let second_id = second.id();
    let first_observer = first.observer();
    let second_observer = second.observer();
    let first_ticket = first.ticket();
    let second_ticket = second.ticket();
    transport.close();
    assert!(transport.reserve().is_none());
    assert!(!first_ticket.is_available());
    assert!(!second_ticket.is_available());
    first_ticket.ready();
    drop(second_ticket);

    let mut ids = Vec::new();
    while let Some(notification) = transport.try_recv() {
        ids.push(notification.id());
        let before_claim = wake.generation();
        assert_eq!(notification.claim(), ClaimResult::Suppress);
        assert!(wake.generation() > before_claim);
        notification.settle(SWDeliveryStatus::Suppressed);
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![first_id, second_id]);
    assert_eq!(first_observer.status(), SWDeliveryStatus::Suppressed);
    assert_eq!(second_observer.status(), SWDeliveryStatus::Suppressed);
    assert!(transport.is_empty());
}

#[test]
fn cancellation_winning_claim_suppresses_even_when_ready_races() {
    let transport = Transport::new(NonZeroUsize::new(1).unwrap(), 1);
    let reservation = transport.reserve().unwrap();
    let observer = reservation.observer();
    let control = reservation.control();
    let ticket = reservation.ticket();
    let gate = Arc::new(Barrier::new(2));
    let worker_gate = Arc::clone(&gate);
    let worker = std::thread::spawn(move || {
        worker_gate.wait();
        ticket.ready();
    });
    gate.wait();
    assert_eq!(control.cancel(), SWCancelResult::Requested);
    worker.join().unwrap();
    let notification = transport.try_recv().unwrap();
    assert_eq!(notification.claim(), ClaimResult::Suppress);
    notification.settle(SWDeliveryStatus::Suppressed);
    assert_eq!(observer.status(), SWDeliveryStatus::Suppressed);
    assert!(transport.try_recv().is_none());
}

#[test]
fn observer_drop_does_not_cancel_and_reuse_waits_for_settlement() {
    let transport = Transport::new(NonZeroUsize::new(1).unwrap(), 3);
    let reservation = transport.reserve().unwrap();
    let id = reservation.id();
    drop(reservation.observer());
    reservation.ticket().ready();
    let notification = transport.try_recv().unwrap();
    assert_eq!(notification.id(), id);
    assert!(transport.reserve().is_none());
    assert_eq!(notification.claim(), ClaimResult::Run);
    notification.settle(SWDeliveryStatus::Published);
    assert!(transport.reserve().is_some());
}

use crate::{SWPhase, SWPumpBudget, SWRuntime, SWRuntimeConfig, SWTask, SWWorkerConfig};

struct CloseTransportOnDrop(Arc<Transport>);

impl Drop for CloseTransportOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

#[test]
fn publication_honors_transport_closure_on_either_side_of_claim() {
    for close_before_claim in [true, false] {
        let config = SWRuntimeConfig::new(3, [SWWorkerConfig::new(1); 3]).unwrap();
        let mut runtime = SWRuntime::builder(config).build().unwrap();
        let mut owner = runtime
            .owner(0usize, NonZeroUsize::new(1).unwrap())
            .unwrap();
        let phase = SWPhase(1);
        owner.set_phase(phase).unwrap();
        let (delivery, control) = owner.try_post(phase, |state| *state += 1).unwrap();
        let (pending, _sink) = SWTask::<()>::pending_pair();

        if close_before_claim {
            // Pause closure after its linearization point but before record
            // cancellation. Claim must consult transport closure itself.
            owner.transport.state.lock().unwrap().closed = true;
        } else {
            // Detaching a registration is the existing boundary immediately
            // after claim and before invocation. Use its capture destructor
            // to close transport at precisely that point without a test hook
            // in production execution or a timing-dependent race.
            let closer = CloseTransportOnDrop(Arc::clone(&owner.transport));
            let subscription = pending
                .completion()
                .subscribe_cancelable(Box::new(move |_| drop(closer)));
            owner.callbacks.values_mut().next().unwrap().subscription = Some(subscription);
        }

        let report = owner.pump(phase, SWPumpBudget::new(1)).unwrap();
        assert_eq!(report.invoked, usize::from(!close_before_claim));
        assert_eq!(report.suppressed, usize::from(close_before_claim));
        assert_eq!(*owner.state(), usize::from(!close_before_claim));
        assert_eq!(
            delivery.status(),
            if close_before_claim {
                SWDeliveryStatus::Suppressed
            } else {
                SWDeliveryStatus::Published
            }
        );
        assert_eq!(control.cancel(), SWCancelResult::Settled);
        owner.close();
        runtime.shutdown().unwrap();
    }
}
