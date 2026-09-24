//! Bounded terminal notifications for callbacks owned by one thread.
//!
//! Records carry no owner state or local captures. Each live record owns one
//! notification entitlement, so a terminal send cannot block a CPU worker.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};

use crate::notification::NotifySource;
use crate::owner::SWOwnerError;
use crate::progress::{SWOwnerProgress, SWWake};
use crate::scheduler::reservation::SWReservationPool;
use crate::scheduler::{SWCost, SWReservation, SWReservationError};
use crate::task::{SWCompletion, Subscription};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWDeliveryStatus {
    Waiting,
    Ready,
    Claimed,
    Published,
    Suppressed,
    Panicked,
}

impl SWDeliveryStatus {
    pub fn is_settled(self) -> bool {
        matches!(self, Self::Published | Self::Suppressed | Self::Panicked)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWCancelResult {
    Requested,
    TooLate,
    Settled,
}

/// Observing delivery does not alter the callback or its producer.
#[derive(Clone)]
pub struct SWDelivery {
    record: Arc<Record>,
}

impl SWDelivery {
    pub fn status(&self) -> SWDeliveryStatus {
        self.record.state.lock().unwrap().status
    }
}

/// Transferable suppression request, effective until callback claim.
#[derive(Clone)]
pub struct SWDeliveryControl {
    record: Arc<Record>,
}

impl SWDeliveryControl {
    pub fn cancel(&self) -> SWCancelResult {
        self.record.cancel()
    }
}

struct RecordState {
    status: SWDeliveryStatus,
    cancelled: bool,
    notified: bool,
    committed: bool,
    subscription: Option<Subscription>,
    capacity: Option<SWReservation>,
    accounted: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum EndpointKind {
    Ordinary,
    Protected,
}

struct Record {
    id: u64,
    runtime_identity: u64,
    state: Mutex<RecordState>,
    sender: Sender<NotificationMessage>,
    transport: Weak<Transport>,
    endpoint: EndpointKind,
}

impl Record {
    fn notify(self: &Arc<Self>, state: &mut RecordState) {
        if state.notified {
            return;
        }
        state.notified = true;
        // At most one message exists per live reservation; channel capacity is
        // at least the reservation limit, including sends in flight.
        match self.sender.try_send(NotificationMessage {
            id: self.id,
            pin: Arc::clone(self),
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => unreachable!("notification entitlement exceeded"),
            Err(TrySendError::Disconnected(_)) => {
                unreachable!("transport retains receiver while records exist")
            }
        }
    }

    fn ready(self: &Arc<Self>) {
        let mut state = self.state.lock().unwrap();
        if state.status == SWDeliveryStatus::Waiting && !state.cancelled {
            state.status = SWDeliveryStatus::Ready;
            self.notify(&mut state);
            drop(state);
            if let Some(transport) = self.transport.upgrade() {
                transport.notify_progress();
            }
        }
    }

    fn cancel(self: &Arc<Self>) -> SWCancelResult {
        let mut state = self.state.lock().unwrap();
        let result = match state.status {
            SWDeliveryStatus::Waiting | SWDeliveryStatus::Ready => {
                state.cancelled = true;
                self.notify(&mut state);
                SWCancelResult::Requested
            }
            SWDeliveryStatus::Claimed => SWCancelResult::TooLate,
            SWDeliveryStatus::Published
            | SWDeliveryStatus::Suppressed
            | SWDeliveryStatus::Panicked => SWCancelResult::Settled,
        };
        let subscription = state.subscription.take();
        drop(state);
        if result == SWCancelResult::Requested
            && let Some(transport) = self.transport.upgrade()
        {
            transport.notify_progress();
        }
        drop(subscription);
        result
    }

    fn set_subscription(&self, subscription: Subscription) {
        let mut state = self.state.lock().unwrap();
        if state.status == SWDeliveryStatus::Waiting && !state.cancelled {
            state.subscription = Some(subscription);
            return;
        }
        drop(state);
        drop(subscription);
    }
}

struct NotificationMessage {
    id: u64,
    // Keeps the record alive while a terminal send is in flight or queued.
    pin: Arc<Record>,
}

struct TransportState {
    closed: bool,
    next_id: u64,
    records: HashMap<u64, Arc<Record>>,
    ordinary_records: usize,
    protected_records: usize,
}

/// The owner-side transport. Its receiver stays on the owner thread.
pub(super) struct Transport {
    ordinary_capacity: usize,
    protected_capacity: usize,
    capacity_pool: Option<SWReservationPool>,
    runtime_identity: u64,
    sender: Sender<NotificationMessage>,
    receiver: Receiver<NotificationMessage>,
    state: Mutex<TransportState>,
    wake: OnceLock<Arc<SWWake>>,
    notification_source: OnceLock<NotifySource>,
}

pub(super) struct TransportClosure {
    records: Vec<Arc<Record>>,
}

impl Transport {
    pub(super) fn runtime_identity(&self) -> u64 {
        self.runtime_identity
    }

    #[cfg(test)]
    pub(super) fn new(capacity: NonZeroUsize, runtime_identity: u64) -> Arc<Self> {
        Self::new_with_capacity(capacity, runtime_identity, None)
    }

    pub(super) fn new_with_capacity(
        ordinary_capacity: NonZeroUsize,
        runtime_identity: u64,
        capacity_pool: Option<SWReservationPool>,
    ) -> Arc<Self> {
        let protected_capacity = capacity_pool
            .as_ref()
            .map_or(0, SWReservationPool::required_delivery_capacity);
        let (sender, receiver) =
            bounded(ordinary_capacity.get().saturating_add(protected_capacity));
        Arc::new(Self {
            ordinary_capacity: ordinary_capacity.get(),
            protected_capacity,
            capacity_pool,
            runtime_identity,
            sender,
            receiver,
            state: Mutex::new(TransportState {
                closed: false,
                next_id: 1,
                records: HashMap::new(),
                ordinary_records: 0,
                protected_records: 0,
            }),
            wake: OnceLock::new(),
            notification_source: OnceLock::new(),
        })
    }

    pub(super) fn install_wake(&self, wake: Arc<SWWake>) {
        assert!(self.wake.set(wake).is_ok(), "owner wake installed once");
    }

    pub(super) fn install_notification_source(&self, source: NotifySource) {
        assert!(
            self.notification_source.set(source).is_ok(),
            "owner notification source installed once"
        );
    }

    pub(super) fn notification_source(&self) -> Option<NotifySource> {
        self.notification_source.get().cloned()
    }

    pub(super) fn notifications_enabled(&self) -> bool {
        self.notification_source.get().is_some()
    }

    fn notify_progress(&self) {
        self.notify_runtime_progress();
        if let Some(source) = self.notification_source.get() {
            source.publish();
        }
    }

    fn notify_runtime_progress(&self) {
        if let Some(wake) = self.wake.get() {
            wake.notify();
        }
    }

    /// Snapshot transferable records only. No local callback or capture is
    /// inspected, and the transport lock always precedes each record lock.
    pub(super) fn progress(&self) -> SWOwnerProgress {
        let state = self.state.lock().unwrap();
        let mut progress = SWOwnerProgress {
            routes: 1,
            ..SWOwnerProgress::default()
        };
        for record in state.records.values() {
            let record_state = record.state.lock().unwrap();
            if state.closed || record_state.cancelled {
                progress.cleanup += 1;
            } else {
                match record_state.status {
                    SWDeliveryStatus::Waiting => progress.waiting += 1,
                    SWDeliveryStatus::Ready => progress.ready += 1,
                    SWDeliveryStatus::Claimed => progress.claimed += 1,
                    SWDeliveryStatus::Published
                    | SWDeliveryStatus::Suppressed
                    | SWDeliveryStatus::Panicked => progress.cleanup += 1,
                }
            }
        }
        progress
    }

    pub(super) fn reserve(self: &Arc<Self>) -> Option<Reservation> {
        let charge = match &self.capacity_pool {
            Some(pool) => Some(pool.try_reserve_ordinary(SWCost::new(0, 0, 1, 0)).ok()?),
            None => None,
        };
        self.reserve_with_charge(EndpointKind::Ordinary, charge)
    }

    pub(super) fn reserve_reserved(
        self: &Arc<Self>,
        reservation: &SWReservation,
    ) -> Result<Reservation, SWOwnerError> {
        if reservation.runtime_identity() != self.runtime_identity {
            return Err(SWOwnerError::InvalidContext);
        }
        if self.capacity_pool.is_none() {
            return Err(SWOwnerError::InvalidContext);
        }
        let charge = reservation
            .stage(SWCost::new(0, 0, 1, 0))
            .map_err(|error| match error {
                SWReservationError::Closed => SWOwnerError::Closed,
                SWReservationError::Full
                | SWReservationError::TooLarge
                | SWReservationError::InsufficientCredits => SWOwnerError::Full,
            })?;
        let endpoint = if reservation.is_required() {
            EndpointKind::Protected
        } else {
            EndpointKind::Ordinary
        };
        self.reserve_with_charge(endpoint, Some(charge))
            .ok_or_else(|| {
                if self.is_closed() {
                    SWOwnerError::Closed
                } else {
                    SWOwnerError::Full
                }
            })
    }

    fn reserve_with_charge(
        self: &Arc<Self>,
        endpoint: EndpointKind,
        charge: Option<SWReservation>,
    ) -> Option<Reservation> {
        let mut state = self.state.lock().unwrap();
        let full = match endpoint {
            EndpointKind::Ordinary => state.ordinary_records >= self.ordinary_capacity,
            EndpointKind::Protected => state.protected_records >= self.protected_capacity,
        };
        if state.closed || full {
            return None;
        }
        let id = state.next_id;
        state.next_id = state.next_id.checked_add(1)?;
        let record = Arc::new(Record {
            id,
            runtime_identity: self.runtime_identity,
            state: Mutex::new(RecordState {
                status: SWDeliveryStatus::Waiting,
                cancelled: false,
                notified: false,
                committed: false,
                subscription: None,
                accounted: charge.is_some(),
                capacity: charge,
            }),
            sender: self.sender.clone(),
            transport: Arc::downgrade(self),
            endpoint,
        });
        state.records.insert(id, Arc::clone(&record));
        match endpoint {
            EndpointKind::Ordinary => state.ordinary_records += 1,
            EndpointKind::Protected => state.protected_records += 1,
        }
        drop(state);
        self.notify_runtime_progress();
        Some(Reservation {
            notifier: Notifier {
                inner: Arc::new(NotifierInner {
                    record,
                    terminal: AtomicBool::new(false),
                }),
            },
        })
    }

    pub(super) fn try_recv(&self) -> Option<Notification> {
        self.receiver
            .try_recv()
            .ok()
            .map(|message| Notification { message })
    }

    pub(super) fn pending_count(&self) -> usize {
        self.state.lock().unwrap().records.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending_count() == 0
    }

    pub(super) fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    pub(super) fn begin_close(&self) -> TransportClosure {
        let records = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.records.values().cloned().collect::<Vec<_>>()
        };
        TransportClosure { records }
    }

    pub(super) fn finish_close(&self, closure: TransportClosure) {
        self.notify_progress();
        for record in closure.records {
            record.cancel();
        }
    }

    pub(super) fn close(&self) {
        self.finish_close(self.begin_close());
    }

    pub(super) fn suppress_all(&self) {
        let records = self
            .state
            .lock()
            .unwrap()
            .records
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for record in records {
            record.cancel();
        }
    }
}

/// One reserved record and terminal notification entitlement.
pub(super) struct Reservation {
    notifier: Notifier,
}

impl Reservation {
    pub(super) fn id(&self) -> u64 {
        self.notifier.inner.record.id
    }

    pub(super) fn observer(&self) -> SWDelivery {
        SWDelivery {
            record: Arc::clone(&self.notifier.inner.record),
        }
    }

    pub(super) fn control(&self) -> SWDeliveryControl {
        SWDeliveryControl {
            record: Arc::clone(&self.notifier.inner.record),
        }
    }

    pub(super) fn notifier(&self) -> Notifier {
        self.notifier.clone()
    }

    pub(super) fn ticket(self) -> SWDeliveryTicket {
        SWDeliveryTicket {
            notifier: self.notifier,
        }
    }
}

/// Terminal signal capability for a completion subscription.
struct NotifierInner {
    record: Arc<Record>,
    terminal: AtomicBool,
}

impl Drop for NotifierInner {
    fn drop(&mut self) {
        if !self.terminal.load(Ordering::Acquire) {
            self.record.cancel();
        }
    }
}

#[derive(Clone)]
pub(super) struct Notifier {
    inner: Arc<NotifierInner>,
}

impl Notifier {
    pub(super) fn ready(&self) {
        if !self.inner.terminal.swap(true, Ordering::AcqRel) {
            self.inner.record.ready();
        }
    }

    pub(super) fn suppress(&self) {
        if !self.inner.terminal.swap(true, Ordering::AcqRel) {
            self.inner.record.cancel();
        }
    }
}

/// Transferable delivery promise. Dropping it requests local suppression.
pub struct SWDeliveryTicket {
    notifier: Notifier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TicketCommitError {
    Closed,
    AlreadyCommitted,
}

impl SWDeliveryTicket {
    pub(crate) fn is_accounted(&self) -> bool {
        self.notifier.inner.record.state.lock().unwrap().accounted
    }

    pub(crate) fn attach_capacity(&self, capacity: SWReservation) {
        let mut state = self.notifier.inner.record.state.lock().unwrap();
        if !state.status.is_settled() {
            assert!(
                state.capacity.is_none(),
                "delivery capacity already attached"
            );
            state.accounted = true;
            state.capacity = Some(capacity);
        } else {
            drop(state);
            drop(capacity);
        }
    }
    pub(crate) fn runtime_identity(&self) -> u64 {
        self.notifier.inner.record.runtime_identity
    }

    #[cfg(test)]
    pub(crate) fn is_available(&self) -> bool {
        let Some(transport) = self.notifier.inner.record.transport.upgrade() else {
            return false;
        };
        if transport.state.lock().unwrap().closed {
            return false;
        }
        let state = self.notifier.inner.record.state.lock().unwrap();
        state.status == SWDeliveryStatus::Waiting && !state.cancelled && !state.committed
    }

    /// This is the last fallible step of CPU admission. It arbitrates with
    /// owner close under the transport lock before the scheduler exposes work.
    pub(crate) fn try_commit(&mut self) -> Result<(), TicketCommitError> {
        let Some(transport) = self.notifier.inner.record.transport.upgrade() else {
            return Err(TicketCommitError::Closed);
        };
        let transport_state = transport.state.lock().unwrap();
        if transport_state.closed {
            return Err(TicketCommitError::Closed);
        }
        let mut record_state = self.notifier.inner.record.state.lock().unwrap();
        if record_state.committed {
            return Err(TicketCommitError::AlreadyCommitted);
        }
        if record_state.cancelled || record_state.status != SWDeliveryStatus::Waiting {
            return Err(TicketCommitError::Closed);
        }
        record_state.committed = true;
        Ok(())
    }

    pub(crate) fn bind(self, completion: SWCompletion) {
        let record = Arc::clone(&self.notifier.inner.record);
        let subscription = completion.subscribe_cancelable(Box::new(move |_| self.ready()));
        record.set_subscription(subscription);
    }

    pub fn ready(self) {
        self.notifier.ready();
    }

    pub fn suppress(self) {
        self.notifier.suppress();
    }
}

impl Drop for SWDeliveryTicket {
    fn drop(&mut self) {
        self.notifier.suppress();
    }
}

/// Owner-side terminal notification; its record remains pinned until settled.
pub(super) struct Notification {
    message: NotificationMessage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClaimResult {
    Run,
    Suppress,
}

impl Notification {
    pub(super) fn id(&self) -> u64 {
        self.message.id
    }

    pub(super) fn claim(&self) -> ClaimResult {
        // Closure and claim share the transport lock. Keep the same
        // transport -> record order as ticket commitment. Once claim wins,
        // later closure cannot retract its decision.
        let transport = self.message.pin.transport.upgrade();
        let transport_state = transport
            .as_ref()
            .map(|transport| transport.state.lock().unwrap());
        let mut state = self.message.pin.state.lock().unwrap();
        let run = transport_state.as_ref().is_some_and(|state| !state.closed)
            && state.status == SWDeliveryStatus::Ready
            && !state.cancelled;
        assert!(
            matches!(
                state.status,
                SWDeliveryStatus::Waiting | SWDeliveryStatus::Ready
            ) && state.notified,
            "each notification is claimed once"
        );
        state.status = SWDeliveryStatus::Claimed;
        drop(state);
        drop(transport_state);
        if let Some(transport) = transport {
            transport.notify_progress();
        }
        if run {
            ClaimResult::Run
        } else {
            ClaimResult::Suppress
        }
    }

    pub(super) fn settle(self, status: SWDeliveryStatus) {
        assert!(
            matches!(
                status,
                SWDeliveryStatus::Published
                    | SWDeliveryStatus::Suppressed
                    | SWDeliveryStatus::Panicked
            ),
            "only terminal delivery states settle"
        );
        let (subscription, capacity) = {
            let mut state = self.message.pin.state.lock().unwrap();
            assert_eq!(state.status, SWDeliveryStatus::Claimed);
            state.status = status;
            (state.subscription.take(), state.capacity.take())
        };
        drop(subscription);
        drop(capacity);
        if let Some(transport) = self.message.pin.transport.upgrade() {
            let mut state = transport.state.lock().unwrap();
            if state.records.remove(&self.message.id).is_some() {
                match self.message.pin.endpoint {
                    EndpointKind::Ordinary => state.ordinary_records -= 1,
                    EndpointKind::Protected => state.protected_records -= 1,
                }
            }
            drop(state);
            transport.notify_progress();
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/owner_delivery.rs"]
mod tests;
