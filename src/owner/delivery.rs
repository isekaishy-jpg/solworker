//! Bounded terminal notifications for callbacks owned by one thread.
//!
//! Records carry no owner state or local captures. Each live record owns one
//! notification entitlement, so a terminal send cannot block a CPU worker.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};

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
}

struct Record {
    id: u64,
    runtime_identity: u64,
    state: Mutex<RecordState>,
    sender: Sender<NotificationMessage>,
    transport: Weak<Transport>,
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
}

/// The owner-side transport. Its receiver stays on the owner thread.
pub(super) struct Transport {
    capacity: usize,
    runtime_identity: u64,
    sender: Sender<NotificationMessage>,
    receiver: Receiver<NotificationMessage>,
    state: Mutex<TransportState>,
}

impl Transport {
    pub(super) fn new(capacity: NonZeroUsize, runtime_identity: u64) -> Arc<Self> {
        let (sender, receiver) = bounded(capacity.get());
        Arc::new(Self {
            capacity: capacity.get(),
            runtime_identity,
            sender,
            receiver,
            state: Mutex::new(TransportState {
                closed: false,
                next_id: 1,
                records: HashMap::new(),
            }),
        })
    }

    pub(super) fn reserve(self: &Arc<Self>) -> Option<Reservation> {
        let mut state = self.state.lock().unwrap();
        if state.closed || state.records.len() >= self.capacity {
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
            }),
            sender: self.sender.clone(),
            transport: Arc::downgrade(self),
        });
        state.records.insert(id, Arc::clone(&record));
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

    pub(super) fn close(&self) {
        let records = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.records.values().cloned().collect::<Vec<_>>()
        };
        for record in records {
            record.cancel();
        }
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
        let subscription = {
            let mut state = self.message.pin.state.lock().unwrap();
            assert_eq!(state.status, SWDeliveryStatus::Claimed);
            state.status = status;
            state.subscription.take()
        };
        drop(subscription);
        if let Some(transport) = self.message.pin.transport.upgrade() {
            transport
                .state
                .lock()
                .unwrap()
                .records
                .remove(&self.message.id);
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/owner_delivery.rs"]
mod tests;
