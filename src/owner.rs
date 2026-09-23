//! Thread-bound owner state and transferable delivery routes.

mod delivery;
mod phase;
mod sender;

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::runtime::{OwnerCallbackGuard, OwnerRegistration, RuntimeControl};
use crate::{SWCompletion, SWExecutionError, SWOutcome, SWShared, SWTaskStatus};
pub(crate) use delivery::TicketCommitError;
use delivery::{ClaimResult, Notification, Reservation, Transport};
pub use delivery::{
    SWCancelResult, SWDelivery, SWDeliveryControl, SWDeliveryStatus, SWDeliveryTicket,
};
pub use phase::{SWPhase, SWPumpBudget, SWPumpMode, SWPumpReport};
use sender::Inbox;
pub use sender::SWOwnerSender;

/// An expected owner operation failure. `Panicked` means a local callback
/// unwound and this owner has faulted; its state is not rolled back.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWOwnerError {
    Closed,
    Full,
    InvalidContext,
    WrongPhase,
    Faulted,
    Panicked,
    NotReady,
}

/// Rejected registration returns the uninvoked local callback to its caller.
pub struct SWOwnerRejected<F> {
    pub reason: SWOwnerError,
    pub callback: F,
}

impl<F> fmt::Debug for SWOwnerRejected<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SWOwnerRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

/// Result of immediate ready access. A pending result preserves its callback.
pub enum SWReadyAccess<R, F> {
    Ready(R),
    Rejected(SWOwnerRejected<F>),
    Panicked,
}

/// A locally stored callback with its retained prerequisite registration.
struct LocalDelivery<O> {
    phase: SWPhase,
    callback: Box<dyn FnOnce(&mut O)>,
    subscription: Option<crate::task::Subscription>,
}

/// Returned by a promised local delivery reservation. The ticket is Send and
/// can be attached to admitted CPU work; the callback remains owner-local.
pub struct SWPreparedDelivery {
    pub ticket: SWDeliveryTicket,
    pub delivery: SWDelivery,
    pub control: SWDeliveryControl,
}

/// A callback-safe teardown request. The current mutable state borrow ends
/// before the owner performs local capture cleanup.
#[derive(Clone)]
pub struct SWOwnerControl {
    close_requested: Arc<AtomicBool>,
}

impl SWOwnerControl {
    /// Requests closure for the next owner pump or callback return. This does
    /// not run cleanup on the requesting thread. Sender admission closes when
    /// the owner observes the request; no unclaimed callback then publishes.
    pub fn request_close(&self) {
        self.close_requested.store(true, Ordering::Release);
    }
}

/// A host-thread owner. `O` and local callbacks can be non-Send; the explicit
/// `Rc` marker prevents moving this value to another thread even when `O: Send`.
pub struct SWOwner<O> {
    state: O,
    transport: Arc<Transport>,
    inbox: Arc<Inbox<O>>,
    registration: Option<OwnerRegistration>,
    phase: Option<SWPhase>,
    callbacks: HashMap<u64, LocalDelivery<O>>,
    pending: VecDeque<Notification>,
    closed: bool,
    faulted: bool,
    active: bool,
    close_requested: Arc<AtomicBool>,
    _thread_bound: PhantomData<Rc<()>>,
}

impl<O> SWOwner<O> {
    pub(crate) fn new(
        control: Arc<RuntimeControl>,
        state: O,
        capacity: NonZeroUsize,
    ) -> Result<Self, SWOwnerError> {
        let transport = Transport::new(capacity, control.identity());
        let for_close = Arc::clone(&transport);
        let registration = control
            .register_owner(Arc::new(move || for_close.close()))
            .map_err(|error| match error {
                SWExecutionError::Closed => SWOwnerError::Closed,
                _ => SWOwnerError::InvalidContext,
            })?;
        let inbox = Inbox::new(Arc::clone(&transport));
        Ok(Self {
            state,
            transport,
            inbox,
            registration: Some(registration),
            phase: None,
            callbacks: HashMap::new(),
            pending: VecDeque::new(),
            closed: false,
            faulted: false,
            active: false,
            close_requested: Arc::new(AtomicBool::new(false)),
            _thread_bound: PhantomData,
        })
    }

    /// The host explicitly establishes the current application phase. It may
    /// then use immediate access or pump that phase's delivery route.
    pub fn set_phase(&mut self, phase: SWPhase) -> Result<(), SWOwnerError> {
        if self.active {
            return Err(SWOwnerError::InvalidContext);
        }
        self.phase = Some(phase);
        Ok(())
    }

    pub fn phase(&self) -> Option<SWPhase> {
        self.phase
    }

    pub fn control(&self) -> SWOwnerControl {
        SWOwnerControl {
            close_requested: Arc::clone(&self.close_requested),
        }
    }

    /// A transferable phase-addressed sender. Its callbacks must be Send;
    /// they still execute and are destroyed on this owner thread.
    pub fn sender(&self) -> SWOwnerSender<O> {
        self.inbox.sender()
    }

    /// State inspection is explicit host recovery after a caught callback
    /// panic. Calling this does not clear the fault automatically.
    pub fn state(&self) -> &O {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut O {
        &mut self.state
    }

    pub fn recover(&mut self) -> Result<(), SWOwnerError> {
        if self.closed || self.transport.is_closed() {
            return Err(SWOwnerError::Closed);
        }
        if !self.callbacks.is_empty() || !self.inbox.is_empty() || !self.transport.is_empty() {
            return Err(SWOwnerError::Faulted);
        }
        self.faulted = false;
        self.inbox.set_faulted(false);
        Ok(())
    }

    fn accept(&self) -> Result<(), SWOwnerError> {
        if self.closed || self.transport.is_closed() || self.close_requested.load(Ordering::Acquire)
        {
            Err(SWOwnerError::Closed)
        } else if self.faulted {
            Err(SWOwnerError::Faulted)
        } else if self.active {
            Err(SWOwnerError::InvalidContext)
        } else {
            Ok(())
        }
    }

    fn reserve<F>(&mut self, phase: SWPhase, callback: F) -> Result<Reservation, SWOwnerRejected<F>>
    where
        F: FnOnce(&mut O) + 'static,
    {
        if let Err(reason) = self.accept() {
            return Err(SWOwnerRejected { reason, callback });
        }
        let Some(reservation) = self.transport.reserve() else {
            return Err(SWOwnerRejected {
                reason: if self.transport.is_closed() {
                    SWOwnerError::Closed
                } else {
                    SWOwnerError::Full
                },
                callback,
            });
        };
        self.callbacks.insert(
            reservation.id(),
            LocalDelivery {
                phase,
                callback: Box::new(callback),
                subscription: None,
            },
        );
        Ok(reservation)
    }

    /// Reserves bounded notification capacity before an application admits
    /// producing work. Dropping an unused ticket suppresses and locally cleans
    /// the callback at the next owner pump or close.
    pub fn prepare_delivery<F>(
        &mut self,
        phase: SWPhase,
        callback: F,
    ) -> Result<SWPreparedDelivery, SWOwnerRejected<F>>
    where
        F: FnOnce(&mut O) + 'static,
    {
        let reservation = self.reserve(phase, callback)?;
        Ok(SWPreparedDelivery {
            delivery: reservation.observer(),
            control: reservation.control(),
            ticket: reservation.ticket(),
        })
    }

    /// Queues a local callback for this phase. It never invokes inline.
    pub fn try_post<F>(
        &mut self,
        phase: SWPhase,
        callback: F,
    ) -> Result<(SWDelivery, SWDeliveryControl), SWOwnerRejected<F>>
    where
        F: FnOnce(&mut O) + 'static,
    {
        let reservation = self.reserve(phase, callback)?;
        let observer = reservation.observer();
        let control = reservation.control();
        reservation.notifier().ready();
        Ok((observer, control))
    }

    /// Subscribes to a CPU completion while retaining the callback locally.
    /// Ready-at-registration follows the same deferred path as pending work.
    pub fn on_ready<F>(
        &mut self,
        completion: &SWCompletion,
        phase: SWPhase,
        callback: F,
    ) -> Result<(SWDelivery, SWDeliveryControl), SWOwnerRejected<F>>
    where
        F: FnOnce(&mut O, SWTaskStatus) + 'static,
    {
        if let Err(reason) = self.accept() {
            return Err(SWOwnerRejected { reason, callback });
        }
        let Some(reservation) = self.transport.reserve() else {
            return Err(SWOwnerRejected {
                reason: if self.transport.is_closed() {
                    SWOwnerError::Closed
                } else {
                    SWOwnerError::Full
                },
                callback,
            });
        };
        let observed = completion.clone();
        self.callbacks.insert(
            reservation.id(),
            LocalDelivery {
                phase,
                callback: Box::new(move |state| {
                    let status = observed
                        .status()
                        .expect("ready delivery has published completion status");
                    callback(state, status);
                }),
                subscription: None,
            },
        );
        let id = reservation.id();
        let observer = reservation.observer();
        let control = reservation.control();
        let ticket = reservation.ticket();
        let subscription = completion.subscribe_cancelable(Box::new(move |_| ticket.ready()));
        self.callbacks
            .get_mut(&id)
            .expect("reserved callback remains local")
            .subscription = Some(subscription);
        Ok((observer, control))
    }

    /// Runs a ready shared outcome synchronously in the selected valid phase.
    /// A pending input returns the untouched callback. Caught panics fault all
    /// ordinary routes; the host may inspect and explicitly recover or close.
    pub fn with_ready<T, F, R>(
        &mut self,
        result: &SWShared<T>,
        phase: SWPhase,
        callback: F,
    ) -> SWReadyAccess<R, F>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&mut O, Arc<SWOutcome<T>>) -> R,
    {
        if let Err(reason) = self.accept() {
            return SWReadyAccess::Rejected(SWOwnerRejected { reason, callback });
        }
        if self.phase != Some(phase) {
            return SWReadyAccess::Rejected(SWOwnerRejected {
                reason: SWOwnerError::WrongPhase,
                callback,
            });
        }
        let Some(value) = result.try_result() else {
            return SWReadyAccess::Rejected(SWOwnerRejected {
                reason: SWOwnerError::NotReady,
                callback,
            });
        };
        self.active = true;
        let outcome = {
            let _guard = OwnerCallbackGuard::enter();
            catch_unwind(AssertUnwindSafe(|| callback(&mut self.state, value)))
        };
        self.active = false;
        let access = match outcome {
            Ok(value) => SWReadyAccess::Ready(value),
            Err(_) => {
                self.fault();
                SWReadyAccess::Panicked
            }
        };
        self.finish_close_request();
        access
    }

    fn receive(&mut self) {
        while let Some(notification) = self.transport.try_recv() {
            if let Some((phase, callback)) = self.inbox.take(notification.id()) {
                self.callbacks.insert(
                    notification.id(),
                    LocalDelivery {
                        phase,
                        callback,
                        subscription: None,
                    },
                );
            }
            self.pending.push_back(notification);
        }
    }

    fn next_for(&mut self, phase: SWPhase) -> Option<Notification> {
        if self.closed || self.faulted || self.transport.is_closed() {
            return self.pending.pop_front();
        }
        let index = self.pending.iter().position(|notification| {
            self.callbacks
                .get(&notification.id())
                .is_some_and(|entry| entry.phase == phase)
        })?;
        self.pending.remove(index)
    }

    /// Runs or cleans eligible entries within the caller's count/time budget.
    /// No callback or local destructor executes under transport locks.
    pub fn pump(
        &mut self,
        phase: SWPhase,
        budget: SWPumpBudget,
    ) -> Result<SWPumpReport, SWOwnerError> {
        if self.active {
            return Err(SWOwnerError::InvalidContext);
        }
        self.finish_close_request();
        if self.transport.is_closed() {
            self.closed = true;
        }
        if self.phase != Some(phase) && !self.closed && !self.faulted && !self.transport.is_closed()
        {
            return Err(SWOwnerError::WrongPhase);
        }
        self.receive();
        let frontier = if budget.mode == SWPumpMode::Batch {
            self.pending
                .iter()
                .filter(|notification| {
                    if self.closed || self.faulted || self.transport.is_closed() {
                        return true;
                    }
                    self.callbacks
                        .get(&notification.id())
                        .is_some_and(|entry| entry.phase == phase)
                })
                .count()
        } else {
            usize::MAX
        };
        let started = Instant::now();
        let mut report = SWPumpReport::default();
        while report.processed() < budget.max_entries && report.processed() < frontier {
            if budget
                .max_duration
                .is_some_and(|duration| started.elapsed() >= duration)
            {
                break;
            }
            if budget.mode == SWPumpMode::Live {
                self.receive();
            }
            let Some(notification) = self.next_for(phase) else {
                break;
            };
            self.process(notification, &mut report);
        }
        self.unregister_if_drained();
        Ok(report)
    }

    fn process(&mut self, notification: Notification, report: &mut SWPumpReport) {
        let id = notification.id();
        let Some(mut entry) = self.callbacks.remove(&id) else {
            notification.claim();
            notification.settle(SWDeliveryStatus::Suppressed);
            report.suppressed += 1;
            return;
        };
        let claimed = notification.claim();
        // Detach unresolved prerequisite capture before callback/destructor
        // cleanup, outside transport synchronization.
        entry.subscription.take();
        // The synchronized claim is the cutoff, including runtime abandonment.
        // Rechecking transport closure here would retract an accepted claim.
        if claimed == ClaimResult::Suppress {
            {
                let _guard = OwnerCallbackGuard::enter();
                drop(entry);
            }
            notification.settle(SWDeliveryStatus::Suppressed);
            report.suppressed += 1;
            // Local destructors may request teardown just like callbacks.
            self.finish_close_request();
            return;
        }
        self.active = true;
        let result = {
            let _guard = OwnerCallbackGuard::enter();
            catch_unwind(AssertUnwindSafe(|| (entry.callback)(&mut self.state)))
        };
        self.active = false;
        match result {
            Ok(()) => {
                notification.settle(SWDeliveryStatus::Published);
                report.invoked += 1;
            }
            Err(_) => {
                notification.settle(SWDeliveryStatus::Panicked);
                report.suppressed += 1;
                self.fault();
            }
        }
        self.finish_close_request();
    }

    fn finish_close_request(&mut self) {
        if self.close_requested.swap(false, Ordering::AcqRel) {
            self.closed = true;
            self.inbox.mark_closed();
        }
        self.unregister_if_drained();
    }

    fn unregister_if_drained(&mut self) {
        if self.closed
            && self.callbacks.is_empty()
            && self.inbox.is_empty()
            && self.transport.is_empty()
        {
            self.registration.take();
        }
    }

    fn fault(&mut self) {
        self.faulted = true;
        self.inbox.set_faulted(true);
        self.transport.suppress_all();
    }

    fn cleanup_all(&mut self) {
        self.receive();
        while let Some(notification) = self.pending.pop_front() {
            let id = notification.id();
            notification.claim();
            if let Some(entry) = self.callbacks.remove(&id) {
                let _guard = OwnerCallbackGuard::enter();
                drop(entry);
            }
            notification.settle(SWDeliveryStatus::Suppressed);
        }
    }

    /// Rejects new registrations and suppresses unclaimed callbacks. Local
    /// cleanup finishes before runtime owner registration is released.
    pub fn close(&mut self) {
        self.closed = true;
        let transferred = self.inbox.close_and_take();
        for (id, phase, callback) in transferred {
            self.callbacks.insert(
                id,
                LocalDelivery {
                    phase,
                    callback,
                    subscription: None,
                },
            );
        }
        self.transport.close();
        self.cleanup_all();
        self.registration.take();
    }
}

impl<O> Drop for SWOwner<O> {
    fn drop(&mut self) {
        self.close();
    }
}
