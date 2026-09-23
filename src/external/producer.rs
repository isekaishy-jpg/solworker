//! Logical results supplied by an external provider.

use std::sync::{Arc, Mutex, Weak};

use crate::owner::SWDeliveryTicket;
use crate::scheduler::reservation::SWByteLease;
use crate::scheduler::{
    OwnedScheduler, SWCost, SWDemandSnapshot, SWDiscoveryPermit, SWPriority, SWReservation,
    SWSpawnError, SWWorkSet,
};
use crate::task::{CompletionSink, SWOutcome, SWProducerControl, SWRetained, SWTask, SWTaskStatus};

/// Optional admission, lifetime, publication, and demand policy for one
/// provider-supplied result. The provider is started only after admission.
#[derive(Default)]
pub struct SWExternalOptions<'a> {
    pub work_set: Option<&'a SWWorkSet>,
    pub discovery: Option<&'a SWDiscoveryPermit>,
    pub priority: Option<SWPriority>,
    pub reservation: Option<&'a SWReservation>,
    pub cost: SWCost,
    pub retained_bytes: usize,
    pub delivery: Option<SWDeliveryTicket>,
    /// Called outside scheduler locks with versioned aggregate demand.
    /// A panic is contained and that notification is discarded.
    /// Calls may overlap or arrive after logical settlement; the adapter must
    /// ignore older versions and validate its own provider lifetime before use.
    pub provider_demand: Option<Arc<dyn Fn(SWDemandSnapshot) + Send + Sync>>,
}

/// Accepted producer, reusable result and independent cancellation authority.
pub type SWExternalResult<'a, T> =
    Result<(SWProducer<T>, SWTask<SWRetained<T>>, SWProducerControl), SWExternalRejected<'a>>;

/// Rejection returns all uncommitted options, including the delivery ticket.
pub struct SWExternalRejected<'a> {
    pub reason: SWSpawnError,
    pub options: SWExternalOptions<'a>,
}

impl std::fmt::Debug for SWExternalRejected<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SWExternalRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

type PendingExternal<T> = (CompletionSink<SWRetained<T>>, Option<SWByteLease>);

pub(crate) struct ExternalCore<T> {
    pending: Mutex<Option<PendingExternal<T>>>,
}

impl<T> ExternalCore<T> {
    pub(crate) fn new(sink: CompletionSink<SWRetained<T>>, bytes: Option<SWByteLease>) -> Self {
        Self {
            pending: Mutex::new(Some((sink, bytes))),
        }
    }

    pub(crate) fn publish(&self, outcome: SWOutcome<T>, application_failed: bool) {
        let (sink, bytes) = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .expect("external result claimed exactly once");
        let outcome = match outcome {
            SWOutcome::Success(value) => SWOutcome::Success(SWRetained::new(value, bytes)),
            SWOutcome::Cancelled => SWOutcome::Cancelled,
            SWOutcome::PrerequisiteFailed => SWOutcome::PrerequisiteFailed,
            SWOutcome::Panicked => SWOutcome::Panicked,
            SWOutcome::Abandoned => SWOutcome::Abandoned,
        };
        sink.finish(outcome, application_failed);
    }

    pub(crate) fn publish_retained(&self, value: SWRetained<T>, application_failed: bool) {
        let (sink, unused_bytes) = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .expect("external result claimed exactly once");
        // A provider-owned output charge already follows the value. The
        // producer's optional retained charge is unused on this route.
        drop(unused_bytes);
        sink.finish(SWOutcome::Success(value), application_failed);
    }

    pub(crate) fn publish_status(&self, status: SWTaskStatus) {
        let outcome = match status {
            SWTaskStatus::Cancelled => SWOutcome::Cancelled,
            SWTaskStatus::Abandoned => SWOutcome::Abandoned,
            _ => unreachable!("external control settles cancellation or abandonment"),
        };
        self.publish(outcome, false);
    }
}

/// The single logical result authority for a provider. `complete` may be called
/// from any thread. Dropping an unfinished producer publishes abandonment. This
/// handle does not own or end a provider's separate physical access.
pub struct SWProducer<T: Send + 'static> {
    pub(crate) scheduler: Weak<OwnedScheduler>,
    pub(crate) id: u64,
    pub(crate) core: Arc<ExternalCore<T>>,
}

impl<T: Send + 'static> SWProducer<T> {
    /// Publishes a result if cancellation or abandonment has not already won.
    /// On a lost race, returns the untouched value to its caller. A successful
    /// result may precede physical retirement only when the value is already
    /// safe to observe independently; otherwise await provider acknowledgement
    /// before completing it.
    pub fn complete(&self, value: T) -> Result<(), T> {
        self.complete_with(value, false)
    }

    /// Publishes a value whose declared bytes are already held by its
    /// physical-access lease. Configure `retained_bytes: 0` when using this
    /// route so the producer does not reserve a second output charge.
    pub fn complete_retained(&self, value: SWRetained<T>) -> Result<(), SWRetained<T>> {
        self.complete_retained_with(value, false)
    }

    pub(crate) fn complete_with(&self, value: T, application_failed: bool) -> Result<(), T> {
        let Some(scheduler) = self.scheduler.upgrade() else {
            return Err(value);
        };
        if !scheduler.claim_external(self.id, false) {
            return Err(value);
        }
        scheduler.finish_external(self.id, || {
            self.core
                .publish(SWOutcome::Success(value), application_failed)
        });
        Ok(())
    }

    fn complete_retained_with(
        &self,
        value: SWRetained<T>,
        application_failed: bool,
    ) -> Result<(), SWRetained<T>> {
        let Some(scheduler) = self.scheduler.upgrade() else {
            return Err(value);
        };
        if !scheduler.claim_external(self.id, false) {
            return Err(value);
        }
        scheduler.finish_external(self.id, || {
            self.core.publish_retained(value, application_failed)
        });
        Ok(())
    }

    /// Requests logical cancellation. The provider must still retire any
    /// physical access through its independent access lease.
    pub fn cancel(&self) {
        if let Some(scheduler) = self.scheduler.upgrade() {
            scheduler.settle_external(self.id, SWTaskStatus::Cancelled);
        }
    }

    /// Settles an unfulfilled result as abandoned.
    pub fn abandon(&self) {
        if let Some(scheduler) = self.scheduler.upgrade() {
            scheduler.settle_external(self.id, SWTaskStatus::Abandoned);
        }
    }
}

impl<T: Send + 'static, E: Send + 'static> SWProducer<Result<T, E>> {
    /// Preserves the typed `Err` payload and marks dependency status as an
    /// application failure.
    pub fn complete_fallible(&self, value: Result<T, E>) -> Result<(), Result<T, E>> {
        let failed = value.is_err();
        self.complete_with(value, failed)
    }

    /// Publishes an already charged application result while preserving a
    /// typed `Err` and its dependency failure status.
    pub fn complete_fallible_retained(
        &self,
        value: SWRetained<Result<T, E>>,
    ) -> Result<(), SWRetained<Result<T, E>>> {
        let failed = value.is_err();
        self.complete_retained_with(value, failed)
    }
}

impl<T: Send + 'static> Drop for SWProducer<T> {
    fn drop(&mut self) {
        self.abandon();
    }
}
