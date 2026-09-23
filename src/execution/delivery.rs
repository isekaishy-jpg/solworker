//! Atomic admission of CPU work with a reserved owner publication.

use std::fmt;

use super::{SWGroup, SWLane};
use crate::owner::SWDeliveryTicket;
use crate::scheduler::{
    SWDependencyPolicy, SWSpawnError, SWSpawnOptions, SubmitRequest, call_once, never_fail,
    result_is_err,
};
use crate::task::{SWCompletion, SWProducerControl, SWTask};

/// CPU routing for a reserved owner delivery. Dependencies gate execution;
/// every terminal CPU outcome makes the delivery ready for owner inspection.
#[derive(Clone, Copy)]
pub struct SWDeliveryOptions<'a> {
    pub spawn: SWSpawnOptions,
    pub group: Option<&'a SWGroup>,
    pub prerequisites: &'a [SWCompletion],
    pub policy: SWDependencyPolicy,
}

impl Default for SWDeliveryOptions<'_> {
    fn default() -> Self {
        Self::new(SWSpawnOptions::default())
    }
}

impl SWDeliveryOptions<'_> {
    pub fn new(spawn: SWSpawnOptions) -> Self {
        Self {
            spawn,
            group: None,
            prerequisites: &[],
            policy: SWDependencyPolicy::SuccessOnly,
        }
    }
}

/// Rejection preserves the uninvoked operation and its uncommitted ticket.
pub struct SWDeliverySpawnRejected<'a, F> {
    pub reason: SWSpawnError,
    pub operation: F,
    pub ticket: SWDeliveryTicket,
    pub options: SWDeliveryOptions<'a>,
}

impl<F> fmt::Debug for SWDeliverySpawnRejected<'_, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SWDeliverySpawnRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

pub type SWDeliverySpawnResult<'a, T, F> =
    Result<(SWTask<T>, SWProducerControl), SWDeliverySpawnRejected<'a, F>>;

impl SWLane {
    /// Commits the reserved owner delivery before exposing CPU work. Rejection
    /// returns both inputs. Closing the owner after acceptance can suppress
    /// publication independently of CPU completion. Never invokes the owner callback.
    pub fn try_spawn_delivering<'a, F, T>(
        &self,
        options: SWDeliveryOptions<'a>,
        ticket: SWDeliveryTicket,
        operation: F,
    ) -> SWDeliverySpawnResult<'a, T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_delivering(options, ticket, operation, never_fail::<T>, false)
    }

    /// As `try_spawn_delivering`, classifying a returned `Err` as application
    /// failure for dependent CPU work. Owner notification still occurs.
    pub fn try_spawn_fallible_delivering<'a, F, T, E>(
        &self,
        options: SWDeliveryOptions<'a>,
        ticket: SWDeliveryTicket,
        operation: F,
    ) -> SWDeliverySpawnResult<'a, Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_delivering(options, ticket, operation, result_is_err::<T, E>, false)
    }

    /// Allows explicitly caller-eligible work to run here when runnable capacity
    /// is full. The delivery is committed first and publication remains deferred.
    pub fn submit_or_run_delivering<'a, F, T>(
        &self,
        options: SWDeliveryOptions<'a>,
        ticket: SWDeliveryTicket,
        operation: F,
    ) -> SWDeliverySpawnResult<'a, T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_delivering(options, ticket, operation, never_fail::<T>, true)
    }

    /// Fallible counterpart of `submit_or_run_delivering`.
    pub fn submit_or_run_fallible_delivering<'a, F, T, E>(
        &self,
        options: SWDeliveryOptions<'a>,
        ticket: SWDeliveryTicket,
        operation: F,
    ) -> SWDeliverySpawnResult<'a, Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_delivering(options, ticket, operation, result_is_err::<T, E>, true)
    }

    fn submit_delivering<'a, F, T>(
        &self,
        options: SWDeliveryOptions<'a>,
        ticket: SWDeliveryTicket,
        operation: F,
        application_failed: fn(&T) -> bool,
        allow_inline: bool,
    ) -> SWDeliverySpawnResult<'a, T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let scheduler = match self.control.owned_scheduler() {
            Ok(scheduler) => scheduler,
            Err(reason) => {
                return Err(SWDeliverySpawnRejected {
                    reason,
                    operation,
                    ticket,
                    options,
                });
            }
        };
        let mut delivery = Some(ticket);
        scheduler
            .submit_payload_delivering(
                SubmitRequest {
                    control: &self.control,
                    class: self.class,
                    group: options.group,
                    options: options.spawn,
                    prerequisites: options.prerequisites,
                    policy: options.policy,
                    allow_inline,
                },
                operation,
                call_once::<F, T>,
                application_failed,
                &mut delivery,
            )
            .map_err(|rejected| SWDeliverySpawnRejected {
                reason: rejected.reason,
                operation: rejected.operation,
                ticket: delivery.expect("rejected admission retains its delivery ticket"),
                options,
            })
    }
}
