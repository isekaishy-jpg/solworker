//! Composition of lifetime, resource demand, delivery and declared capacity.

use super::{SWGroup, SWLane};
use crate::owner::SWDeliveryTicket;
use crate::scheduler::{
    SWCost, SWDependencyPolicy, SWDiscoveryPermit, SWPriority, SWReservation, SWSpawnError,
    SWSpawnOptions, SWWorkSet,
};
use crate::task::{SWCompletion, SWProducerControl, SWRetained, SWTask};

/// Optional policies for a resource stage. Ordinary `try_spawn` remains available
/// without pipeline prediction or declared bytes. Rejection preserves this bundle.
pub struct SWStageOptions<'a> {
    pub spawn: SWSpawnOptions,
    pub group: Option<&'a SWGroup>,
    pub prerequisites: &'a [SWCompletion],
    pub policy: SWDependencyPolicy,
    pub work_set: Option<&'a SWWorkSet>,
    pub discovery: Option<&'a SWDiscoveryPermit>,
    pub priority: Option<SWPriority>,
    pub reservation: Option<&'a SWReservation>,
    pub cost: SWCost,
    pub retained_bytes: usize,
    pub delivery: Option<SWDeliveryTicket>,
}

impl Default for SWStageOptions<'_> {
    fn default() -> Self {
        Self {
            spawn: SWSpawnOptions::default(),
            group: None,
            prerequisites: &[],
            policy: SWDependencyPolicy::SuccessOnly,
            work_set: None,
            discovery: None,
            priority: None,
            reservation: None,
            cost: SWCost::default(),
            retained_bytes: 0,
            delivery: None,
        }
    }
}

pub struct SWStageRejected<'a, F> {
    pub reason: SWSpawnError,
    pub operation: F,
    pub options: SWStageOptions<'a>,
}

impl<F> std::fmt::Debug for SWStageRejected<'_, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SWStageRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

pub type SWStageResult<'a, T, F> =
    Result<(SWTask<SWRetained<T>>, SWProducerControl), SWStageRejected<'a, F>>;

impl SWLane {
    /// Admits a resource stage without executing it on the caller. Declared
    /// retained bytes follow the returned value even after set/runtime drain.
    pub fn try_spawn_stage<'a, F, T>(
        &self,
        options: SWStageOptions<'a>,
        operation: F,
    ) -> SWStageResult<'a, T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_stage(options, operation, |_| false, false)
    }

    pub fn try_spawn_fallible_stage<'a, F, T, E>(
        &self,
        options: SWStageOptions<'a>,
        operation: F,
    ) -> SWStageResult<'a, Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_stage(options, operation, |result| result.is_err(), false)
    }

    /// Saturated caller execution still requires explicit caller eligibility.
    pub fn submit_or_run_stage<'a, F, T>(
        &self,
        options: SWStageOptions<'a>,
        operation: F,
    ) -> SWStageResult<'a, T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_stage(options, operation, |_| false, true)
    }

    pub fn submit_or_run_fallible_stage<'a, F, T, E>(
        &self,
        options: SWStageOptions<'a>,
        operation: F,
    ) -> SWStageResult<'a, Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_stage(options, operation, |result| result.is_err(), true)
    }

    fn submit_stage<'a, F, T>(
        &self,
        mut options: SWStageOptions<'a>,
        operation: F,
        failed: fn(&SWRetained<T>) -> bool,
        allow_inline: bool,
    ) -> SWStageResult<'a, T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let scheduler = match self.control.owned_scheduler() {
            Ok(scheduler) => scheduler,
            Err(reason) => {
                return Err(SWStageRejected {
                    reason,
                    operation,
                    options,
                });
            }
        };
        let prepared = scheduler.prepare_stage(self.control.identity(), &options);
        let (extras, bytes) = match prepared {
            Ok(prepared) => prepared,
            Err(reason) => {
                return Err(SWStageRejected {
                    reason,
                    operation,
                    options,
                });
            }
        };
        let request = crate::scheduler::SubmitRequest {
            control: &self.control,
            class: self.class,
            group: options.group,
            options: options.spawn,
            prerequisites: options.prerequisites,
            policy: options.policy,
            allow_inline,
        };
        let result = scheduler.submit_payload_accounted(
            request,
            (operation, bytes),
            run_stage::<F, T>,
            failed,
            &mut options.delivery,
            extras,
        );
        result.map_err(|rejected| {
            let (operation, _bytes) = rejected.operation;
            SWStageRejected {
                reason: rejected.reason,
                operation,
                options,
            }
        })
    }
}

fn run_stage<F, T>(
    (operation, bytes): (F, Option<crate::scheduler::reservation::SWByteLease>),
) -> SWRetained<T>
where
    F: FnOnce() -> T,
{
    SWRetained::new(operation(), bytes)
}
