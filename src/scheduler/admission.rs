//! Bounded owned metadata and runnable admission.

use crate::execution::SWGroup;
use crate::runtime::config::SWExecutionClass;
use crate::task::{SWCompletion, SWProducerControl, SWTask};

/// An accepted task and explicit cancellation authority, or the untouched
/// operation with its admission failure.
pub type SWSpawnResult<T, F> = Result<(SWTask<T>, SWProducerControl), SWSpawnRejected<F>>;

/// Common execution and dependency constraints for an owned admission batch.
/// A supplied group stays open until its caller seals it.
#[derive(Clone, Copy)]
pub struct SWBatchSpawnOptions<'a> {
    pub spawn: SWSpawnOptions,
    pub group: Option<&'a SWGroup>,
    pub prerequisites: &'a [SWCompletion],
    pub dependency_policy: SWDependencyPolicy,
}

impl Default for SWBatchSpawnOptions<'_> {
    fn default() -> Self {
        Self {
            spawn: SWSpawnOptions::default(),
            group: None,
            prerequisites: &[],
            dependency_policy: SWDependencyPolicy::SuccessOnly,
        }
    }
}

impl std::fmt::Debug for SWBatchSpawnOptions<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SWBatchSpawnOptions")
            .field("spawn", &self.spawn)
            .field("group", &self.group.as_ref().map(|group| group.class()))
            .field("prerequisites", &self.prerequisites.len())
            .field("dependency_policy", &self.dependency_policy)
            .finish()
    }
}

/// Ordered accepted handles, or an accepted prefix and untouched input suffix.
pub type SWBatchSpawnResult<'a, T, F> =
    Result<Vec<(SWTask<T>, SWProducerControl)>, SWBatchSpawnRejected<'a, T, F>>;

/// Admission stopped at the first member in `remaining`. Earlier accepted work
/// may already be executing; retry only the returned suffix. Dropping this
/// receipt does not cancel accepted work.
#[must_use]
pub struct SWBatchSpawnRejected<'a, T: Send + 'static, F> {
    pub reason: SWSpawnError,
    pub accepted: Vec<(SWTask<T>, SWProducerControl)>,
    pub remaining: Vec<F>,
    pub options: SWBatchSpawnOptions<'a>,
}

impl<T: Send + 'static, F> std::fmt::Debug for SWBatchSpawnRejected<'_, T, F> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SWBatchSpawnRejected")
            .field("reason", &self.reason)
            .field("accepted", &self.accepted.len())
            .field("remaining", &self.remaining.len())
            .field("options", &self.options)
            .finish()
    }
}

/// Independent owned-work capacities. Array entries are Low, Mid, High.
/// `records` counts waiting, ready, handed, running, and finalizing jobs;
/// `edges` counts registered prerequisites through terminal detachment.
/// `runnable` counts ready and handed jobs, excluding running jobs, and reserves
/// slots for accepted immediate batch members while their attachment closes.
/// `handoff` counts wrappers offered to the backend until they return,
/// including a wrapper whose job is running. Zero edges disables dependent
/// submissions. Saturated caller execution can exceed only the runnable
/// limit; it still needs record and edge credits.
/// Internal reuse caches retain at most `records` job, group, and completion
/// allocations each, plus `edges` slots of detached subscription buffers.
/// Job and completion caches contain returned controls; groups retire when the
/// last public handle drops after sealing and reuse waits for settlement.
/// Outstanding handles prevent reuse; retained payloads keep their own lifetime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWOwnedLimits {
    pub(crate) records: usize,
    pub(crate) edges: usize,
    pub(crate) runnable: [usize; 3],
    pub(crate) handoff: [usize; 3],
}

impl SWOwnedLimits {
    pub fn new(
        records: usize,
        edges: usize,
        runnable: [usize; 3],
        handoff: [usize; 3],
    ) -> Result<Self, SWOwnedConfigError> {
        if records == 0 {
            return Err(SWOwnedConfigError::ZeroRecords);
        }
        for class in SWExecutionClass::ALL {
            if runnable[class.index()] == 0 {
                return Err(SWOwnedConfigError::ZeroRunnable(class));
            }
            if handoff[class.index()] == 0 {
                return Err(SWOwnedConfigError::ZeroHandoff(class));
            }
        }
        Ok(Self {
            records,
            edges,
            runnable,
            handoff,
        })
    }

    pub(crate) fn runnable_for(self, class: SWExecutionClass) -> usize {
        self.runnable[class.index()]
    }

    pub(crate) fn handoff_for(self, class: SWExecutionClass) -> usize {
        self.handoff[class.index()]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWOwnedConfigError {
    ZeroRecords,
    ZeroRunnable(SWExecutionClass),
    ZeroHandoff(SWExecutionClass),
}

impl std::fmt::Display for SWOwnedConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroRecords => formatter.write_str("owned record capacity must be nonzero"),
            Self::ZeroRunnable(class) => {
                write!(formatter, "{class:?} runnable capacity must be nonzero")
            }
            Self::ZeroHandoff(class) => {
                write!(formatter, "{class:?} handoff capacity must be nonzero")
            }
        }
    }
}

impl std::error::Error for SWOwnedConfigError {}

/// Why an owned job could not be admitted. Rejected inputs remain with caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWSpawnError {
    Full,
    TooLarge,
    Closed,
    Disabled,
    InvalidContext,
    InvalidGroup,
    /// The promised delivery belongs to another runtime or was already used.
    InvalidDelivery,
    InvalidPriority,
    InvalidReservation,
    Consumed,
}

/// Whether a retained job may run on an explicitly participating caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWCallerEligibility {
    WorkerOnly,
    CallerEligible,
}

/// Whether prerequisite failure suppresses a successor's closure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWDependencyPolicy {
    SuccessOnly,
    OutcomeAware,
}

/// Execution constraints for an owned job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWSpawnOptions {
    pub eligibility: SWCallerEligibility,
}

impl Default for SWSpawnOptions {
    fn default() -> Self {
        Self {
            eligibility: SWCallerEligibility::WorkerOnly,
        }
    }
}

/// An owned admission rejection, preserving the uninvoked closure and options.
pub struct SWSpawnRejected<F> {
    pub reason: SWSpawnError,
    pub operation: F,
    pub options: SWSpawnOptions,
}

impl<F> std::fmt::Debug for SWSpawnRejected<F> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SWSpawnRejected")
            .field("reason", &self.reason)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}
