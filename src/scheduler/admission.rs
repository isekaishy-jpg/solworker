//! Bounded owned metadata and runnable admission.

use crate::runtime::config::SWExecutionClass;
use crate::task::{SWProducerControl, SWTask};

/// An accepted task and explicit cancellation authority, or the untouched
/// operation with its admission failure.
pub type SWSpawnResult<T, F> = Result<(SWTask<T>, SWProducerControl), SWSpawnRejected<F>>;

/// Independent owned-work capacities. Array entries are Low, Mid, High.
/// `records` counts waiting, ready, handed, running, and finalizing jobs;
/// `edges` counts registered prerequisites through terminal detachment.
/// `runnable` counts ready and handed jobs, excluding running jobs.
/// `handoff` counts wrappers offered to the backend until they return,
/// including a wrapper whose job is running. Zero edges disables dependent
/// submissions. Saturated caller execution can exceed only the runnable
/// limit; it still needs record and edge credits.
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
