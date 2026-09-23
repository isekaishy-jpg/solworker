//! CPU execution, scoped parallel work, and owner-thread coordination.
//!
//! Three independently configured worker pools support borrowed joins, indexed
//! chunks, and worker/owner overlap with fallible startup and explicit shutdown.
//! Owned jobs support bounded admission, dependencies and retained groups.
//! Thread-bound owners publish results through explicit host phases.

#![deny(unsafe_op_in_unsafe_fn)]

mod backend;
mod execution;
mod external;
mod owner;
mod platform;
mod progress;
mod runtime;
mod scheduler;
mod task;

pub use execution::{
    SWBatchRejected, SWBranchOutcome, SWDeliveryOptions, SWDeliverySpawnRejected,
    SWDeliverySpawnResult, SWExecutionError, SWGroup, SWJoinRejected, SWLane, SWPanic,
    SWStageOptions, SWStageRejected, SWStageResult, SWWorkOptions,
};
pub use owner::{
    SWCancelResult, SWDelivery, SWDeliveryControl, SWDeliveryStatus, SWDeliveryTicket, SWOwner,
    SWOwnerControl, SWOwnerError, SWOwnerRejected, SWOwnerSender, SWPhase, SWPreparedDelivery,
    SWPumpBudget, SWPumpMode, SWPumpReport, SWReadyAccess,
};
pub use platform::SWWorkerSetupError;
pub use runtime::config::{
    SWConfigError, SWExecutionClass, SWRuntimeConfig, SWThreadPriority, SWWorkerConfig,
};
pub use runtime::{SWBuildError, SWRuntime, SWRuntimeBuilder, SWRuntimeState, SWShutdownError};
pub use scheduler::{
    SWByteLease, SWCallerEligibility, SWCapacityUsage, SWCost, SWDemand, SWDemandError,
    SWDemandSnapshot, SWDependencyPolicy, SWDiscoveryError, SWDiscoveryPermit, SWLimitError,
    SWLimits, SWOwnedConfigError, SWOwnedLimits, SWPriority, SWReservation, SWReservationError,
    SWSpawnError, SWSpawnOptions, SWSpawnRejected, SWSpawnResult, SWWorkSet, SWWorkSetProgress,
};
pub use task::{
    SWCompletion, SWOutcome, SWOutcomeRef, SWProducerControl, SWRetained, SWShared, SWTask,
    SWTaskStatus, SWThenRejected, SWThenResult, SWWaitError,
};
