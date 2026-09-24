//! CPU execution, scoped parallel work, and owner-thread coordination.
//!
//! Three independently configured worker pools support borrowed joins, indexed
//! chunks, and worker/owner overlap with fallible startup and explicit shutdown.
//! Owned jobs support bounded admission, dependencies and retained groups.
//! Thread-bound owners publish results through explicit host phases.
//! External providers fulfill logical outcomes separately from unsafe adapter
//! acknowledgement of physical storage release.
//! Explicit root closure lets hosts service accepted work before joining;
//! passive progress snapshots and deadline-based wakes report remaining work.
//!
//! The source checkout includes an application usage guide at
//! `pubdocs/README.md` and runnable integration examples under `examples/`.
//!
//! Runtime-discarded panic payloads are destroyed inside containment. If that
//! destruction panics, its new payload is deliberately retained to avoid an
//! unbounded disposal/unwind chain. Caller-owned panic payloads stay caller-owned.

#![deny(unsafe_op_in_unsafe_fn)]

mod backend;
mod cleanup;
mod execution;
mod external;
mod owner;
mod platform;
mod progress;
mod runtime;
mod scheduler;
mod task;

pub use execution::{
    SWBatch, SWBatchError, SWBatchRejected, SWBranchOutcome, SWDeliveryOptions,
    SWDeliverySpawnRejected, SWDeliverySpawnResult, SWExecutionError, SWGroup, SWJoinRejected,
    SWLane, SWPanic, SWStageOptions, SWStageRejected, SWStageResult, SWWorkOptions,
};
pub use external::{
    SWExternalAccess, SWExternalAccessOptions, SWExternalAccessRejected,
    SWExternalActivationRejected, SWExternalOptions, SWExternalPrepared, SWExternalProgress,
    SWExternalRejected, SWExternalResult, SWProducer,
};
pub use owner::{
    SWCancelResult, SWDelivery, SWDeliveryControl, SWDeliveryStatus, SWDeliveryTicket, SWOwner,
    SWOwnerControl, SWOwnerError, SWOwnerRejected, SWOwnerSender, SWPhase, SWPreparedDelivery,
    SWPumpBudget, SWPumpMode, SWPumpReport, SWReadyAccess,
};
pub use platform::SWWorkerSetupError;
pub use progress::{
    SWOwnerProgress, SWProgress, SWProgressWait, SWProgressWaitError, SWSchedulerProgress,
    SWWorkSetsProgress,
};
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
