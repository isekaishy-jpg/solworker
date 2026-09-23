//! CPU execution, scoped parallel work, and owner-thread coordination.
//!
//! Three independently configured worker pools support borrowed joins, indexed
//! chunks, and worker/owner overlap with fallible startup and explicit shutdown.
//! Owned jobs support bounded admission, dependencies and retained groups.
//! Owner-thread delivery is not available yet.

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
    SWBatchRejected, SWBranchOutcome, SWExecutionError, SWGroup, SWJoinRejected, SWLane, SWPanic,
};
pub use platform::SWWorkerSetupError;
pub use runtime::config::{
    SWConfigError, SWExecutionClass, SWRuntimeConfig, SWThreadPriority, SWWorkerConfig,
};
pub use runtime::{SWBuildError, SWRuntime, SWRuntimeBuilder, SWRuntimeState, SWShutdownError};
pub use scheduler::{
    SWCallerEligibility, SWDependencyPolicy, SWOwnedConfigError, SWOwnedLimits, SWSpawnError,
    SWSpawnOptions, SWSpawnRejected, SWSpawnResult,
};
pub use task::{
    SWCompletion, SWOutcome, SWOutcomeRef, SWProducerControl, SWShared, SWTask, SWTaskStatus,
    SWThenRejected, SWThenResult, SWWaitError,
};
