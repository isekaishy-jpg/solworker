//! CPU execution, scoped parallel work, and owner-thread coordination.
//!
//! The runtime foundation provides three independently configured worker pools,
//! fallible startup, and explicit worker shutdown. Work submission, scoped work,
//! and owner-thread delivery are not available yet.

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

pub use platform::SWWorkerSetupError;
pub use runtime::config::{
    SWConfigError, SWExecutionClass, SWRuntimeConfig, SWThreadPriority, SWWorkerConfig,
};
pub use runtime::{SWBuildError, SWRuntime, SWRuntimeBuilder, SWRuntimeState};
