//! Runtime-associated lanes and caller participation.

pub(crate) mod context;
mod delivery;
pub(crate) mod group;
mod owned;
mod scope;

pub use context::SWExecutionError;
pub use delivery::{SWDeliveryOptions, SWDeliverySpawnRejected, SWDeliverySpawnResult};
pub use group::SWGroup;
pub use scope::{SWBatchRejected, SWBranchOutcome, SWJoinRejected, SWLane, SWPanic};

pub(crate) use context::ContextGuard;
