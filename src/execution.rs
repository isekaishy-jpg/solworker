//! Runtime-associated lanes and caller participation.

mod batch;
pub(crate) mod context;
mod delivery;
pub(crate) mod group;
mod owned;
mod scope;
mod work_set;
pub use work_set::SWWorkOptions;
mod stage;
pub use stage::{SWStageOptions, SWStageRejected, SWStageResult};

pub use batch::{SWBatch, SWBatchError};
pub use context::SWExecutionError;
pub use delivery::{SWDeliveryOptions, SWDeliverySpawnRejected, SWDeliverySpawnResult};
pub use group::SWGroup;
pub use scope::{SWBatchRejected, SWBranchOutcome, SWJoinRejected, SWLane, SWPanic};

pub(crate) use context::ContextGuard;
