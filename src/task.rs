//! Owned task handles, shared observation, and producer cancellation authority.
//!
//! Unique results move once; shared handles retain immutable ownership. Dropping
//! an observer does not cancel its producer. Immediate results require no CPU job.

mod completion;
mod dependency;

use std::sync::Arc;

pub(crate) use completion::CompletionSink;
pub(crate) use completion::Subscription;
pub use completion::{
    SWCompletion, SWOutcome, SWOutcomeRef, SWShared, SWTask, SWTaskStatus, SWWaitError,
};
pub use dependency::{SWThenRejected, SWThenResult};

/// Explicit producer cancellation authority. Observing or dropping a task is
/// never a cancellation request. Cancellation suppresses work not yet claimed;
/// it does not forcibly stop an invocation already running.
#[derive(Clone)]
pub struct SWProducerControl {
    cancel: Arc<dyn Fn() + Send + Sync + 'static>,
}

impl SWProducerControl {
    pub(crate) fn new(cancel: Box<dyn Fn() + Send + Sync + 'static>) -> Self {
        Self {
            cancel: Arc::from(cancel),
        }
    }

    pub fn cancel(&self) {
        (self.cancel)();
    }
}
