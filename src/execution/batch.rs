//! Reusable ownership of successive retained waves in one lane.

use super::{SWGroup, SWLane};
use crate::runtime::config::SWExecutionClass;
use crate::scheduler::SWSpawnError;

#[cfg(test)]
#[path = "../../tests/unit/batch.rs"]
mod tests;

/// Why a reusable batch could not start another wave.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWBatchError {
    /// The current wave has not been sealed and settled.
    Busy,
    /// Runtime admission rejected the new wave. `Closed` includes runtime
    /// shutdown; `Disabled` means owned scheduling was not configured.
    Admission(SWSpawnError),
}

/// A persistent owner of successive retained groups in one lane.
///
/// Construction does not admit work. Each successful [`begin`](Self::begin)
/// starts an open wave. The owner retains only its current wave; cloned group
/// handles and completion tokens continue to observe their original wave.
/// Dropping the owner neither seals nor waits for the current wave.
pub struct SWBatch {
    lane: SWLane,
    current: Option<SWGroup>,
}

impl SWBatch {
    pub(crate) fn new(lane: SWLane) -> Self {
        Self {
            lane,
            current: None,
        }
    }

    /// Returns the execution class selected when the owner was created.
    pub fn class(&self) -> SWExecutionClass {
        self.lane.class()
    }

    /// Returns the current wave, if one has been started.
    pub fn current(&self) -> Option<&SWGroup> {
        self.current.as_ref()
    }

    /// Starts a distinct open wave after the previous wave is sealed and
    /// settled. A `Busy` result takes precedence over runtime admission.
    /// Failed admission leaves the previous wave available through `current`.
    /// Uses the same admission rules as [`SWLane::group`], including accepted
    /// execution descendants during graceful shutdown. This does not wait for
    /// members or run their work on the caller.
    pub fn begin(&mut self) -> Result<&SWGroup, SWBatchError> {
        if let Some(group) = self.current.as_mut() {
            if !group.is_complete() {
                return Err(SWBatchError::Busy);
            }
            self.lane
                .control
                .owned_scheduler()
                .map_err(SWBatchError::Admission)?
                .renew_group(group)
                .map_err(SWBatchError::Admission)?;
        } else {
            self.current = Some(self.lane.group().map_err(SWBatchError::Admission)?);
        }
        Ok(self
            .current
            .as_ref()
            .expect("begin installed a current wave"))
    }
}

impl SWLane {
    /// Creates a lazy reusable owner for successive retained groups in this
    /// lane. The first wave is admitted by [`SWBatch::begin`].
    pub fn batch(&self) -> SWBatch {
        SWBatch::new(self.clone())
    }
}
