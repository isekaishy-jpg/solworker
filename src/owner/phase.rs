//! Application-defined phase eligibility and bounded pumping.

use std::time::Duration;

/// A host-defined publication phase. The host chooses its values and advances
/// the owner explicitly; this crate does not impose game or frame event IDs.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SWPhase(pub u64);

/// Whether a pump sees newly eligible callbacks during the same call.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SWPumpMode {
    /// Recheck the route after each callback and local cleanup.
    #[default]
    Live,
    /// Freeze the eligible frontier at pump entry.
    Batch,
}

/// Host-chosen limit for one route pump. Limits are checked between entries,
/// including suppressed callbacks needing local cleanup. A single callback or
/// destructor may exceed the time limit. A zero count performs no work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWPumpBudget {
    pub max_entries: usize,
    pub max_duration: Option<Duration>,
    pub mode: SWPumpMode,
}

impl SWPumpBudget {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            max_duration: None,
            mode: SWPumpMode::Live,
        }
    }

    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.max_duration = Some(duration);
        self
    }

    pub fn with_mode(mut self, mode: SWPumpMode) -> Self {
        self.mode = mode;
        self
    }
}

/// Work completed by one bounded route pump.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SWPumpReport {
    pub invoked: usize,
    pub suppressed: usize,
}

impl SWPumpReport {
    pub fn processed(self) -> usize {
        self.invoked + self.suppressed
    }
}
