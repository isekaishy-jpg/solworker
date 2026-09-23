//! Host-supplied worker allocation for the three execution classes.

use std::error::Error;
use std::fmt;

/// The capacity class of CPU work. Classes have separate workers and queues.
///
/// An execution class does not express resource urgency or an operating-system
/// thread priority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SWExecutionClass {
    Low,
    Mid,
    High,
}

impl SWExecutionClass {
    /// The fixed order of class entries in [`SWRuntimeConfig`].
    pub const ALL: [Self; 3] = [Self::Low, Self::Mid, Self::High];

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Low => 0,
            Self::Mid => 1,
            Self::High => 2,
        }
    }
}

/// A requested operating-system priority for workers in one class.
///
/// These values correspond to relative priorities -1, 0 and +1 on Windows.
/// Priority setup is fallible; a platform that cannot apply a request rejects
/// runtime construction rather than silently ignoring it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWThreadPriority {
    BelowNormal,
    Normal,
    AboveNormal,
}

/// Host configuration for the workers of one execution class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWWorkerConfig {
    worker_count: usize,
    requested_priority: Option<SWThreadPriority>,
}

impl SWWorkerConfig {
    /// Configures a class with the given count and no OS priority request.
    ///
    /// The count is checked when the complete runtime configuration is built.
    pub const fn new(worker_count: usize) -> Self {
        Self {
            worker_count,
            requested_priority: None,
        }
    }

    /// Requests an OS priority for every worker in this class.
    pub const fn with_priority(mut self, priority: SWThreadPriority) -> Self {
        self.requested_priority = Some(priority);
        self
    }

    pub const fn worker_count(self) -> usize {
        self.worker_count
    }

    pub const fn requested_priority(self) -> Option<SWThreadPriority> {
        self.requested_priority
    }
}

/// A validated, host-supplied aggregate worker budget and Low/Mid/High split.
///
/// Each class has at least one worker, so accepted asynchronous work always has
/// a worker route. The class counts may use less than the aggregate budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SWRuntimeConfig {
    worker_budget: usize,
    classes: [SWWorkerConfig; 3],
}

impl SWRuntimeConfig {
    /// Validates the aggregate budget and class split before worker startup.
    pub fn new(worker_budget: usize, classes: [SWWorkerConfig; 3]) -> Result<Self, SWConfigError> {
        if worker_budget == 0 {
            return Err(SWConfigError::ZeroBudget);
        }
        for (class, config) in SWExecutionClass::ALL.into_iter().zip(classes) {
            if config.worker_count == 0 {
                return Err(SWConfigError::ZeroWorkers(class));
            }
        }

        let configured = classes[0]
            .worker_count
            .checked_add(classes[1].worker_count)
            .and_then(|count| count.checked_add(classes[2].worker_count))
            .ok_or(SWConfigError::WorkerCountOverflow)?;
        if configured > worker_budget {
            return Err(SWConfigError::BudgetExceeded {
                budget: worker_budget,
                configured,
            });
        }

        Ok(Self {
            worker_budget,
            classes,
        })
    }

    pub const fn worker_budget(&self) -> usize {
        self.worker_budget
    }

    pub const fn workers_for(&self, class: SWExecutionClass) -> SWWorkerConfig {
        self.classes[class.index()]
    }
}

/// Invalid aggregate worker allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWConfigError {
    ZeroBudget,
    ZeroWorkers(SWExecutionClass),
    WorkerCountOverflow,
    BudgetExceeded { budget: usize, configured: usize },
}

impl fmt::Display for SWConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBudget => formatter.write_str("worker budget must be nonzero"),
            Self::ZeroWorkers(class) => write!(formatter, "{class:?} class has no workers"),
            Self::WorkerCountOverflow => formatter.write_str("worker counts overflow usize"),
            Self::BudgetExceeded { budget, configured } => write!(
                formatter,
                "worker budget is {budget}, but class counts require {configured}"
            ),
        }
    }
}

impl Error for SWConfigError {}
