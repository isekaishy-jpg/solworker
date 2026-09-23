//! Typed continuations over already admitted prerequisites.

use super::{SWOutcome, SWProducerControl, SWShared, SWTask};
use crate::execution::SWLane;
use crate::scheduler::{
    SWDependencyPolicy, SWSpawnError, SWSpawnOptions, SWSpawnRejected, never_fail, result_is_err,
};

/// Rejected continuation inputs. Admission consumes neither the input handle
/// nor the operation, and the operation has not run.
pub struct SWThenRejected<I, F> {
    pub reason: SWSpawnError,
    pub input: I,
    pub operation: F,
    pub options: SWSpawnOptions,
}

impl<I, F> std::fmt::Debug for SWThenRejected<I, F> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SWThenRejected")
            .field("reason", &self.reason)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

fn run_unique_success<T, U, F>((mut task, operation): (SWTask<T>, F)) -> U
where
    T: Send + 'static,
    F: FnOnce(T) -> U,
{
    match task.try_take() {
        Some(SWOutcome::Success(value)) => operation(value),
        _ => unreachable!("success-only continuation invoked without a successful input"),
    }
}

fn run_unique_outcome<T, U, F>((mut task, operation): (SWTask<T>, F)) -> U
where
    T: Send + 'static,
    F: FnOnce(SWOutcome<T>) -> U,
{
    let outcome = task
        .try_take()
        .expect("outcome-aware continuation invoked before input completion");
    operation(outcome)
}

fn run_shared<T, U, F>((shared, operation): (SWShared<T>, F)) -> U
where
    T: Send + Sync + 'static,
    F: FnOnce(SWShared<T>) -> U,
{
    operation(shared)
}

/// An accepted continuation and its cancellation authority, or unchanged inputs.
pub type SWThenResult<T, I, F> = Result<(SWTask<T>, SWProducerControl), SWThenRejected<I, F>>;

impl<I, F> From<SWSpawnRejected<(I, F)>> for SWThenRejected<I, F> {
    fn from(rejected: SWSpawnRejected<(I, F)>) -> Self {
        let (input, operation) = rejected.operation;
        Self {
            reason: rejected.reason,
            input,
            operation,
            options: rejected.options,
        }
    }
}

impl<T: Send + 'static> SWTask<T> {
    /// Admits a successor transferring the unique input on acceptance.
    /// Suppresses the operation unless the prerequisite succeeded.
    /// Any returned value, including `Result::Err`, counts as success.
    /// Rejection returns the input and uninvoked operation. A previously taken
    /// result is rejected with [`SWSpawnError::Consumed`].
    pub fn try_then<U, F>(
        self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWThenResult<U, Self, F>
    where
        U: Send + 'static,
        F: FnOnce(T) -> U + Send + 'static,
    {
        self.then(
            lane,
            options,
            SWDependencyPolicy::SuccessOnly,
            operation,
            run_unique_success::<T, U, F>,
            never_fail::<U>,
        )
    }

    /// Admits a successor transferring the unique input on acceptance.
    /// Suppresses the operation unless the prerequisite succeeded.
    /// A returned `Err` marks application failure while retaining its typed error.
    /// Rejection returns the input and uninvoked operation. A previously taken
    /// result is rejected with [`SWSpawnError::Consumed`].
    pub fn try_then_fallible<U, E, F>(
        self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWThenResult<Result<U, E>, Self, F>
    where
        U: Send + 'static,
        E: Send + 'static,
        F: FnOnce(T) -> Result<U, E> + Send + 'static,
    {
        self.then(
            lane,
            options,
            SWDependencyPolicy::SuccessOnly,
            operation,
            run_unique_success::<T, Result<U, E>, F>,
            result_is_err::<U, E>,
        )
    }

    /// Admits a successor transferring the unique input on acceptance.
    /// Runs even if the prerequisite failed; inspect its outcome inside the operation.
    /// Any returned value, including `Result::Err`, counts as success.
    /// Rejection returns the input and uninvoked operation. A previously taken
    /// result is rejected with [`SWSpawnError::Consumed`].
    pub fn try_then_outcome<U, F>(
        self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWThenResult<U, Self, F>
    where
        U: Send + 'static,
        F: FnOnce(SWOutcome<T>) -> U + Send + 'static,
    {
        self.then(
            lane,
            options,
            SWDependencyPolicy::OutcomeAware,
            operation,
            run_unique_outcome::<T, U, F>,
            never_fail::<U>,
        )
    }

    /// Admits a successor transferring the unique input on acceptance.
    /// Runs even if the prerequisite failed; inspect its outcome inside the operation.
    /// A returned `Err` marks application failure while retaining its typed error.
    /// Rejection returns the input and uninvoked operation. A previously taken
    /// result is rejected with [`SWSpawnError::Consumed`].
    pub fn try_then_outcome_fallible<U, E, F>(
        self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWThenResult<Result<U, E>, Self, F>
    where
        U: Send + 'static,
        E: Send + 'static,
        F: FnOnce(SWOutcome<T>) -> Result<U, E> + Send + 'static,
    {
        self.then(
            lane,
            options,
            SWDependencyPolicy::OutcomeAware,
            operation,
            run_unique_outcome::<T, Result<U, E>, F>,
            result_is_err::<U, E>,
        )
    }

    fn then<U, F>(
        self,
        lane: &SWLane,
        options: SWSpawnOptions,
        policy: SWDependencyPolicy,
        operation: F,
        run: fn((Self, F)) -> U,
        application_failed: fn(&U) -> bool,
    ) -> SWThenResult<U, Self, F>
    where
        U: Send + 'static,
        F: Send + 'static,
    {
        if !self.continuation_input_available() {
            return Err(SWThenRejected {
                reason: SWSpawnError::Consumed,
                input: self,
                operation,
                options,
            });
        }
        let prerequisite = self.completion();
        lane.try_spawn_with_payload(
            options,
            &[prerequisite],
            policy,
            (self, operation),
            run,
            application_failed,
        )
        .map_err(SWThenRejected::from)
    }
}
impl<T: Send + Sync + 'static> SWShared<T> {
    /// Admits a successor with shared immutable input.
    /// Suppresses the operation unless the prerequisite succeeded.
    /// Any returned value, including `Result::Err`, counts as success.
    /// Rejection returns the input and uninvoked operation. A previously taken
    /// result is rejected with [`SWSpawnError::Consumed`].
    pub fn try_then<U, F>(
        self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWThenResult<U, Self, F>
    where
        U: Send + 'static,
        F: FnOnce(SWShared<T>) -> U + Send + 'static,
    {
        self.then(
            lane,
            options,
            SWDependencyPolicy::SuccessOnly,
            operation,
            run_shared::<T, U, F>,
            never_fail::<U>,
        )
    }

    /// Admits a successor with shared immutable input.
    /// Suppresses the operation unless the prerequisite succeeded.
    /// A returned `Err` marks application failure while retaining its typed error.
    /// Rejection returns the input and uninvoked operation. A previously taken
    /// result is rejected with [`SWSpawnError::Consumed`].
    pub fn try_then_fallible<U, E, F>(
        self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWThenResult<Result<U, E>, Self, F>
    where
        U: Send + 'static,
        E: Send + 'static,
        F: FnOnce(SWShared<T>) -> Result<U, E> + Send + 'static,
    {
        self.then(
            lane,
            options,
            SWDependencyPolicy::SuccessOnly,
            operation,
            run_shared::<T, Result<U, E>, F>,
            result_is_err::<U, E>,
        )
    }

    /// Admits a successor with shared immutable input.
    /// Runs even if the prerequisite failed; inspect its outcome inside the operation.
    /// Any returned value, including `Result::Err`, counts as success.
    /// Rejection returns the input and uninvoked operation. A previously taken
    /// result is rejected with [`SWSpawnError::Consumed`].
    pub fn try_then_outcome<U, F>(
        self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWThenResult<U, Self, F>
    where
        U: Send + 'static,
        F: FnOnce(SWShared<T>) -> U + Send + 'static,
    {
        self.then(
            lane,
            options,
            SWDependencyPolicy::OutcomeAware,
            operation,
            run_shared::<T, U, F>,
            never_fail::<U>,
        )
    }

    /// Admits a successor with shared immutable input.
    /// Runs even if the prerequisite failed; inspect its outcome inside the operation.
    /// A returned `Err` marks application failure while retaining its typed error.
    /// Rejection returns the input and uninvoked operation. A previously taken
    /// result is rejected with [`SWSpawnError::Consumed`].
    pub fn try_then_outcome_fallible<U, E, F>(
        self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWThenResult<Result<U, E>, Self, F>
    where
        U: Send + 'static,
        E: Send + 'static,
        F: FnOnce(SWShared<T>) -> Result<U, E> + Send + 'static,
    {
        self.then(
            lane,
            options,
            SWDependencyPolicy::OutcomeAware,
            operation,
            run_shared::<T, Result<U, E>, F>,
            result_is_err::<U, E>,
        )
    }

    fn then<U, F>(
        self,
        lane: &SWLane,
        options: SWSpawnOptions,
        policy: SWDependencyPolicy,
        operation: F,
        run: fn((Self, F)) -> U,
        application_failed: fn(&U) -> bool,
    ) -> SWThenResult<U, Self, F>
    where
        U: Send + 'static,
        F: Send + 'static,
    {
        if !self.continuation_input_available() {
            return Err(SWThenRejected {
                reason: SWSpawnError::Consumed,
                input: self,
                operation,
                options,
            });
        }
        let prerequisite = self.completion();
        lane.try_spawn_with_payload(
            options,
            &[prerequisite],
            policy,
            (self, operation),
            run,
            application_failed,
        )
        .map_err(SWThenRejected::from)
    }
}
