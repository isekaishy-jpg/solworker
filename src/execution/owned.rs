//! Owned admission through runtime-associated lanes.

use super::{SWGroup, SWLane};
use crate::scheduler::{
    SWDependencyPolicy, SWSpawnError, SWSpawnOptions, SWSpawnRejected, SWSpawnResult,
    SubmitRequest, call_once, never_fail, result_is_err,
};
use crate::task::SWCompletion;

struct OwnedRoute<'a> {
    options: SWSpawnOptions,
    group: Option<&'a SWGroup>,
    prerequisites: &'a [SWCompletion],
    policy: SWDependencyPolicy,
    allow_inline: bool,
}

impl OwnedRoute<'_> {
    fn new(options: SWSpawnOptions) -> Self {
        Self {
            options,
            group: None,
            prerequisites: &[],
            policy: SWDependencyPolicy::SuccessOnly,
            allow_inline: false,
        }
    }
}

impl SWLane {
    /// Creates an open retained group in this lane. Seal it before waiting for
    /// completion; dropping an observer never cancels its accepted members.
    pub fn group(&self) -> Result<SWGroup, SWSpawnError> {
        self.control.owned_scheduler()?.group(self.class)
    }

    /// Admits a job without waiting for capacity or executing it on this caller.
    /// Any returned value, including `Result::Err`, counts as success.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn try_spawn<F, T>(&self, options: SWSpawnOptions, operation: F) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute::new(options),
            operation,
            call_once::<F, T>,
            never_fail::<T>,
        )
    }

    /// Admits a job without waiting for capacity or executing it on this caller.
    /// Membership is admitted to an open group from this lane.
    /// Any returned value, including `Result::Err`, counts as success.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn try_spawn_in<F, T>(
        &self,
        group: &SWGroup,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                group: Some(group),
                ..OwnedRoute::new(options)
            },
            operation,
            call_once::<F, T>,
            never_fail::<T>,
        )
    }

    /// Admits a job without waiting for capacity or executing it on this caller.
    /// A returned `Err` marks application failure for success-only dependents;
    /// the typed error stays in the result. Prerequisite policy is independent.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn try_spawn_fallible<F, T, E>(
        &self,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWSpawnResult<Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute::new(options),
            operation,
            call_once::<F, Result<T, E>>,
            result_is_err::<T, E>,
        )
    }

    /// Admits a job without waiting for capacity or executing it on this caller.
    /// Membership is admitted to an open group from this lane.
    /// A returned `Err` marks application failure for success-only dependents;
    /// the typed error stays in the result. Prerequisite policy is independent.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn try_spawn_fallible_in<F, T, E>(
        &self,
        group: &SWGroup,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWSpawnResult<Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                group: Some(group),
                ..OwnedRoute::new(options)
            },
            operation,
            call_once::<F, Result<T, E>>,
            result_is_err::<T, E>,
        )
    }

    /// May execute caller-eligible work synchronously on runnable saturation.
    /// Record/edge limits still apply. Handoff pressure alone never runs it inline.
    /// Any returned value, including `Result::Err`, counts as success.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn submit_or_run<F, T>(&self, options: SWSpawnOptions, operation: F) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                allow_inline: true,
                ..OwnedRoute::new(options)
            },
            operation,
            call_once::<F, T>,
            never_fail::<T>,
        )
    }

    /// May execute caller-eligible work synchronously on runnable saturation.
    /// Record/edge limits still apply. Handoff pressure alone never runs it inline.
    /// Membership is admitted to an open group from this lane.
    /// Any returned value, including `Result::Err`, counts as success.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn submit_or_run_in<F, T>(
        &self,
        group: &SWGroup,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                group: Some(group),
                allow_inline: true,
                ..OwnedRoute::new(options)
            },
            operation,
            call_once::<F, T>,
            never_fail::<T>,
        )
    }

    /// May execute caller-eligible work synchronously on runnable saturation.
    /// Record/edge limits still apply. Handoff pressure alone never runs it inline.
    /// A returned `Err` marks application failure for success-only dependents;
    /// the typed error stays in the result. Prerequisite policy is independent.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn submit_or_run_fallible<F, T, E>(
        &self,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWSpawnResult<Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                allow_inline: true,
                ..OwnedRoute::new(options)
            },
            operation,
            call_once::<F, Result<T, E>>,
            result_is_err::<T, E>,
        )
    }

    /// May execute caller-eligible work synchronously on runnable saturation.
    /// Record/edge limits still apply. Handoff pressure alone never runs it inline.
    /// Membership is admitted to an open group from this lane.
    /// A returned `Err` marks application failure for success-only dependents;
    /// the typed error stays in the result. Prerequisite policy is independent.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn submit_or_run_fallible_in<F, T, E>(
        &self,
        group: &SWGroup,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWSpawnResult<Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                group: Some(group),
                allow_inline: true,
                ..OwnedRoute::new(options)
            },
            operation,
            call_once::<F, Result<T, E>>,
            result_is_err::<T, E>,
        )
    }

    /// Admits a successor without occupying a worker while prerequisites wait.
    /// Outcome-aware work must inspect its captured prerequisite outcomes.
    /// Any returned value, including `Result::Err`, counts as success.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn try_spawn_after<F, T>(
        &self,
        options: SWSpawnOptions,
        prerequisites: &[SWCompletion],
        policy: SWDependencyPolicy,
        operation: F,
    ) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                prerequisites,
                policy,
                ..OwnedRoute::new(options)
            },
            operation,
            call_once::<F, T>,
            never_fail::<T>,
        )
    }

    /// Admits a successor without occupying a worker while prerequisites wait.
    /// Outcome-aware work must inspect its captured prerequisite outcomes.
    /// Membership is admitted to an open group from this lane.
    /// Any returned value, including `Result::Err`, counts as success.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn try_spawn_after_in<F, T>(
        &self,
        group: &SWGroup,
        options: SWSpawnOptions,
        prerequisites: &[SWCompletion],
        policy: SWDependencyPolicy,
        operation: F,
    ) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                group: Some(group),
                prerequisites,
                policy,
                ..OwnedRoute::new(options)
            },
            operation,
            call_once::<F, T>,
            never_fail::<T>,
        )
    }

    /// Admits a successor without occupying a worker while prerequisites wait.
    /// Outcome-aware work must inspect its captured prerequisite outcomes.
    /// A returned `Err` marks application failure for success-only dependents;
    /// the typed error stays in the result. Prerequisite policy is independent.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn try_spawn_after_fallible<F, T, E>(
        &self,
        options: SWSpawnOptions,
        prerequisites: &[SWCompletion],
        policy: SWDependencyPolicy,
        operation: F,
    ) -> SWSpawnResult<Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                prerequisites,
                policy,
                ..OwnedRoute::new(options)
            },
            operation,
            call_once::<F, Result<T, E>>,
            result_is_err::<T, E>,
        )
    }

    /// Admits a successor without occupying a worker while prerequisites wait.
    /// Outcome-aware work must inspect its captured prerequisite outcomes.
    /// Membership is admitted to an open group from this lane.
    /// A returned `Err` marks application failure for success-only dependents;
    /// the typed error stays in the result. Prerequisite policy is independent.
    /// Rejection returns the uninvoked operation. Dropping observers never cancels work.
    pub fn try_spawn_after_fallible_in<F, T, E>(
        &self,
        group: &SWGroup,
        options: SWSpawnOptions,
        prerequisites: &[SWCompletion],
        policy: SWDependencyPolicy,
        operation: F,
    ) -> SWSpawnResult<Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                group: Some(group),
                prerequisites,
                policy,
                ..OwnedRoute::new(options)
            },
            operation,
            call_once::<F, Result<T, E>>,
            result_is_err::<T, E>,
        )
    }

    fn submit_owned<P, T>(
        &self,
        route: OwnedRoute<'_>,
        payload: P,
        run: fn(P) -> T,
        application_failed: fn(&T) -> bool,
    ) -> SWSpawnResult<T, P>
    where
        P: Send + 'static,
        T: Send + 'static,
    {
        let scheduler = match self.control.owned_scheduler() {
            Ok(scheduler) => scheduler,
            Err(reason) => {
                return Err(SWSpawnRejected {
                    reason,
                    operation: payload,
                    options: route.options,
                });
            }
        };
        scheduler.submit_payload(
            SubmitRequest {
                control: &self.control,
                class: self.class,
                group: route.group,
                options: route.options,
                prerequisites: route.prerequisites,
                policy: route.policy,
                allow_inline: route.allow_inline,
            },
            payload,
            run,
            application_failed,
        )
    }

    pub(crate) fn try_spawn_with_payload<P, T>(
        &self,
        options: SWSpawnOptions,
        prerequisites: &[SWCompletion],
        policy: SWDependencyPolicy,
        payload: P,
        run: fn(P) -> T,
        application_failed: fn(&T) -> bool,
    ) -> SWSpawnResult<T, P>
    where
        P: Send + 'static,
        T: Send + 'static,
    {
        self.submit_owned(
            OwnedRoute {
                prerequisites,
                policy,
                ..OwnedRoute::new(options)
            },
            payload,
            run,
            application_failed,
        )
    }
}
