//! Set-owned root and descendant submission through runtime-associated lanes.

use super::{SWGroup, SWLane};
use crate::scheduler::work_set::WorkSetLease;
use crate::scheduler::{
    SWDependencyPolicy, SWDiscoveryError, SWDiscoveryPermit, SWSpawnError, SWSpawnOptions,
    SWSpawnRejected, SWSpawnResult, SWWorkSet, SubmitRequest, call_once, never_fail, result_is_err,
};
use crate::task::SWCompletion;

/// Owned submission choices for a work set root or discovery descendant.
/// Group membership and prerequisites commit with the job's admission.
#[derive(Clone, Copy)]
pub struct SWWorkOptions<'a> {
    pub spawn: SWSpawnOptions,
    pub group: Option<&'a SWGroup>,
    pub prerequisites: &'a [SWCompletion],
    pub dependency_policy: SWDependencyPolicy,
    pub allow_inline: bool,
}

impl Default for SWWorkOptions<'_> {
    fn default() -> Self {
        Self {
            spawn: SWSpawnOptions::default(),
            group: None,
            prerequisites: &[],
            dependency_policy: SWDependencyPolicy::SuccessOnly,
            allow_inline: false,
        }
    }
}

enum Source<'a> {
    Root(&'a SWWorkSet),
    Child(&'a SWDiscoveryPermit),
}

fn spawn<P, T>(
    source: Source<'_>,
    lane: &SWLane,
    work: SWWorkOptions<'_>,
    operation: P,
    run: fn(P) -> T,
    application_failed: fn(&T) -> bool,
) -> SWSpawnResult<T, P>
where
    P: Send + 'static,
    T: Send + 'static,
{
    let options = work.spawn;
    let reject = |reason, operation| SWSpawnRejected {
        reason,
        operation,
        options,
    };
    let scheduler = match lane.control.owned_scheduler() {
        Ok(scheduler) => scheduler,
        Err(reason) => return Err(reject(reason, operation)),
    };
    let identity = lane.control.identity();
    let lease_result = match source {
        Source::Root(set) => set.try_root_lease(identity),
        Source::Child(permit) => permit.try_child_lease(identity),
    };
    let lease: WorkSetLease = match lease_result {
        Ok(lease) => lease,
        Err(reason) => return Err(reject(map_discovery_error(reason), operation)),
    };
    scheduler.submit_payload_in_set(
        SubmitRequest {
            control: &lane.control,
            class: lane.class,
            group: work.group,
            options,
            prerequisites: work.prerequisites,
            policy: work.dependency_policy,
            allow_inline: work.allow_inline,
        },
        operation,
        run,
        application_failed,
        lease,
    )
}

fn map_discovery_error(error: SWDiscoveryError) -> SWSpawnError {
    match error {
        SWDiscoveryError::Full => SWSpawnError::Full,
        SWDiscoveryError::Closed | SWDiscoveryError::Cancelled => SWSpawnError::Closed,
    }
}

impl SWWorkSet {
    /// Admits a root job while the set is open. The accepted job keeps the set
    /// active until its capture, dependencies and cancellation settle.
    pub fn try_spawn<F, T>(
        &self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.try_spawn_with(
            lane,
            SWWorkOptions {
                spawn: options,
                ..SWWorkOptions::default()
            },
            operation,
        )
    }

    /// On runnable saturation, caller-eligible work may execute synchronously
    /// through the same set accounting and scheduler claim path.
    pub fn submit_or_run<F, T>(
        &self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.try_spawn_with(
            lane,
            SWWorkOptions {
                spawn: options,
                allow_inline: true,
                ..SWWorkOptions::default()
            },
            operation,
        )
    }

    /// Admits a root with optional group, prerequisites and caller eligibility.
    pub fn try_spawn_with<F, T>(
        &self,
        lane: &SWLane,
        work: SWWorkOptions<'_>,
        operation: F,
    ) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        spawn(
            Source::Root(self),
            lane,
            work,
            operation,
            call_once::<F, T>,
            never_fail::<T>,
        )
    }

    /// A typed `Err` stays in the result and marks success-only dependents failed.
    pub fn try_spawn_fallible_with<F, T, E>(
        &self,
        lane: &SWLane,
        work: SWWorkOptions<'_>,
        operation: F,
    ) -> SWSpawnResult<Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        spawn(
            Source::Root(self),
            lane,
            work,
            operation,
            call_once::<F, Result<T, E>>,
            result_is_err::<T, E>,
        )
    }
}

impl SWDiscoveryPermit {
    /// Admits a descendant while this permit is live, including after the set
    /// is sealed. A child is counted before this permit can be released.
    pub fn try_spawn<F, T>(
        &self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.try_spawn_with(
            lane,
            SWWorkOptions {
                spawn: options,
                ..SWWorkOptions::default()
            },
            operation,
        )
    }

    /// Admits a descendant with caller execution on runnable saturation when
    /// the job explicitly allows that context.
    pub fn submit_or_run<F, T>(
        &self,
        lane: &SWLane,
        options: SWSpawnOptions,
        operation: F,
    ) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.try_spawn_with(
            lane,
            SWWorkOptions {
                spawn: options,
                allow_inline: true,
                ..SWWorkOptions::default()
            },
            operation,
        )
    }

    /// Admits a descendant with optional group and prerequisite edges.
    pub fn try_spawn_with<F, T>(
        &self,
        lane: &SWLane,
        work: SWWorkOptions<'_>,
        operation: F,
    ) -> SWSpawnResult<T, F>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        spawn(
            Source::Child(self),
            lane,
            work,
            operation,
            call_once::<F, T>,
            never_fail::<T>,
        )
    }

    /// Admits a fallible descendant; the typed `Err` remains observable.
    pub fn try_spawn_fallible_with<F, T, E>(
        &self,
        lane: &SWLane,
        work: SWWorkOptions<'_>,
        operation: F,
    ) -> SWSpawnResult<Result<T, E>, F>
    where
        F: FnOnce() -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        spawn(
            Source::Child(self),
            lane,
            work,
            operation,
            call_once::<F, Result<T, E>>,
            result_is_err::<T, E>,
        )
    }
}
