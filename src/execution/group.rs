//! Retained, classed batches and exact-group helping.

use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

use crate::execution::context::{SWExecutionError, current};
use crate::notification::NotifyDomain;
use crate::runtime::config::SWExecutionClass;
use crate::scheduler::OwnedScheduler;
use crate::task::{SWCompletion, SWTaskStatus};

#[cfg(test)]
#[path = "../../tests/unit/group.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/unit/group_membership.rs"]
mod membership_tests;

pub(crate) struct GroupInner {
    pub(crate) id: u64,
    pub(crate) class: SWExecutionClass,
    state: Mutex<GroupState>,
    changed: Condvar,
    completion: SWCompletion,
    public_handles: AtomicUsize,
    retirement: Weak<crate::scheduler::storage::GroupRetirement>,
    members: Mutex<BTreeMap<u64, crate::scheduler::storage::JobWeak>>,
}

struct GroupState {
    sealed: bool,
    pending: usize,
    generation: u64,
    failed: bool,
    publishing: bool,
}

impl GroupInner {
    #[cfg(feature = "diagnostics")]
    fn trace(&self, event: &'static str) {
        let runtime = self.completion.notification_runtime_id().unwrap_or(0);
        crate::diagnostics::record(event, runtime, self.id, 0);
    }

    pub(crate) fn new(id: u64, class: SWExecutionClass) -> Self {
        Self {
            id,
            class,
            state: Mutex::new(GroupState {
                sealed: false,
                pending: 0,
                generation: 0,
                failed: false,
                publishing: false,
            }),
            changed: Condvar::new(),
            completion: SWCompletion::pending(),
            public_handles: AtomicUsize::new(1),
            retirement: Weak::new(),
            members: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn set_retirement(
        &mut self,
        retirement: Weak<crate::scheduler::storage::GroupRetirement>,
    ) {
        self.retirement = retirement;
    }

    pub(crate) fn reset(&mut self, id: u64, class: SWExecutionClass) {
        self.id = id;
        self.class = class;
        *self.public_handles.get_mut() = 1;
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        debug_assert_eq!(state.pending, 0);
        *state = GroupState {
            sealed: false,
            pending: 0,
            generation: 0,
            failed: false,
            publishing: false,
        };
        // Retained tokens keep their old signal; only an exclusive signal resets.
        self.completion.reset();
        debug_assert!(
            self.members
                .get_mut()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty()
        );
    }

    pub(crate) fn completion(&self) -> SWCompletion {
        self.completion.clone()
    }

    pub(crate) fn set_notification_runtime(&self, runtime: u64) {
        self.completion.set_runtime_identity(runtime);
    }

    pub(crate) fn set_notification_source(&self, domain: &Arc<NotifyDomain>) {
        self.completion.set_notification_source(domain);
    }

    pub(crate) fn add(&self) -> bool {
        self.add_many(1)
    }

    pub(crate) fn is_open(&self) -> bool {
        !self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sealed
    }

    /// Sealing accepts the selected portion or rejects all of it at one cut.
    pub(crate) fn add_many(&self, count: usize) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.sealed {
            return false;
        }
        state.pending = state
            .pending
            .checked_add(count)
            .expect("group member count exhausted");
        state.generation = state.generation.wrapping_add(1);
        self.changed.notify_all();
        true
    }

    pub(crate) fn register_member(&self, job: crate::scheduler::storage::JobWeak) {
        let previous = self
            .members
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(job.id(), job);
        debug_assert!(previous.is_none(), "group member identities are unique");
    }

    pub(crate) fn register_members(&self, jobs: Vec<crate::scheduler::storage::JobWeak>) {
        let mut members = self
            .members
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for job in jobs {
            let previous = members.insert(job.id(), job);
            debug_assert!(previous.is_none(), "group member identities are unique");
        }
    }

    pub(crate) fn retire_member(&self, id: u64) {
        // Release the association after the membership guard; its recycler
        // ownership never becomes a lock-held cleanup boundary.
        let removed = self
            .members
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&id);
        drop(removed);
    }

    pub(crate) fn member_after(&self, after: u64) -> Option<crate::scheduler::storage::JobHandle> {
        let members = self
            .members
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // The cursor starts at its stable ID in logarithmic time. Expired weak
        // associations are skipped in one ordered pass, never by rescanning
        // the full live group for each next member. The returned control leaves
        // this guard before the scheduler claims or invokes anything.
        members
            .range((Excluded(after), Unbounded))
            .find_map(|(_, job)| job.upgrade())
    }

    pub(crate) fn finish(&self, status: Option<SWTaskStatus>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.pending -= 1;
        state.failed |= status.is_some_and(|status| !status.is_success());
        state.generation = state.generation.wrapping_add(1);
        self.changed.notify_all();
        let terminal = Self::terminal(&mut state);
        drop(state);
        self.publish(terminal);
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.completion.status().is_some()
    }

    fn release_public(group: &Arc<Self>) {
        if group.public_handles.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let sealed = group
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sealed;
        if sealed && let Some(retirement) = group.retirement.upgrade() {
            retirement.retire(Arc::clone(group));
        }
    }

    fn seal(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.sealed = true;
        state.generation = state.generation.wrapping_add(1);
        self.changed.notify_all();
        let terminal = Self::terminal(&mut state);
        drop(state);
        self.publish(terminal);
    }

    fn terminal(state: &mut GroupState) -> Option<SWTaskStatus> {
        if !state.sealed || state.pending != 0 || state.publishing {
            return None;
        }
        state.publishing = true;
        Some(if state.failed {
            SWTaskStatus::PrerequisiteFailed
        } else {
            SWTaskStatus::Succeeded
        })
    }

    fn publish(&self, terminal: Option<SWTaskStatus>) {
        if let Some(status) = terminal {
            #[cfg(feature = "diagnostics")]
            self.trace("group.publish.begin");
            // Dependencies can reenter the scheduler. Never publish under the
            // group mutex (or a scheduler admission lock).
            self.completion.publish_notifying(status, || {
                #[cfg(feature = "diagnostics")]
                self.trace("group.terminal_visible");
                self.notify_ready();
            });
            #[cfg(feature = "diagnostics")]
            self.trace("group.publish.end");
        }
    }

    pub(crate) fn notify_ready(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.generation = state.generation.wrapping_add(1);
        self.changed.notify_all();
    }

    fn generation(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .generation
    }

    fn wait_change(&self, generation: u64) {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.generation == generation && !self.is_complete() {
            #[cfg(feature = "diagnostics")]
            self.trace("group.condvar.begin");
            drop(
                self.changed
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner()),
            );
            #[cfg(feature = "diagnostics")]
            self.trace("group.condvar.end");
        }
    }
}

/// A retained wave of owned jobs in one execution class.
pub struct SWGroup {
    pub(crate) inner: Arc<GroupInner>,
    pub(crate) scheduler: Weak<OwnedScheduler>,
    pub(crate) runtime: u64,
}

impl Clone for SWGroup {
    fn clone(&self) -> Self {
        self.inner.public_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
            scheduler: self.scheduler.clone(),
            runtime: self.runtime,
        }
    }
}

impl Drop for SWGroup {
    fn drop(&mut self) {
        GroupInner::release_public(&self.inner);
    }
}

impl SWGroup {
    pub(crate) fn new(
        inner: Arc<GroupInner>,
        scheduler: Weak<OwnedScheduler>,
        runtime: u64,
    ) -> Self {
        Self {
            inner,
            scheduler,
            runtime,
        }
    }

    /// Reuses this wave's allocation only when no other group handle or
    /// scheduler member can still refer to its old identity. Retained
    /// completion tokens keep their terminal signal when the wave resets.
    pub(crate) fn try_reset(&mut self, id: u64) -> bool {
        let Some(inner) = Arc::get_mut(&mut self.inner) else {
            return false;
        };
        if *inner.public_handles.get_mut() != 1 || !inner.is_complete() {
            return false;
        }
        inner.reset(id, inner.class);
        true
    }

    pub fn class(&self) -> SWExecutionClass {
        self.inner.class
    }

    /// Closes membership. Completion follows every member's capture cleanup.
    pub fn seal(&self) {
        self.inner.seal();
    }

    pub fn is_complete(&self) -> bool {
        self.inner.is_complete()
    }

    /// Observes the sealed wave's all-settled boundary as a prerequisite.
    /// The token may be attached before sealing, but membership must eventually
    /// be sealed. Success means every accepted member succeeded (including an
    /// empty wave); otherwise the status is `PrerequisiteFailed`. Individual
    /// task outcomes retain the specific failure. Rejected submissions do not
    /// fail the group. This token does not propagate resource demand to members.
    /// Never make a member depend on its own group, directly or indirectly.
    pub fn completion(&self) -> SWCompletion {
        self.inner.completion()
    }

    /// Executes one ready member of this group when the caller is eligible.
    pub fn help_ready(&self) -> Result<bool, SWExecutionError> {
        self.check_context()?;
        let Some(scheduler) = self.scheduler.upgrade() else {
            return Ok(false);
        };
        Ok(scheduler.help_group(&self.inner))
    }

    /// Helps caller-eligible members of this exact group, then parks until it
    /// is sealed and settled. Do not wait on a group containing this invocation
    /// or any suspended enclosing invocation. Direct self-waits and their
    /// borrowed branches are rejected; arbitrary application cycles are not
    /// detected.
    /// A participating worker must not be the sole executor needed by any
    /// remaining worker-only member.
    pub fn wait_helping(&self) -> Result<(), SWExecutionError> {
        self.check_context()?;
        if current().is_some_and(|context| {
            context.runtime == self.runtime && context.group == Some(self.inner.id)
        }) {
            return Err(SWExecutionError::InvalidContext);
        }
        loop {
            if self.is_complete() {
                return Ok(());
            }
            let generation = self.inner.generation();
            if !self.help_ready()? {
                self.inner.wait_change(generation);
            }
        }
    }

    fn check_context(&self) -> Result<(), SWExecutionError> {
        if crate::notification::invocation_active() {
            return Err(SWExecutionError::InvalidContext);
        }
        match current() {
            Some(context)
                if context.runtime == self.runtime && context.class == self.inner.class =>
            {
                Ok(())
            }
            Some(_) => Err(SWExecutionError::InvalidContext),
            None => Ok(()),
        }
    }
}
