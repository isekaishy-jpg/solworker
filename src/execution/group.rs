//! Retained, classed batches and exact-group helping.

use std::sync::{Arc, Condvar, Mutex, Weak};

use crate::execution::context::{SWExecutionError, current};
use crate::runtime::config::SWExecutionClass;
use crate::scheduler::OwnedScheduler;
use crate::task::{SWCompletion, SWTaskStatus};

#[cfg(test)]
#[path = "../../tests/unit/group.rs"]
mod tests;

pub(crate) struct GroupInner {
    pub(crate) id: u64,
    pub(crate) class: SWExecutionClass,
    state: Mutex<GroupState>,
    changed: Condvar,
    completion: SWCompletion,
}

struct GroupState {
    sealed: bool,
    pending: usize,
    generation: u64,
    failed: bool,
    publishing: bool,
}

impl GroupInner {
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
        }
    }

    pub(crate) fn reset(&mut self, id: u64, class: SWExecutionClass) {
        self.id = id;
        self.class = class;
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
    }

    pub(crate) fn completion(&self) -> SWCompletion {
        self.completion.clone()
    }

    pub(crate) fn add(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.sealed {
            return false;
        }
        state.pending = state
            .pending
            .checked_add(1)
            .expect("group member count exhausted");
        state.generation = state.generation.wrapping_add(1);
        self.changed.notify_all();
        true
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
            // Dependencies can reenter the scheduler. Never publish under the
            // group mutex (or a scheduler admission lock).
            self.completion
                .publish_notifying(status, || self.notify_ready());
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
            drop(
                self.changed
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner()),
            );
        }
    }
}

/// A retained wave of owned jobs in one execution class.
#[derive(Clone)]
pub struct SWGroup {
    pub(crate) inner: Arc<GroupInner>,
    pub(crate) scheduler: Weak<OwnedScheduler>,
    pub(crate) runtime: u64,
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
        Ok(scheduler.help_group(self.inner.id))
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
