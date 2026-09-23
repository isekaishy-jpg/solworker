//! Participation context shared by workers and helping callers.

use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::runtime::config::SWExecutionClass;

/// Why a scoped invocation could not be admitted. Rejection invokes none of
/// the submitted closures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SWExecutionError {
    /// The runtime has closed admission for new root invocations.
    Closed,
    /// This thread is participating in another lane/runtime, or would wait on
    /// its own retained group.
    InvalidContext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionContext {
    pub(crate) runtime: u64,
    pub(crate) class: SWExecutionClass,
    pub(crate) group: Option<u64>,
}

thread_local! {
    static CURRENT: Cell<Option<ExecutionContext>> = const { Cell::new(None) };
    static LIVE_OWNERS: Cell<usize> = const { Cell::new(0) };
    static OWNER_CALLBACKS: Cell<usize> = const { Cell::new(0) };
    static CONTROL_CALLBACKS: Cell<usize> = const { Cell::new(0) };
}

pub(crate) fn register_live_owner() {
    LIVE_OWNERS.with(|count| {
        count.set(
            count
                .get()
                .checked_add(1)
                .expect("live owner count overflow"),
        )
    });
}

pub(crate) fn unregister_live_owner() {
    LIVE_OWNERS.with(|count| count.set(count.get().checked_sub(1).expect("registered owner")));
}

pub(crate) fn owner_callback_active() -> bool {
    OWNER_CALLBACKS.with(|count| count.get() != 0)
}

/// Passive waiting never services an owner's phase or legal capture cleanup.
pub(crate) fn passive_wait_forbidden() -> bool {
    current().is_some()
        || owner_callback_active()
        || control_callback_active()
        || LIVE_OWNERS.with(|count| count.get() != 0)
}

pub(crate) fn control_callback_active() -> bool {
    CONTROL_CALLBACKS.with(|count| count.get() != 0)
}

/// Provider hooks and external settlement retain accounting until they return.
/// Reentrant passive waiting could therefore wait for the caller itself.
pub(crate) struct ControlCallbackGuard(PhantomData<Rc<()>>);

impl ControlCallbackGuard {
    pub(crate) fn enter() -> Self {
        CONTROL_CALLBACKS.with(|count| {
            count.set(
                count
                    .get()
                    .checked_add(1)
                    .expect("control callback depth overflow"),
            )
        });
        Self(PhantomData)
    }
}

impl Drop for ControlCallbackGuard {
    fn drop(&mut self) {
        CONTROL_CALLBACKS.with(|count| count.set(count.get() - 1));
    }
}

pub(crate) struct OwnerCallbackGuard {
    not_send: PhantomData<Rc<()>>,
}

impl OwnerCallbackGuard {
    pub(crate) fn enter() -> Self {
        OWNER_CALLBACKS.with(|count| {
            count.set(
                count
                    .get()
                    .checked_add(1)
                    .expect("owner callback depth overflow"),
            )
        });
        Self {
            not_send: PhantomData,
        }
    }
}

impl Drop for OwnerCallbackGuard {
    fn drop(&mut self) {
        OWNER_CALLBACKS.with(|count| count.set(count.get() - 1));
    }
}

pub(crate) fn current() -> Option<ExecutionContext> {
    CURRENT.with(Cell::get)
}

/// Restores an enclosing context on return or unwind.
pub(crate) struct ContextGuard {
    previous: Option<ExecutionContext>,
    not_send: PhantomData<Rc<()>>,
}

impl ContextGuard {
    pub(crate) fn enter(runtime: u64, class: SWExecutionClass) -> Self {
        let previous = CURRENT.with(|slot| {
            let group = slot
                .get()
                .filter(|context| context.runtime == runtime && context.class == class)
                .and_then(|context| context.group);
            slot.replace(Some(ExecutionContext {
                runtime,
                class,
                group,
            }))
        });
        Self {
            previous,
            not_send: PhantomData,
        }
    }

    /// An owned claim replaces any enclosing group identity. A nested scoped
    /// call uses `enter` and preserves this marker for self-wait detection.
    pub(crate) fn enter_owned(runtime: u64, class: SWExecutionClass, group: Option<u64>) -> Self {
        let previous = CURRENT.with(|slot| {
            slot.replace(Some(ExecutionContext {
                runtime,
                class,
                group,
            }))
        });
        Self {
            previous,
            not_send: PhantomData,
        }
    }
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        CURRENT.with(|slot| slot.set(self.previous));
    }
}
