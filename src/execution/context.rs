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
