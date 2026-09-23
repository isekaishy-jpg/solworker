//! Synchronous borrowed work on a runtime lane.

use std::any::Any;
use std::fmt;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use crate::execution::{ContextGuard, SWExecutionError, context};
use crate::runtime::RuntimeControl;
use crate::runtime::config::SWExecutionClass;

/// A caught application panic. Its payload remains owned by the caller.
pub struct SWPanic(Box<dyn Any + Send>);

impl SWPanic {
    pub fn into_payload(self) -> Box<dyn Any + Send> {
        self.0
    }
}

impl fmt::Debug for SWPanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SWPanic(..)")
    }
}

/// A branch's value or caught panic. Every accepted branch settles before
/// either result is returned.
pub type SWBranchOutcome<T> = Result<T, SWPanic>;

/// A rejected join returns both closures uninvoked to their owner.
pub struct SWJoinRejected<L, R> {
    pub reason: SWExecutionError,
    pub left: L,
    pub right: R,
}

impl<L, R> fmt::Debug for SWJoinRejected<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SWJoinRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

/// A rejected batch returns its operation uninvoked. Its borrowed slice stays
/// with the caller throughout admission.
pub struct SWBatchRejected<F> {
    pub reason: SWExecutionError,
    pub operation: F,
}

impl<F> fmt::Debug for SWBatchRejected<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SWBatchRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

/// A reusable handle selecting one runtime's dedicated worker class.
/// Cloning a lane retains its admission control, but cannot keep the runtime's
/// workers alive after the runtime is dropped.
/// Borrowed worker branches and chunk operations may run on a pool worker or
/// the assisting caller. Caller threads have not run the worker setup hook, so
/// those operations must be valid in either execution context.
#[derive(Clone)]
pub struct SWLane {
    pub(super) control: Arc<RuntimeControl>,
    pub(super) class: SWExecutionClass,
}

impl SWLane {
    pub(crate) fn new(control: Arc<RuntimeControl>, class: SWExecutionClass) -> Self {
        Self { control, class }
    }

    pub fn class(&self) -> SWExecutionClass {
        self.class
    }

    /// Runs two borrowed, transferable branches. Both branches finish and
    /// their captures are destroyed before return, even if either panics.
    /// Same-lane nesting is supported; blocking calls to a different lane or
    /// runtime reject before running either closure.
    pub fn join<L, R, LO, RO>(
        &self,
        left: L,
        right: R,
    ) -> Result<(SWBranchOutcome<LO>, SWBranchOutcome<RO>), SWJoinRejected<L, R>>
    where
        L: FnOnce() -> LO + Send,
        R: FnOnce() -> RO + Send,
        LO: Send,
        RO: Send,
    {
        let lease = match self.control.acquire(self.class) {
            Ok(lease) => lease,
            Err(reason) => {
                return Err(SWJoinRejected {
                    reason,
                    left,
                    right,
                });
            }
        };
        let runtime = self.control.identity();
        let class = self.class;
        let group = inherited_group(runtime, class);
        let _caller = ContextGuard::enter(runtime, class);
        Ok(lease.pool().with_context(|| {
            lease.pool().join(
                move || {
                    let _branch = ContextGuard::enter_owned(runtime, class, group);
                    catch_branch(left)
                },
                move || {
                    let _branch = ContextGuard::enter_owned(runtime, class, group);
                    catch_branch(right)
                },
            )
        }))
    }

    /// Advertises the transferable worker branch before running `owner` on
    /// this calling thread. The owner closure and result need not be Send.
    /// With no foreground slot, execution is serial in owner-then-worker order.
    /// The branches must also be valid when run serially: `owner` must not
    /// wait for the unfinished worker branch through a channel, lock, or other
    /// side channel. Advertisement does not guarantee concurrent execution.
    pub fn join_with_owner<W, O, WO, OO>(
        &self,
        worker: W,
        owner: O,
    ) -> Result<(SWBranchOutcome<WO>, SWBranchOutcome<OO>), SWJoinRejected<W, O>>
    where
        W: FnOnce() -> WO + Send,
        WO: Send,
        O: FnOnce() -> OO,
    {
        let lease = match self.control.acquire(self.class) {
            Ok(lease) => lease,
            Err(reason) => {
                return Err(SWJoinRejected {
                    reason,
                    left: worker,
                    right: owner,
                });
            }
        };
        let runtime = self.control.identity();
        let class = self.class;
        let group = inherited_group(runtime, class);
        let _caller = ContextGuard::enter(runtime, class);
        Ok(lease.pool().with_context(|| {
            lease.pool().join_with_owner(
                move || {
                    let _branch = ContextGuard::enter_owned(runtime, class, group);
                    catch_branch(worker)
                },
                || catch_branch(owner),
            )
        }))
    }

    /// Applies `operation` once to each disjoint mutable chunk, supplying its
    /// stable starting index. All chunks settle before return. The first caught
    /// panic is returned after the remaining chunks finish; its identity is
    /// nondeterministic if multiple chunks panic.
    pub fn for_each_chunk<T, F>(
        &self,
        input: &mut [T],
        chunk_size: NonZeroUsize,
        operation: F,
    ) -> Result<SWBranchOutcome<()>, SWBatchRejected<F>>
    where
        T: Send,
        F: Fn(usize, &mut [T]) + Sync,
    {
        let lease = match self.control.acquire(self.class) {
            Ok(lease) => lease,
            Err(reason) => return Err(SWBatchRejected { reason, operation }),
        };
        let runtime = self.control.identity();
        let class = self.class;
        let group = inherited_group(runtime, class);
        let _caller = ContextGuard::enter(runtime, class);
        let size = chunk_size.get();
        let count = input.len().div_ceil(size);
        let shared = MutableChunks {
            ptr: input.as_mut_ptr(),
            len: input.len(),
            size,
        };
        let result = run_chunks(lease.pool(), runtime, class, group, count, |index| {
            // SAFETY: invoke_indexed calls each index in 0..count once and
            // joins every call. Distinct indices address disjoint chunks; the
            // input is exclusively borrowed until run_chunks returns.
            let (ptr, len) = shared.raw_chunk(index);
            let chunk = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
            operation(index * size, chunk);
        });
        drop(operation);
        Ok(result)
    }

    /// Applies `operation` to immutable indexed chunks. Concurrent operations
    /// share the input, and all invocations settle before return.
    pub fn for_each_read_chunk<T, F>(
        &self,
        input: &[T],
        chunk_size: NonZeroUsize,
        operation: F,
    ) -> Result<SWBranchOutcome<()>, SWBatchRejected<F>>
    where
        T: Sync,
        F: Fn(usize, &[T]) + Sync,
    {
        let lease = match self.control.acquire(self.class) {
            Ok(lease) => lease,
            Err(reason) => return Err(SWBatchRejected { reason, operation }),
        };
        let runtime = self.control.identity();
        let class = self.class;
        let group = inherited_group(runtime, class);
        let _caller = ContextGuard::enter(runtime, class);
        let size = chunk_size.get();
        let count = input.len().div_ceil(size);
        let result = run_chunks(lease.pool(), runtime, class, group, count, |index| {
            let start = index * size;
            let end = start.saturating_add(size).min(input.len());
            operation(start, &input[start..end]);
        });
        drop(operation);
        Ok(result)
    }
}

fn catch_branch<T>(branch: impl FnOnce() -> T) -> SWBranchOutcome<T> {
    catch_unwind(AssertUnwindSafe(branch)).map_err(SWPanic)
}

fn inherited_group(runtime: u64, class: SWExecutionClass) -> Option<u64> {
    context::current()
        .filter(|current| current.runtime == runtime && current.class == class)
        .and_then(|current| current.group)
}

fn run_chunks(
    pool: &crate::backend::MicropoolBackend,
    runtime: u64,
    class: SWExecutionClass,
    group: Option<u64>,
    count: usize,
    operation: impl Fn(usize) + Sync,
) -> SWBranchOutcome<()> {
    let first_panic = Mutex::new(None);
    pool.with_context(|| {
        pool.invoke_indexed(count, |index| {
            let _branch = ContextGuard::enter_owned(runtime, class, group);
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| operation(index))) {
                let loser = {
                    let mut first = first_panic
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if first.is_none() {
                        *first = Some(SWPanic(payload));
                        None
                    } else {
                        Some(payload)
                    }
                };
                drop(loser);
            }
        });
    });
    match first_panic
        .into_inner()
        .unwrap_or_else(|error| error.into_inner())
    {
        Some(panic) => Err(panic),
        None => Ok(()),
    }
}

struct MutableChunks<T> {
    ptr: *mut T,
    len: usize,
    size: usize,
}

// SAFETY: raw_chunk is used only to create references to disjoint chunks. The
// enclosing invocation joins all accesses before its exclusive slice borrow
// ends. T: Send permits moving each chunk's access among threads.
unsafe impl<T: Send> Sync for MutableChunks<T> {}

impl<T> MutableChunks<T> {
    fn raw_chunk(&self, index: usize) -> (*mut T, usize) {
        let start = index * self.size;
        let end = start.saturating_add(self.size).min(self.len);
        (self.ptr.wrapping_add(start), end - start)
    }
}
