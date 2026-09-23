//! Disposal of panic payloads owned by runtime containment boundaries.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Attempt destruction once. If it panics, deliberately retain the new payload:
/// recursively destroying arbitrary panic payloads cannot guarantee termination
/// or prevent another unwind into the executor. This exceptional leak does not
/// apply to payloads returned to the caller or propagated with resume_unwind.
/// Call outside bookkeeping locks, inside the relevant participation context.
pub(crate) fn discard_panic(payload: Box<dyn Any + Send>) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(secondary);
    }
}
