//! Declared outcome storage that follows the actual retained payload.

use crate::scheduler::reservation::SWByteLease;
use std::ops::Deref;

/// An immutable stage value with its declared retained-byte charge. Shared
/// outcome clones keep the same wrapper and charge, rather than copying either.
pub struct SWRetained<T> {
    value: T,
    _charge: Option<SWByteLease>,
}

impl<T> SWRetained<T> {
    /// Wraps an already available value without scheduling work. The lease
    /// accounts declared storage, not allocations discovered inside `T`.
    pub fn from_lease(value: T, charge: SWByteLease) -> Self {
        Self::new(value, Some(charge))
    }
    pub(crate) fn new(value: T, charge: Option<SWByteLease>) -> Self {
        Self {
            value,
            _charge: charge,
        }
    }

    /// Transfers storage accounting to the application. The caller must account
    /// for the returned value in its own cache/resource budget from this point.
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Deref for SWRetained<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SWRetained<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}
