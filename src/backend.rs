//! Private CPU executor integration.
//!
//! Keep dependency types and raw pool handles inside this boundary. Connect
//! execution and lifecycle contracts without introducing a second executor
//! abstraction or speculative interchangeable-backend traits.

mod micropool;

pub(crate) use micropool::MicropoolBackend;

#[cfg(test)]
#[path = "../tests/unit/backend.rs"]
mod tests;
