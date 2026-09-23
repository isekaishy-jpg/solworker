//! Owned task handles, shared observation, and producer cancellation authority.
//!
//! Unique results move once; shared handles retain immutable ownership. Dropping
//! an observer does not cancel its producer. Immediate results require no CPU job.

mod completion;
mod dependency;
