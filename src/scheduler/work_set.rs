//! Lifetime accounting for accepted work and descendant discovery.
//!
//! Account children before releasing the last discoverer. Track shared producers
//! separately from consumer subscriptions and their direct state accesses.
//! Empty queues alone do not establish drain or global quiescence.
