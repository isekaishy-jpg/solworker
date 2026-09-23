//! Eligibility of workers, helping callers, and saturated submitters.
//!
//! Check actual affinity and context capabilities rather than assuming a caller
//! has worker-local state. Preserve valid same-lane nested execution.
