//! Execution lanes and caller participation.
//!
//! A lane selects dedicated worker capacity. Scoped work and retained groups
//! share execution-context rules but have different lifetime boundaries.

mod context;
mod group;
mod scope;
