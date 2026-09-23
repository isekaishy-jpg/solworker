//! Coordination of accepted owned work.
//!
//! Own the shared admission, record identity, claiming, and completion transitions
//! that must commit together. Child modules define policies within those
//! transactions rather than independent schedulers or unrelated lock domains.
//! User code, destructors, provider hooks, and backend calls run outside locks.

mod admission;
mod demand;
mod ready;
mod work_set;
