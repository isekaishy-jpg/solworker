//! External logical outcomes and separately tracked physical accesses.
//!
//! Provider cancellation, abandonment, and timeout cannot release storage still
//! in use. Expose payloads only when access is valid, and retain cleanup until
//! the provider acknowledges release. I/O and graphics backends remain external.
