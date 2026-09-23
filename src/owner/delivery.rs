//! Deferred publication, claim/cancel arbitration, and local cleanup.
//!
//! Reserve notification capacity before promising delivery. Keep records alive
//! through in-flight notifications, and settle captures in their legal context.
//! Publication completion is separate from the producer's CPU completion.
