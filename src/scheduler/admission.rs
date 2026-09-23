//! Atomic admission and optional capacity reservations.
//!
//! Account records, prerequisites, and promised delivery before exposing a stage.
//! Return unaccepted ownership on rejection. Keep runnable saturation distinct
//! from metadata exhaustion, handoff permits, and declared payload pressure.
