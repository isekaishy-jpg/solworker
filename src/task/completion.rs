//! Task outcomes, failure status, and completion observation.
//!
//! CPU completion follows execution-capture cleanup. Retained result ownership,
//! owner publication, and external physical release remain distinct boundaries.
//! Polling does not execute work, pump callbacks, or promote demand.
