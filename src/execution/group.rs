//! Retained, classed batches and exact-group helping.
//!
//! Preserve group identity through admission and backend handoff. Claim each
//! eligible member once, and settle captures before reporting group completion.
//! Helping does not implicitly pump owner callbacks or unrelated groups.
