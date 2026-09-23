//! Thread-bound owner state and transferable delivery routes.
//!
//! Owner state and local captures stay on their creation thread. Sender handles
//! expose routing without granting access to owner state.

mod delivery;
mod phase;
