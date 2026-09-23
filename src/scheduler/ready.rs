//! Ready selection and bounded executor handoff.
//!
//! Coordinate FIFO class dispatch, eligible group claims, and explicitly selected
//! resource-demand ordering. Preserve exactly-once execution across helping,
//! cancellation, and handoff; backend queue ownership is not completion.
