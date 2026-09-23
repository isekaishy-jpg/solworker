//! Application-defined phase eligibility and bounded pumping.
//!
//! Live pumping rechecks arrivals; batch pumping freezes the entry frontier.
//! Immediate ready access is explicit. Budgets apply between callbacks, and
//! nested pumping must not create a second mutable owner borrow.
