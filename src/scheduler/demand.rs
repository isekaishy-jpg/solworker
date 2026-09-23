//! Consumer interest and resource-stage urgency.
//!
//! Aggregate independent consumers and propagate versioned demand through
//! unresolved prerequisites. Ordinary execution-class queues remain FIFO;
//! resource ranks and freshness policy do not replace capacity classes.
