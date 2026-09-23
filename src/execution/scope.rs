//! Lexically borrowed joins, indexed batches, and caller-bound overlap.
//!
//! All borrowed accesses settle before return, including failure paths. Keep
//! owner-only preparation on its caller and preserve the no-slot serial fallback.
//! Per-item execution bypasses owned-job graph and admission bookkeeping.
