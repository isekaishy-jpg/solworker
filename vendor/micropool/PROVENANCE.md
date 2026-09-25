# Micropool provenance

This directory contains micropool 0.4.2 from
[`DouglasDwyer/micropool` at `c5ddc7b4397443ff0ced59d2a51b309d1b646a09`](https://github.com/DouglasDwyer/micropool/tree/c5ddc7b4397443ff0ced59d2a51b309d1b646a09).
The upstream crate is dual licensed under MIT or Apache-2.0; its original
`LICENSE-MIT` and `LICENSE-APACHE` files are retained here.

The local changes include fallible guarded worker construction, a checked
owned-task handoff, explicit stop/join or stop/detach lifecycle paths, and
clearing worker-local pool pointers before their stack owner expires. Scoped
execution adds same-pool context reuse, indexed borrowed invocation, and a
caller-only owner continuation that advertises its transferable worker unit
before running the owner branch. Foreground slot capacity can be configured
exactly despite bitset rounding. Ordinary micropool submission and execution
semantics remain upstream.
The event listener now re-arms its waiter flag on every wait retry. A delayed
notifier can clear a newer listener's flag without changing that listener's
saved version; the upstream one-time arm then repeatedly returns from the OS
wait. The local retry preserves the strong compare-exchange, release/acquire
publication ordering, and wake-all behavior. A controlled test in
`tests/unit/event.rs` covers this interleaving against the actual wait loop.
`stop_and_detach` discards queued owned callables, while already claimed tasks
may complete. A discarded raw task handle has no terminal result and must not be
joined; the adapter must keep raw task handles private. Runtime-owned terminal
cancellation remains a later integration responsibility. The runtime must
establish global quiescence before `stop_and_join`.
