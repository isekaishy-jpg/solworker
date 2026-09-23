# Micropool provenance

This directory contains micropool 0.4.2 from
[`DouglasDwyer/micropool` at `c5ddc7b4397443ff0ced59d2a51b309d1b646a09`](https://github.com/DouglasDwyer/micropool/tree/c5ddc7b4397443ff0ced59d2a51b309d1b646a09).
The upstream crate is dual licensed under MIT or Apache-2.0; its original
`LICENSE-MIT` and `LICENSE-APACHE` files are retained here.

The local changes are limited to fallible guarded worker construction, a
checked owned-task handoff, and explicit stop/join or stop/detach lifecycle
paths, plus clearing worker-local pool pointers before their stack owner expires.
Ordinary micropool submission and execution semantics remain upstream.
`stop_and_detach` discards queued owned callables, while already claimed tasks
may complete. A discarded raw task handle has no terminal result and must not be
joined; the adapter must keep raw task handles private. Runtime-owned terminal
cancellation remains a later integration responsibility. The runtime must
establish global quiescence before `stop_and_join`.
