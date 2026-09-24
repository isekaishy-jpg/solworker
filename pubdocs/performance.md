# Performance and qualification

[Guide index](README.md)

## Optimize the complete work arrangement

Pool dispatch is one component of a frame or pipeline. Main-thread preparation,
allocation, merging, publication, provider waits and GPU execution can dominate
even when worker utilization looks good. Start with the actual dependency path
to the consumer, not an assumption that more jobs will make it shorter.

Prefer finite chunks that amortize admission and synchronization while exposing
enough independent work. Group related items where that preserves scratch,
cache locality and output ordering. Borrow within a real lexical lifetime; use
owned work when it enables useful overlap. Avoid a heap allocation, shared-handle
clone and task submission per trivial element when one chunk can own the work.

Reuse task metadata and useful application storage separately. `SWBatch` helps
with recurring group ownership, but keeping vector capacities, pose buffers,
decode scratch or recording contexts is still a domain responsibility. Retained
observers may prevent metadata reuse. Do not promise allocation-free execution
merely because a batch object itself is reused.

Launch independent work before serial preparation that does not depend on it.
Admit downstream work behind completion tokens early where ownership allows.
Keep caller helping selective and give helpers valid scratch. Avoid a large
serial owner merge when finalization is actually worker-safe, but preserve its
semantic ordering and publish only fully valid results.

## Tune one cause at a time

| Observation | Investigate before changing the pool |
| --- | --- |
| Many workers idle with ready owned work | Handoff/runnable windows, class assignment, owner dependencies and admission limits. |
| Main thread spikes | Domain preparation, callback volume, allocation/copy work, inline fallback and large helping jobs. |
| More recording jobs make frames slower | State-cache resets, repeated packing, command-list count and reduced grouping. |
| Streaming appears stalled | Demand service, provider pumping, child discovery, byte pressure and missing successor capacity. |
| Shorter submission time but unchanged frame time | Work shifted to another stage; unchanged critical path; GPU or presentation wait. |
| High average throughput but poor tail latency | Bursts, long indivisible jobs, owner backlog, allocator contention and class capacity contention. |
| Reuse grows memory over time | Retained results/observers, unreleased discovery permits, access orphans and unbounded domain caches. |

Worker class, resource urgency, caller eligibility, chunk width and admission
capacity solve different problems. Change the control that corresponds to the
observed cause. Separate classes reserve execution routes but still share CPUs,
memory bandwidth and the OS scheduler. They do not establish a fairness deadline.

## Keep the same useful workload

A valid comparison preserves input data, output equivalence and required domain
events. For an interactive scene, hold the population, camera, movement path,
resolution, graphics settings, resident state and simulation policy constant.
For a processing pipeline, hold source size, versions, cache state and outputs
constant. A faster run that skips callbacks, changes visible work or consumes a
stale result measures a different workload.

Exercise a small set of distinct scenarios rather than many redundant tests:

- Empty/small waves, where dispatch can exceed useful compute.
- Steady recurring work with warmed storage and realistic result readers.
- Many instances sharing source assets but requiring independent mutable state.
- Cold reads and bursty child discovery, plus warm cache hits through the same
  owner-publication contract.
- Rapid demand changes, consumer cancellation and scene replacement.
- Capacity saturation and failed admission with required cleanup still funded.
- CPU frame overlap with delayed device retirement and out-of-order recording
  completion.

Qualify correctness at ownership transitions: stale generation rejection,
panic/cancel/refusal state return, preserved publication order and no reuse before
retirement. Reuse existing domain oracles; do not duplicate arithmetic tests just
because execution moved to another lane.

## Record latency, useful work and retained state

| Category | Useful measurements |
| --- | --- |
| Host frame/pipeline | p50/p95/p99/max elapsed time, deadline misses and main critical path. |
| CPU stages | Submission/admission duration, ready-to-start or submission-to-start latency, execution, finalization and publication time. |
| Scheduler pressure | Class configuration, active workers, help count, refusals, handoff occupancy, records/edges and pending owner work. |
| Memory | Allocations, retained capacities, copied/packed bytes, declared usage and physical-access backlog. |
| Useful work | Evaluations, callbacks, source reads, decoded bytes, grouped runs, recorded commands and accepted output versions. |
| Graphics/provider | Submission, GPU execution, fence waits, presentation and provider wait time as separate measurements. |

Label what each timing contains. Submission-to-start can include dependencies
and queueing, not just executor dispatch. Parallel CPU durations are not frame
wall time, and nested scopes must not be summed as independent costs. Public
progress snapshots are diagnostic observations, not a substitute for precise
per-operation tracing; some instrumentation belongs in the application adapter.

If evaluating a scheduler-overhead budget, define the accounting first. Separate
admission, dependency/group bookkeeping, helping, result handling and owner
transport from useful kernels, OS scheduling delays and GPU/provider waits.
State whether the number is main-thread elapsed time, aggregate CPU time or
critical-path contribution. A frame's total time alone does not establish a
50-microsecond scheduler cost, and this crate currently promises no such bound.

## Use benchmarks and flamegraphs together

Benchmarks quantify time and variation. CPU flamegraphs locate sampled CPU costs
such as allocation, copying, packing, lock contention and scheduler bookkeeping.
Use optimized builds with symbols, capture representative steady and burst
intervals, then confirm any improvement with unprofiled matched runs.

A CPU flamegraph alone cannot explain parked threads, queue delay or GPU stalls.
Pair it with readiness/latency spans and device/provider timing; use wait or
off-CPU analysis when blocked time is the issue. Profiling and verbose tracing
have overhead, so record their configuration and do not silently compare a
profiled run with an unprofiled one.

Record the revision, hardware, toolchain, build profile, dataset/scene, cache
state, worker split, capacities and profiler settings alongside results. Keep
one execution owner per migrated region. If old and new runtimes coexist during
an experiment, split the same total worker budget explicitly; two full pools
are not an equivalent replacement comparison.

Keep generated profiles and reports under ignored build/output directories with
bounded retention. Cargo's build cache and run-history artifacts serve different
purposes; avoid duplicating the build tree for every measurement. This guide
does not require any private repository tooling to run the public examples.
