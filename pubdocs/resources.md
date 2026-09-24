# Resources, demand and capacity

[Guide index](README.md)

## Separate the service from CPU preparation

A loading path commonly crosses several owners:

```text
consumer request -> source lookup / producer deduplication
                 -> provider read or already-resident bytes
                 -> CPU decode / preparation
                 -> owner publication and child discovery
                 -> renderer upload or another consumer
```

Solworker supplies the CPU, dependency, demand and lifetime mechanisms. The host
keeps cache keys, format parsing, source identity, resident storage and provider
service. File/network/audio services can retain their own execution contexts.
Replacing a CPU pool does not automatically migrate their persistent threads.

Use `SWRuntime::external` to admit a provider-supplied logical result without
occupying a CPU worker. Start the provider only after admission succeeds. The
returned `SWProducer<T>` completes the result, while the task observes it and
the producer control owns cancellation authority. A failed `complete` returns
the value if cancellation or abandonment won the race. Dropping an unfinished
producer publishes abandonment; it does not stop physical access.

Represent a service continuation with owned state and dependencies. While a
source is pending, release the CPU worker. Resume on the appropriate terminal
outcome so typed source errors can be handled. Keep continuation admission or
reserve successor capacity before relinquishing the current state. Otherwise a
full queue can strand a required continuation after its read completes.

Finite blocking decoders or provider operations may be unavoidable, but isolate
their capacity deliberately. Never put a blocking read in a caller-helpable
frame job just because both are related to the same asset.

Sources: [external producers](../src/external/producer.rs),
[resource stages](../src/execution/stage.rs).

## Distinguish a producer from its consumers

A cache can deduplicate a pending producer and give several consumers the same
`SWShared` result. Its producer lifetime may span several scenes or documents.
Each consumer has its own interest, publication and cancellation rules.

For example, two views need one texture. Closing one view should detach that
view's demand and suppress its unclaimed publication, while leaving the shared
producer available to the other view. Give producer cancellation authority to
the cache/service that owns the production decision. Dropping a task observer
or `SWDemand` does not cancel the producer.

`SWCompletion::demand_in` attaches interest to a consumer work set, and
`SWOwner::on_ready_in` attaches that consumer's publication. Their `*_from`
variants use an existing discovery permit for descendants after root closure.
The consumer set can cancel those relationships without taking ownership of the
shared producer. If the producer itself belongs to a cancelled work set, that
set does have producer cancellation authority; choose the owner intentionally.

Warm hits should reuse existing immutable outcomes or create a ready handle.
They can still require owner publication in a particular phase. `on_ready`
preserves deferred behavior on both hits and misses; `with_ready` is an explicit
immediate path when synchronous publication is semantically acceptable.

## Dynamic loading needs a discovery boundary

`SWWorkSet` tracks owned producers, consumer deliveries and live discovery
permits. It is different from a fixed CPU wave: children can be discovered after
roots have been submitted, and an empty runnable queue does not prove completion.

1. Create a work set with a bounded discovery-permit capacity.
2. Acquire a permit before a discoverer must outlive root admission.
3. Admit roots and carry the permit with the state that can discover children.
4. Seal the set when no additional roots should be accepted.
5. Use the permit to admit accounted descendants even after sealing. `fork`
   explicitly consumes capacity for another independent discoverer.
6. Drop each permit when it can no longer discover work. Keep owner/provider
   progress running until `is_drained` becomes true.

A retained unused permit keeps the set undrained. Cancellation rejects new
descendants and requests cancellation of owned producers/unclaimed deliveries;
running work and local cleanup still have to settle. Immutable retained results
do not keep the set active merely because a consumer still holds them.

`wait_drained` is passive and forbidden from a CPU or live-owner context. Use
explicit host progress when loading requires callbacks or provider pumping.

The [resource example](../examples/resource_pipeline.rs) seals a producer set
and then admits a dependent CPU stage through a previously acquired permit.

Sources: [work sets](../src/scheduler/work_set.rs),
[work-set submission](../src/execution/work_set.rs).

## Resource urgency follows pending demand

Configure supported ranks and a lease bound with `with_demand_limits`. A resource
stage can carry a baseline `priority`; consumers retain independent `SWDemand`
leases. Lower ranks are more urgent within the resource route.

| Demand operation | Intended use |
| --- | --- |
| `refresh(priority)` | Change a consumer's urgency and reactivate deferred interest. |
| `promote()` | Move still-pending work ahead of equal-rank work. |
| `defer()` | Keep background interest without active demand. |
| Drop the lease | Remove only that consumer's interest. |

Resource ordering and ordinary CPU FIFO are separate routes. A resource rank is
not a global ordering over every job in a lane. Already claimed/running work is
not preempted. Group completion tokens do not propagate resource demand to all
members; attach interest to the actual producer/dependency chain that represents
the resource.

Call `SWRuntime::service_demand` with a bounded host budget when demand/control
propagation needs servicing. Readiness observation alone does not service it.
Its return value reports whether more propagation remains serviceable.

External providers can receive `provider_demand` callbacks with a versioned
`SWDemandSnapshot`. Calls occur outside scheduler locks, may overlap and may
arrive after logical settlement. The adapter must reject old versions and check
its own provider lifetime. No demand is not a physical cancellation proof. A
callback panic is contained and that notification discarded; provider state
must remain valid without treating a notification as a one-shot ownership grant.

Source: [demand](../src/scheduler/demand.rs).

## Bound metadata, execution pressure and bytes separately

`SWOwnedLimits` defines independent limits:

| Limit | What occupies it |
| --- | --- |
| `records` | Waiting, ready, handed-off, running and finalizing owned jobs. |
| `edges` | Registered prerequisite edges through terminal detachment. Zero disables dependent submissions. |
| `runnable[class]` | Ready and handed-off jobs, excluding running jobs. |
| `handoff[class]` | Backend wrappers until they return, including running wrappers. |

The arrays use Low/Mid/High order. A handoff window of one can serialize owned
worker execution in a class despite several workers. Increasing it blindly can
also change pressure and latency; measure the actual workload and scoped/owned
mixture.

Optional `SWLimits` and `SWCost` add declared records, edges, promised deliveries
and payload bytes. Ordinary work has a target; required pipelines have a
protected allowance and a concurrency bound. Required byte usage may exceed the
ordinary target. Only a configured `hard_byte_ceiling` supplies the aggregate
byte ceiling; `required_allowance.bytes` is not a separate required byte cap.
This policy does not infer allocations or replace domain memory budgets.

`reserve_ordinary` and `reserve_required` produce an `SWReservation`. Its `stage`
or `split` operations distribute credits, `try_grow` is fallible, and
`retain_bytes` transfers a charge to output lifetime. A retained-byte lease can
outlive the pipeline without occupying its concurrency slot. Do not charge one
allocation independently in both a cache budget and SW without an explicit
transfer policy.

For stage and logical-provider options, `retained_bytes` is the portion of the
declared `cost.bytes` that must follow the output. It must not exceed that total.
Include both temporary and retained storage in the stage's peak declaration;
completion releases temporary credits while retained bytes follow their value.
Setting a byte count does not allocate or reserve a Rust buffer for the caller.

Admission errors preserve uninvoked operations and relevant options. Decide
whether to retry after progress, split an oversized phase, defer optional work
or fail the domain request. `TooLarge` is not resolved by blindly spinning on
the same request. Admission retry must remain bounded and preserve ownership.

Sources: [owned limits](../src/scheduler/admission.rs),
[reservations](../src/scheduler/reservation.rs).

## Coherent publication is an application composition

Some phases must admit every required job before any job mutates live domain
state. Reserving declared credits does not make a sequence of individual
submissions an atomic bulk operation.

One possible composition uses an external logical result as a publication gate:

```text
retain recoverable inputs and reserve phase capacity
admit a gate producer before admitting the phase
admit every required job with a success-only edge from the gate
if all admissions succeed: complete the gate successfully
otherwise: abandon/cancel the gate and preserve the admission error
seal the group; settle suppressed/accepted jobs; reclaim all input owners
```

The gate, dependencies and cleanup need capacity too. Every rollback path must
return mutable state, and cleanup cannot depend on gate success. An external
gate is an existing primitive, not a native atomic-batch guarantee. The host
must implement and verify the composition before relying on it.

Independent incremental jobs often need no gate. Publish them earlier when
domain semantics permit; retain per-job acceptance and failure records. The
choice is about the phase's mutation contract, not a requirement to gate every
batch or serialize independent source preparation.
