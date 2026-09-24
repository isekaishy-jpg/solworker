# Ownership, results and publication

[Guide index](README.md)

## Completion answers a specific question

Use a count, token or guard where work or ownership crosses a boundary. Pure
math, a cache lookup and a policy decision do not each need another counter.
The useful question is what obligation ends when a particular handle settles.

| Boundary | What is established | What it does not establish |
| --- | --- | --- |
| Owned task outcome ready | Invocation and captured state have settled; the terminal outcome is available. | All successor edges have activated; owner publication ran; GPU use ended. |
| Sealed group complete | All accepted members have settled, including group settlement bookkeeping. | Every attempted submission was accepted; downstream stages or publication completed. |
| Owner delivery published | That callback ran and its delivery settled on the owner thread. | All other phase callbacks ran; the domain accepted every input; physical readers retired. |
| Work set drained | Roots are closed and its accounted work/discovery obligations have settled. | Retained output allocations have been freed; unrelated sets or external users are finished. |
| Physical access released | The adapter has proved the particular external access ended with required visibility. | All other owners/accessors have released the same allocation. |
| Runtime joined | Runtime work and required host obligations have drained and workers have joined. | Every immutable output handle has been dropped. |

For external producers, logical completion is a provider publication event,
not proof of physical release. The published value must already be safe to
observe; see [external access](rendering.md).
An optional [completion notification](notifications.md) wakes a host to recheck
that logical status. It neither services the provider nor advances the owner
phase. Bind owner or broad progress too when the host must service those steps.

Counts should follow ownership rather than duplicate it. A renderer that already
owns a fence need not add a scheduler counter for every graphics object. Add SW
physical accounting when that use must participate in SW lifetime or drain
tracking, keeping the exact retirement proof in the device adapter.

## Unique results and shared immutable results

`SWTask<T>` observes a unique result. `try_take` moves the outcome out once;
`try_result` temporarily borrows it. A consumed task can remain terminal while
no longer containing a value. Readiness therefore differs from availability of
an unconsumed unique result.

Use `SWTask::into_shared` when multiple consumers need the same immutable
result. `SWShared<T>` requires `T: Send + Sync + 'static`. Cloning it retains the
same result cell without cloning `T`. `try_result` returns an
`Arc<SWOutcome<T>>`, which can outlive the task handle, work set or runtime.
Neither operation creates another CPU task.

Conversion to shared observation allocates a shared outcome envelope when the
value becomes available. Handle/result clones perform ownership bookkeeping and
are not free. Prefer one handle per meaningful chunk or consumer lifetime over
one clone for every element in a hot loop. Borrow the payload inside that scope.

The unique `try_result` guard holds result-storage access while borrowed. Keep
that borrow short; do not wait or reenter code needing the same result while
holding it. Shared outcomes provide independent immutable ownership instead.

`SWTask::ready`, `ready_outcome`, `ready_fallible` and `SWShared::ready` represent
already available values without dispatch. A cache hit should not need a dummy
CPU task just to produce a handle. Use a ready shared value or reuse the cache's
existing shared handle, then choose the correct owner-publication behavior.
An already-ready standalone completion can be watched on any notification
route; registration requests an immediate recheck and retains no source.

Source: [task and result contracts](../src/task/completion.rs).

## Reuse is several different policies

| Reused item | Mechanism | Host responsibility |
| --- | --- | --- |
| Scheduler metadata | Bounded internal job/group/completion reuse; `SWBatch` for recurring waves. | Release obsolete observers and seal groups; do not assume an allocation-free path. |
| Immutable computed output | `SWShared`, optionally holding `SWRetained<T>`. | Define identity/version, validate reuse and retain storage through all readers. |
| Mutable invocation state | Application-owned slots or job store. | Exclusive checkout, generation validation, unconditional return and panic recovery policy. |
| Temporary scratch | Application-owned chunk storage or explicitly supported helper storage. | Prevent concurrent or nested reuse and preserve required capacity. |
| Cache entry | Application cache. | Keying, deduplication, admission, eviction and semantic invalidation. |
| Frame/command storage | Renderer frame slots. | CPU settlement and exact GPU retirement before reset. |

For example, a prepared mesh version can be shared by a cache, collision
preparation and upload preparation. Those consumers may need different
derivatives, and an upload can outlive CPU computation. Keep the shared base
version immutable; attach separate ownership to mutable staging destinations.
Sharing a source asset does not prove that two instances have the same animated
pose, material state or frame history.

Reference counting protects lifetime. It does not establish that a version is
current, make aliased mutation safe or prove that a graphics device stopped
reading an allocation. Version checks and retirement remain separate.

## Retained bytes follow the payload

`SWRetained<T>` couples a value to a declared `SWByteLease`. Stage and external
producer APIs can return this wrapper automatically. Shared outcome clones keep
the same payload and charge. The charge can remain after the producing job,
work set and runtime have drained.

The accounting is declared, not a recursive measurement of `T`. Include nested
buffers under a consistent domain policy. `into_inner` transfers accounting
responsibility to the application; it does not make the allocation cease to
exist. When moving a physical buffer into a logical result, use the retained
transfer route instead of charging the same bytes again.

Source: [retained values](../src/task/retained.rs).

## Returning mutable state requires a host adapter

An ordinary closure owns its captures. If it is suppressed, cancelled before
claim or unwinds, those captures are cleaned up; SW does not automatically return
the original mutable job object as a successful result. Catching a panic outside
a consuming closure cannot recover state that the closure already destroyed.

For kernels that mutate reusable domain objects, an application adapter can
provide the following contract. This is an application pattern, not a supplied
typed batch API:

1. Store the job in generation-tagged owned storage that outlives invocation.
2. Give exactly one invocation access to mutate it; retain a return guard for
   both invoked and never-invoked terminal paths.
3. Return ownership on success, typed failure, panic, cancellation before claim,
   prerequisite suppression and admission rollback.
4. Reclaim by generation/index exactly once, after settlement. Reject stale IDs.
5. Inspect the outcome before publishing the recovered domain state. A panic can
   leave partial mutation; ownership recovery is not transactional rollback.
6. Begin the next generation only after required jobs are reclaimed and readers
   of mutable storage have ended.

Use existing groups and task status underneath this adapter. It needs retained
storage and recovery rules, not its own scheduler. Destructors for transferable
captures must be legal on execution, rejection or cleanup threads; keep
thread-affine objects with their owner instead.

## Main-thread work is an explicit owner

`SWRuntime::owner(state, capacity)` registers `SWOwner<O>` on the calling thread.
The state and locally registered callbacks may be non-`Send`. The owner itself
stays thread-bound even when `O` happens to be `Send`. An owner can represent a
main thread, a dedicated service thread or another explicit host context.

The host assigns an `SWPhase`, calls `set_phase`, then pumps that phase with
`SWPumpBudget`. A phase is an application eligibility label; the crate does not
advance frames or impose simulation event ordering.

| Operation | Behavior |
| --- | --- |
| `try_post` | Queues a callback; never invokes it inline. |
| `on_ready` | Registers deferred owner inspection of one completion, including already-ready inputs. |
| `with_ready` | Explicit synchronous access to a ready shared outcome in the selected phase. Pending input returns the callback untouched. |
| `prepare_delivery` | Reserves a local callback and returns a transferable ticket before producing work is admitted. |
| `SWOwnerSender::try_post` | Transfers a `Send` callback to an existing owner; its execution and accepted-capture cleanup stay owner-local. |

For required publication, reserve delivery first and attach its ticket through
`try_spawn_delivering`, a stage or an external producer. This prevents accepting
the producer first and discovering later that notification capacity is full.
Rejection preserves the operation and uncommitted ticket. Dropping an unused
ticket suppresses the local callback, whose cleanup still needs an owner pump
or close. Producer success does not force publication after the owner closes.

Completion ordering is not semantic ordering. If callbacks must publish by
sequence number, retain a cursor or ordered inbox in owner state and only advance
through accepted inputs. Aggregate multiple prerequisites explicitly before
registering one `on_ready`, or retain a host inbox that represents them. Never
interpret one ready notice as proof that unrelated prerequisites are satisfied.

`SWPumpMode::Live` rechecks arrivals between entries. `Batch` freezes the eligible
frontier at entry. Count/time limits are checked between callbacks and cleanup;
one long callback or destructor can exceed the duration budget. Keep callback
work short or divide domain work at valid semantic boundaries.

Sources: [owner](../src/owner.rs), [phase and budgets](../src/owner/phase.rs),
[CPU delivery](../src/execution/delivery.rs).
