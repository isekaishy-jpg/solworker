# Progress, failure and shutdown

[Guide index](README.md)

## The host remains a participant

Worker availability alone does not guarantee application progress. Owner
callbacks, provider completions, demand propagation and GPU retirement may all
need services outside the CPU pools. Name who services each dependency before
introducing a wait.

| Operation | Executes CPU jobs? | Executes owner/provider work? |
| --- | --- | --- |
| `SWCompletion::wait` / `wait_timeout` | No. | No. |
| `SWProgress::wait_for_change` | No. | No. |
| `SWWorkSet::wait_drained` | No. | No. |
| `SWGroup::help_ready` | At most one eligible ready job from that group. | No. |
| `SWGroup::wait_helping` | Helps eligible members of that group before parking. | No. |
| `SWOwner::pump` | Does not generally dispatch CPU jobs; callbacks may perform explicitly allowed work. | Runs eligible callbacks/local cleanup on that owner. |
| `SWRuntime::service_demand` | No user CPU jobs. | Propagates control updates and may notify external provider demand hooks. |

Passive waits are rejected in CPU execution and live-owner contexts. A timeout
ends observation, not the producer. Helping does not provide a native input
pump, and a long caller-eligible job can delay the host just as a long callback
can. Eligibility is a latency and reentrancy decision as well as a thread-safety
decision.

Choose the wait arrangement from the services the remaining work needs. A
coordinator can use the following pattern when workers can finish the required
CPU wave without further owner/provider service:

```text
submit and seal the required CPU wave
run independent owner work
join the required group with targeted helping
check admission errors and outcomes, then consume results
service owner publications, platform input and providers at their allowed phases
```

This is a work arrangement, not a mandatory frame order. The host places its
service phases according to application semantics. A group join does not pump
messages or callbacks, and owner publication may still be pending after that
group completes. If the join's duration would delay input too long, or its
completion requires this caller's service, use a host-driven progress path:

```text
inspect the required completion and permitted phase work
service bounded owner/provider/demand work and native input where allowed
help eligible work from the specifically needed group
recheck completion, required services and deadlines
return to the host's normal loop, or use a verified host wait arrangement
```

Neither `help_ready` returning `Ok(false)` nor an empty owner pump proves that
all work is complete or that a native wait is safe. Do not turn this outline into
an unbounded busy loop. Parking requires a host mechanism that cannot miss the
transitions needed for progress; a pending CPU group alone does not provide a
native event-loop signal. Input servicing also need not advance the simulation's
semantic input cutoff. Keep those decisions separate during an in-frame wait.

## Progress snapshots are diagnostic observations

`SWRuntime::progress` reports scheduler, owner, work-set, capacity and external
state plus a wake generation. Components are sampled under separate locks, so
the result is not an atomic snapshot or a proof of drain. Counters can overlap:
for example, runtime active leases and scheduler records are not disjoint work
that should be summed.

After observing progress, a permitted passive observer may call
`wait_for_change(deadline)` and sample again. The signal reports that something
changed, not that a requested task succeeded or an entire runtime drained.
Observation does not pump providers, owner work or demand updates.

Source: [progress API](../src/progress.rs).

## Native event-loop integration is a host choice

The optional [`SWNotifyRoute`](notifications.md) binds completion, owner or broad
runtime progress to a host-supplied signal function. `with_worker_setup` remains
worker initialization. Existing group waits manage their internal
notification/recheck protocol. A finite CPU pipeline or an offscreen renderer
does not need a native wake bridge merely to use that protocol.

A host that parks on a combined input/completion wait has an additional
integration requirement: CPU readiness must be able to reach that wait, and
the host must recheck durable state before sleeping. Platform handles, input
dispatch, pacing and the decision to park belong to the host/platform adapter.
Passive `SWProgress` waits do not replace this integration and remain forbidden
from live owner/CPU execution contexts.

Configure notification limits, bind every source whose progress the host must
service, then use the [reset, arm and recheck protocol](notifications.md#arm-inspect-then-park)
before parking. A completion-only binding cannot wake the host to perform
owner or provider work that the completion still depends on. The host owns the
native wake destination and its consumption.

## Distinguish admission, application failure and execution failure

| Situation | Observable behavior | Host action |
| --- | --- | --- |
| Admission refused | Rejection returns uninvoked inputs and a reason. | Retry after real progress, defer, resize or report failure; preserve the input owner. |
| Ordinary job returns `Err` | `try_spawn` treats the returned value as successful execution. | Use the fallible API if success-only dependencies must be suppressed. |
| Fallible job returns `Err` | Status is `ApplicationFailed`; typed output remains `SWOutcome::Success(Err(error))`. | Read the typed error and apply domain recovery. |
| Prerequisite failure | Success-only operation is suppressed and reports `PrerequisiteFailed`. | Settle cleanup and preserve original error information where needed. |
| Unclaimed work cancelled | Operation is suppressed; outcome reports cancellation. | Do not assume other consumers or physical users are cancelled. |
| Running work receives cancellation | It is not forcibly stopped. | Wait for its real settlement; use domain-specific cooperative cancellation if needed. |
| Invocation panics | With unwinding, panic is contained by the execution boundary. | Treat partially mutated domain state as requiring validation/recovery. |
| External producer abandoned | Logical outcome becomes abandoned. | Independently finish any provider physical access. |

`SWProducerControl` is cancellation authority. Dropping a task, completion,
shared handle or control handle does not request cancellation. CPU cancellation
competes with claiming the invocation; it is not a way to interrupt arbitrary
code.

`OutcomeAware` successors can process terminal failures, but cleanup must also
cover cases where the cleanup job itself cannot be admitted. Use owner guards or
pre-reserved cleanup responsibilities where losing cleanup would lose mutable
state or strand an external use. Do not add recovery work only after exhausting
all capacity needed to admit it.

An owner callback panic faults the owner and does not roll back its state.
Inspect/repair state explicitly and settle pending local cleanup before recovery,
or close the owner. Runtime containment assumes unwinding; an aborting panic
configuration cannot provide recovery from an abort.

Sources: [outcomes](../src/task/completion.rs), [producer control](../src/task.rs),
[owner fault handling](../src/owner.rs).

## Scene replacement and application exit use the same lifetime rules

Replacing a document or world usually cancels that consumer's work set and
invalidates its publication generation. Shared cache producers can remain live.
Keep callbacks from an obsolete generation from publishing into the new owner,
while still returning old mutable jobs and retiring physical users.

For application shutdown:

1. Stop creating domain roots and call `SWRuntime::begin_shutdown`. It closes
   runtime roots and seals work sets without waiting.
2. Seal fixed groups and release discoverers that will admit no more work.
   Accounted descendants can continue under their accepted capabilities.
3. Continue allowed owner publication, suppressed-callback cleanup, provider
   pumping and demand/control service. Keep resources needed for progress alive.
4. Settle backend CPU work and prove external/GPU retirement. Logical
   cancellation cannot substitute for this step.
5. Close owners on their own threads after deciding which publication must
   finish and which should be suppressed. Closing cleans unclaimed local work.
6. Call `try_shutdown` from an eligible host context until it returns `Ok(true)`.
   `Ok(false)` means host progress is still needed; it is not successful joining.
7. Release host services and native notification resources after they can no
   longer be accessed by runtime/provider activity.

Do not spin on `try_shutdown` without servicing the reported obligations. Its
final worker join can still block on thread termination/TLS cleanup; it is not a
hard real-time polling primitive. `shutdown` is a convenience blocking join for
workloads needing no ongoing host service, and reports blockers such as live
owners, work sets, external access or pending control updates.

Dropping the runtime requests stop without joining. `abandon` suppresses
unclaimed work and requests stop; capture cleanup can take time, and running work
may finish later. Abandonment cannot be upgraded to joining shutdown. Neither
operation is a substitute for graceful teardown or permission to free resources
still accessed by foreign code.

Source: [runtime lifecycle API](../src/runtime.rs).
