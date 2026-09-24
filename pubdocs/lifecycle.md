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

An interactive host loop conceptually performs:

```text
run independent owner work
inspect required results and phase eligibility
pump permitted owner publications and provider progress
service bounded demand/control updates
help eligible work from the specifically needed group
recheck results, cleanup obligations and native input/deadlines
park only when the host has no useful permitted work and wake coverage is valid
```

This is an integration pattern, not a supplied event-loop function. Input
servicing need not advance the simulation's semantic input cutoff. Keep those
two decisions separate when a wait occurs inside a frame.

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

## Native wake integration remains a proposed capability

This checkout does **not** expose a runtime-wide native wake callback or a
`SWRuntimeBuilder::wake_hook` method. `with_worker_setup` is worker initialization,
not a notification hook. Passive progress waits do not replace a main-thread
window-system wait.

A future platform-neutral hook should let a host signal its own durable native
wake bridge. The intended integration requirements are:

- Publish readiness/control state before advancing a generation and signalling.
- Cover failure, cancellation, owner delivery, capacity/control progress and
  external completion, not just successful CPU returns.
- Invoke outside scheduler/owner locks, with a short, nonblocking callback safe
  for concurrent notification. Do not reenter scheduler or owner execution.
- Define callback panic reporting and a host path that leaves native parking if
  notifications can no longer be trusted.
- Keep the bridge valid through in-flight notifications and retained observers;
  destroy native handles only after notification activity is safely excluded.
- Use an arm-and-recheck protocol: a wake before arming prevents sleep; a wake
  after arming signals the native wait. Do not rely solely on another thread's
  promise to signal later.

These are design requirements, not guarantees of an existing hook. Integrations
that need combined input/completion parking must supply and verify wake coverage
for all relevant transitions or implement this missing capability first. The
examples deliberately avoid a native parking loop; they are finite CPU examples.

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
