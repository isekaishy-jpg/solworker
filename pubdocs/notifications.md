# Optional host notifications

[Guide index](README.md)

`SWNotifyRoute` connects Solworker state changes to a host wake primitive. The
signal function runs on the thread that publishes the change and asks the host
to recheck state. It carries no result and does not run a job, pump an owner,
service a provider or prove that a device has stopped using memory. Existing
group helping and passive progress waits remain available without a route.
The callback contract forbids calling `SWRuntime::service_demand` or
`SWCompletion::demand` from the signal function: both can reach provider demand
hooks. This is a host adapter obligation, not a general API sandbox enforced on
every call made inside the closure.

Enable routes only when a host needs to combine Solworker progress with its own
input, timers or provider events:

```rust,ignore
let runtime = SWRuntime::builder(config)
    .with_owned_limits(owned_limits)
    .with_notification_limits(SWNotifyLimits { routes: 1, bindings: 4 })
    .build()?;
let host_thread = std::thread::current();
let mut route = runtime.notification_route(move || {
    host_thread.unpark();
    Ok(())
})?;
let completion_binding = route.watch_completion(&group.completion())?;
```

The signal closure owns the context it needs and must be `Send + Sync`. Keep it
short: signal a durable event or waker and return. It may run on a worker,
provider, helper, registration caller, shutdown caller or thread-exit cleanup, and calls from
successive arm cycles may overlap. It must not submit work, pump callbacks, wait
for work or call notification management methods. Signal errors and panics fault
the route; inspect `route.fault()` and replace or close it. A fault does not
rewrite task, group or owner state.

## Bind the state the host will service

| Binding | Change signaled | Recheck |
| --- | --- | --- |
| `watch_completion(&token)` | One task, external result or sealed group becomes terminal, including failure or cancellation. | `status()` and the retained outcome or group result. |
| `watch_owner(&owner)` | Owner delivery readiness, cleanup, capacity or close progress. | The owner's eligible phase work and lifecycle. |
| `watch_progress()` | Broad runtime scheduler, admission, demand, work-set, provider and shutdown progress. | The specific resource or blocking obligation. |

Retain the returned `SWNotifyBinding` while interested. Dropping it detaches
future changes, although a signal already claimed may still arrive. Several
sources can feed one route; separate hosts can bind separate routes to a shared
completion. Register every intermediate source that the host must service to
reach the final result. For example, a host that must pump owner delivery or
publish an external result cannot rely only on a group completion binding.

Registration requests an initial recheck, including when the source already
changed. A completed `SWTask::ready` or `ready_outcome` token is accepted on any
route and returns an inert binding. Runtime-owned tokens retain their runtime
identity after completion; a foreign token is rejected. A reused `SWBatch` wave
has a new completion identity, so bind its current token each wave. An old
binding never follows storage reuse to a new wave.

Route and binding capacity are fixed by `SWNotifyLimits`. Full, disabled,
closed, foreign-source and invalid-context failures are distinct. Route
construction rejection returns the uninvoked signal closure, including its
captures. A route is a single host controller: its `prepare_wait` and `close`
methods require mutable access. Do not let separate consumers drain or reset
the same underlying native event independently.

## Arm, inspect, then park

The host owns consumption of its native wake primitive. Use this order on each
iteration:

1. Drain or reset the native wake according to its platform contract.
2. Call `route.prepare_wait()` to clear the route's pending state and get a stamp.
3. Inspect the bound source states and service a bounded amount of owner,
   provider, demand or other host work that may make them progress.
4. If required work remains actionable, or a service budget was exhausted,
   schedule another turn. Otherwise call `route.changed_since(stamp)`.
5. Recheck if it changed; only then enter the combined native wait with a finite
   deadline.

Resetting the native event after the final state check can erase the one signal
from a racing publisher. The wake primitive must retain a signal produced after
that check until the host consumes it. The route coalesces changes while pending;
one wake can represent several publications. A signal is a request to inspect
authoritative state, never permission to assume a result or reserve capacity.

The finite deadline also lets the host discover a signal failure and service
other obligations even if a wake is delayed. `std::thread::unpark` is a simple
durable token for a single-thread host; the runnable
[host notification example](../examples/host_notifications.rs) shows its
drain, arm, recheck and park sequence. A native event loop should use its own
documented drain/reset operation and keep all inputs that can wake its wait
under one consumption owner.

## Close and release the signal context

`route.close()` stops new bindings and signal claims, and detaches its sources.
It does not wait for a publisher that already claimed an invocation.
`route.is_quiescent()` is true only after closure and all queued or running
invocations have retired. Keep a native handle valid through that boundary
before releasing it. Dropping a route requests close without a blocking join.
Runtime shutdown does not close routes automatically; a route can observe
shutdown progress until the host closes it.
The signal closure owns its captures until route closure and claimed invocations
retire. Capturing the runtime itself can form a retention cycle through the
runtime's notification storage; explicitly close such a route to release it.
Adapter capture destruction also runs outside Solworker bookkeeping locks.

Task completion means its invocation and captures settled. Group completion
means its sealed members settled. Owner callbacks still need an owner pump, and
GPU use still needs the renderer's exact fence or completion proof. See
[ownership](ownership.md) and [rendering](rendering.md) for those boundaries.

## Measure the adapter path

Run `cargo bench --bench notifications` for a retained-wave CPU workload with
disabled, enabled-idle, narrow-completion and broad-progress routes. Each bound
mode compares a counting sink with `std::thread::unpark`, a latched host signal.
The benchmark reports setup time, per-frame tail percentiles, binding and signal
counts, plus the same work checksum for each mode. It also prints focused
fan-out, coalescing, rapid-rearm and successor/group/owner-capacity timing rows.
Set `SW_NOTIFY_BENCH_MODE=NarrowNative` (or another printed mode name) to profile
one frame mode. `SW_RUN_OUTPUT_DIR` receives raw frame samples when set. These
measure CPU notification costs in the synthetic fixture; host wait behavior and
deadline choices still need measurement in the integrating application.
