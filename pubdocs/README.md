# Solworker usage guide

Solworker is a Rust CPU execution library for applications that combine recurring
parallel work, background preparation and thread-bound publication. It provides
three separately configured worker pools, borrowed parallel operations, owned
jobs, dependency groups, bounded admission and explicit lifetime boundaries.

The application supplies the work graph and owns its domain state. Solworker
executes eligible CPU work and accounts for the responsibilities attached to it.
This makes the same mechanisms useful for an interactive renderer, editor,
simulation or background processing pipeline.

## Read the guide

| Chapter | Questions it answers |
| --- | --- |
| [Execution and work graphs](execution.md) | How do I configure pools, choose borrowed or owned work, overlap stages and help a group? |
| [Ownership, results and publication](ownership.md) | What does completion permit? How do several consumers reuse a result? What runs on the main thread? |
| [Resources, demand and capacity](resources.md) | How do loading, caches, child discovery, mutable urgency and backpressure connect? |
| [Rendering and external access](rendering.md) | How do preparation, recording, submission, frame overlap and GPU retirement fit together? |
| [Progress, failure and shutdown](lifecycle.md) | Who makes progress, how do failures propagate, and when is teardown complete? |
| [Performance and qualification](performance.md) | How should work be organized and measured without hiding costs or changing useful work? |

The runnable [frame pipeline example](../examples/frame_pipeline.rs) demonstrates
groups, early successor admission, immutable result reuse and owner publication.
The [resource pipeline example](../examples/resource_pipeline.rs) demonstrates a
provider result, child admission after sealing, consumer demand and retained bytes.
Their sizes and worker counts are instructional, not tuning recommendations.

From a checkout:

```sh
cargo run --example frame_pipeline
cargo run --example resource_pipeline
cargo doc --no-deps --open
```

The checkout pins its development toolchain in
[rust-toolchain.toml](../rust-toolchain.toml). The package's declared Rust version
and dependencies are in [Cargo.toml](../Cargo.toml). Use the API reference built
from the same revision as this guide; the public surface is still evolving.

## Responsibility boundaries

| Owner | Responsibilities |
| --- | --- |
| Solworker | CPU execution classes; admission; prerequisite activation; task/group status; explicit helping; owner-delivery transport; demand and declared-capacity accounting. |
| Application | Work partitioning; clocks and semantic ordering; mutable job recovery; domain validation; native event loop; overall thread budget. |
| Cache/source service | Keys; versions; producer deduplication; file/network service; residency and eviction; deciding which consumers still need data. |
| Renderer/device adapter | Command pools; resource barriers; queue and presentation ownership; fences/completion values; device errors and physical retirement. |

Public crate types use the `SW` prefix. Cache and renderer types belong to their
own layers; a host can use `SC` and `SR` prefixes for those APIs. Those prefixes
do not imply that Solworker implements a cache or graphics backend.

## Available mechanisms and application patterns

This guide distinguishes three levels:

- **Available API:** exported by this checkout. API names link to source when a
  chapter needs exact details; runnable examples use only these APIs.
- **Application pattern:** a way to compose existing APIs with domain-owned
  storage or state machines. Pseudocode uses text blocks and is not a new API.
- **Proposed capability:** not callable today. The principal integration gap is
  a runtime-wide native wake hook, described in [progress](lifecycle.md).

`SWBatch` already supports successive group generations. It does not provide a
typed mutable-job store, atomic bulk submission or a reusable compiled graph.
Those distinctions matter when adapting an existing engine. A host may need a
small adapter for retained job state and coherent publication; it should reuse
Solworker's scheduler rather than maintain another runnable queue.

The guide describes intended usage and current contracts. It does not establish
a universal worker split, maximum frame time, scheduler-overhead budget, fairness
deadline or measured speedup for a particular application.
