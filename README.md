# Context Fabric

**Version 1.0.0** — a vendor-neutral, distributed runtime for reusable AI
computational state.

Context Fabric answers the question:

> Where should accumulated reusable computational state live across an
> inference or agent fabric, who owns it, which copies are valid, how should it
> move, when should it persist, and when is moving it cheaper than recomputing it?

It treats reusable computational state itself as a first-class distributed
systems object: globally stable identity, explicit ownership, replicas,
migrations, durable persistence, integrity verification, placement, and
transfer-versus-recompute decisions — independent of any one model framework,
GPU vendor, or inference runtime.

KV cache is **one supported state class**, not the abstraction. Context Fabric
also manages prompt/prefix state, recurrent state, tensors, embeddings,
retrieval state, agent working state, tool artifacts, checkpoints, and
arbitrary application-defined computational state.

## Why "computational state", not "KV cache"?

Inference-serving caches answer "how do I move this prefix's KV blocks between
GPU and CPU fast?" That is a real and hard problem, but it is narrower than
the one a reusable-state fabric must solve. A modern inference or agent system
accumulates many kinds of reusable state — KV blocks, embeddings, retrieved
context, fine-tuned adapter weight, agent scratchpads, tool outputs,
checkpoints — each with its own size, lifetime, mutability, and privacy
properties. A runtime that treats only KV blocks as first-class cannot answer
"should I ship this 2 GiB checkpoint to the new node or recompute it?".

Context Fabric's core is workload-independent. Framework-specific and
model-specific adapters sit *above* the core abstractions.

## What Context Fabric is not

- Not a thin wrapper around an existing KV-cache library.
- Not a clone of LMCache, Mooncake, vLLM KV offload, or NVIDIA Dynamo.
- Not a compute scheduler. Execution placement belongs to a future sibling
  project (Compute Fabric). Context Fabric exposes scheduling *inputs* —
  locality, transfer cost, recomputation cost — but does not place compute.
- Not a replacement for FlashTier. FlashTier is a sibling project one
  abstraction below: it decides where a physical working page lives on *one*
  machine. Context Fabric builds on top of that boundary.

## Core concepts

- **ContextObjectId** — a stable 128-bit identity, independent of process,
  host, or physical residency. Deterministic creation is supported; objects
  keep their identity across migration and restart.
- **Generation** — every mutation creates or advances an explicit version. An
  object is addressable as `(ContextObjectId, Generation)`.
- **Metadata & compatibility** — immutable compatibility descriptors
  (producer, model, revision, tokenizer, attention layout, tensor shape/dtype,
  quantization, namespace, tags). Reuse is refused when required descriptors
  differ, never silently.
- **Ownership & leases** — at most one writable authoritative generation
  exists at a time. Read and write leases are explicit, time-limited, and
  carry the coordinator epoch, so a stale or restarted node cannot use an old
  lease to mutate state after authority has changed.
- **Replicas** — validated read-only copies of an exact generation, created
  and invalidated under explicit coherence rules. A stale replica can never
  silently become authoritative.
- **Migration** — first-class, failure-safe. The destination verifies the
  bytes, the metadata authority atomically switches the authoritative location,
  and only then is the old copy demoted or released. A failed migration never
  destroys the last valid authoritative copy.
- **Persistence** — versioned, checksummed on-disk records with atomic
  commit; torn, truncated, or accidentally corrupted records with mismatched
  checksums are detected and quarantined.
- **Integrity** — CRC-32C plus the first 64 bits of SHA-256 at important
  boundaries. Transfers, replication, migration, persistence, restoration,
  and CUDA round trips compare both values. These are unkeyed corruption
  checks, not authentication against a malicious writer.
- **Placement** — a deterministic engine (baseline and cost-aware policies)
  with stable tie-breaking; every decision records its inputs and reason.
- **Transfer vs recompute** — a planner that chooses between shipping state
  and recomputing it from supplied estimates, recording every decision.
- **Lineage** — inspectable derivation history that survives persistence and
  migration.

## Architecture at a glance

```
                 +----------------------+
                 |  Coordinator (dur.)  |   metadata authority: nodes, objects,
                 |  epochs, leases,     |   replicas, persistence locations,
                 |  registry, idempotency|  generations, integrity
                 +----------+-----------+
                            |  control plane (TCP, framed, checksummed)
              +-------------+-------------+
              |             |             |
        +-----+-----+ +-----+-----+ +-----+-----+
        |  Node A   | |  Node B   | |  Node C   |
        | CUDA GPU  | |  host+ssd | |  host+ssd |
        |  CUDA     | |  HostMem  | |  HostMem  |
        |  HostMem  | |  Persist  | |  Persist  |
        +-----------+ +-----------+ +-----------+
              |             |             |
              +------ data plane (node-to-node TCP) ------+
```

Nodes are independent processes. On a development machine they run as real OS
processes over localhost TCP; the same protocol runs over real network
interfaces on a cluster.

## Reproducible local validation

```
context-fabric         # CLI
context-fabric fabric local --nodes 3               # real CPU multi-process fabric
context-fabric benchmark --quick                    # benchmark suite via CLI
# Add --cuda-node / --cuda only on a CUDA build with a usable device.
ctest --preset ...                                  # native tests
```

## Build

Requirements: a C++20 compiler, CMake >= 3.24, Ninja (optional but recommended).
CUDA toolkit is optional.

```sh
git clone <this repository>
cd Context-Fabric

# CPU-only
cmake --preset cpu-release
cmake --build build/cpu-release

# CUDA-enabled (native build-host architecture by default)
cmake --preset cuda-release
# For a release artifact, set the deployment architectures explicitly:
cmake --preset cuda-release -DCONTEXT_FABRIC_CUDA_ARCHS="89;90;120"
cmake --build build/cuda-release
```

Configuration options:

| Option | Default | Meaning |
| --- | --- | --- |
| `CONTEXT_FABRIC_ENABLE_CUDA` | OFF | Enable CUDA; explicit `ON` requires a usable toolkit/compiler |
| `CONTEXT_FABRIC_CUDA_ARCHS` | `native` | CUDA architectures to compile for; set an explicit deployment list for distributable artifacts |
| `CONTEXT_FABRIC_BUILD_TESTS` | ON | Build native tests |
| `CONTEXT_FABRIC_BUILD_BENCHMARKS` | OFF | Build the benchmark suite |
| `CONTEXT_FABRIC_BUILD_EXAMPLES` | ON | Build runnable examples |
| `CONTEXT_FABRIC_BUILD_CLI` | ON | Build the `context-fabric` CLI |
| `CONTEXT_FABRIC_WARNINGS_AS_ERRORS` | ON | Warnings-as-errors for release validation |
| `CONTEXT_FABRIC_ENABLE_FLASHTIER` | OFF | Reserved; `ON` fails because no concrete adapter is wired |
| `CONTEXT_FABRIC_FLASHTIER_DIR` | (empty) | Reserved path for a future FlashTier integration |

## Test

```sh
ctest --preset cpu-tests            # CPU-only tests
ctest --preset cuda-tests           # CUDA-enabled tests
```

See `tests/` for the native suite (project-owned harness) and `ARCHITECTURE.md`
for what is covered.

## Run

Start a fabric manually:

```sh
context-fabric coordinator --port 9000 --store ./coord.store
context-fabric node --coordinator 127.0.0.1:9000 --id node-a --store ./node-a --host-cap 4GiB
context-fabric node --coordinator 127.0.0.1:9000 --id node-b --store ./node-b --host-cap 4GiB
```

When `--store` is present without `--persist-cap`, the CLI uses a 1 GiB local
persistent-store budget. Set `--persist-cap` explicitly for other budgets.

Or let the launcher manage a complete multi-process fabric:

```sh
context-fabric fabric local --nodes 3 --retain --root ./fabric
```

Then use the CLI:

```sh
context-fabric object create --coordinator 127.0.0.1:9000 --namespace app \
    --name weights --class checkpoint --size 1GiB --owner node-a
context-fabric object inspect --coordinator 127.0.0.1:9000 --id <id>
context-fabric replicate --coordinator 127.0.0.1:9000 --id <id> --to node-b
context-fabric migrate  --coordinator 127.0.0.1:9000 --id <id> --to node-b
context-fabric persist   --coordinator 127.0.0.1:9000 --id <id>
context-fabric verify    --coordinator 127.0.0.1:9000 --id <id> --deep
context-fabric decide    --coordinator 127.0.0.1:9000 --namespace app --name weights \
    --size 1GiB --current node-a --target node-b --recompute-sec 30
```

## Benchmark

```sh
# In-process fabric (fast, safe defaults) via the CLI
context-fabric benchmark --quick

# Full run with CUDA round-trip and CUDA remote path benchmarks
context-fabric benchmark --cuda

# Standalone runner (built when CONTEXT_FABRIC_BUILD_BENCHMARKS=ON)
context_fabric_bench --quick
```

See `BENCHMARKS.md` for methodology and reporting rules.

## Examples

`examples/` contains runnable programs: `example_minimal_object`,
`example_local_multi_node`, `example_migrate_state`,
`example_persistent_state`, `example_cuda_state`, and
`example_reuse_vs_recompute`.

## CUDA support

The CUDA backend enumerates devices at runtime, reports capability and memory,
allocates on a private stream, performs host<->device transfers, and verifies
round-trip integrity (including a device-native checksum kernel). CUDA tests
and `benchmark --cuda` require a real usable device; CPU-only builds need no
CUDA toolkit.

## FlashTier relationship

Context Fabric is a sibling project one abstraction above FlashTier. A narrow
interface in `storage/flashtier_adapter.h` documents the intended boundary,
but this repository does not wire a concrete adapter. Context Fabric builds
and runs without FlashTier; requesting the unwired integration fails at CMake
configure time. See `ARCHITECTURE.md` for the exact boundary.

## License

Apache License 2.0. Copyright 2026 Summon Software Labs. No telemetry transmission.
