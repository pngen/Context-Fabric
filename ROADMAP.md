# Roadmap

This file separates **implemented** work from **future** work. Items in the
future sections are not current features and are never marketed as such.

## Implemented (v1.0.0)

- Global object identity (deterministic and random), generations, lineage.
- Workload-independent metadata model with strict compatibility checking.
- Explicit lifecycle state machine with typed transition enforcement.
- Framed TCP transport with checksums, bounds, partial-I/O handling, and
  deterministic malformed-frame rejection.
- Single configured metadata coordinator with durable snapshot commits,
  coordinator epochs, node sessions, and lease epochs/tokens.
- Read/write leases with expiry, relinquishment, conflict denial,
  stale-lease rejection, and node-disconnect cleanup.
- Multi-process local fabric: coordinator + N real node processes over
  localhost TCP.
- Replication and coherence (validated read-only replicas, invalidation,
  promotion only through authority transfer).
- Failure-safe migration with an explicit commit boundary.
- Durable versioned persistence with checksums, atomic commit, quarantine.
- Restore and deterministic recovery (node loss, node restart, coordinator
  restart).
- Integrity at important boundaries (CRC-32C plus a 64-bit fingerprint derived
  from SHA-256; this is corruption detection, not authentication).
- Deterministic placement (baseline + cost-aware) with stable tie-breaking.
- Transfer-versus-recompute planner with auditable decisions.
- Host memory, local persistent, and CUDA accelerator backends.
- Telemetry (JSON Lines, versioned schema); local only.
- CLI with strict parsing, typed exit codes, and machine-readable output.
- Native tests, benchmark suite, and runnable examples.

## Future work

Under consideration; not yet implemented.

### Distributed / protocol
- **Real multi-host validation** across physical machines.
- **RDMA / GPUDirect** transport paths.
- **Shared-memory IPC** transport for same-host benchmarking.
- **Distributed consensus backend** (Raft/etcd-style) replacing the single
  configured coordinator, keeping ContextObject semantics unchanged.
- **Authenticated / encrypted transports** (the current trust model is an
  explicit trusted-cluster network).

### Hardware / media
- **CXL memory** domains.
- **HIP / ROCm**, **Level Zero**, **Metal** accelerator backends.
- **Object-store backends** (S3-compatible, etc.) for remote persistent
  storage.
- **GPUDirect storage** (NVMe-CUDA) integration.

### Framework adapters
- **llama.cpp adapter**, **vLLM adapter**, **SGLang adapter**, native
  framework adapters mapping real KV/prefix/intermediate state onto the core
  object model.
- **Real KV-cache adapters** with per-layer layout, quantization, and
  compression metadata.

### Runtime behavior
- **Multi-GPU nodes** with per-device placement.
- **Compression / quantization-aware** residency and transfer.
- **Advanced learned placement** (e.g., learned reuse prediction) atop the
  deterministic baseline.
- **Compute Fabric integration** — Context Fabric exposes scheduling inputs
  (locality, transfer cost, recomputation cost); a sibling Compute Fabric
  consumes them for execution placement. This repository does not implement
  compute scheduling.

### Reuse / economics
- **Cost-aware eviction** with transfer-versus-recompute-aware victim choice.
- **Automatic hot/cold promotion** driven by measured access telemetry.
