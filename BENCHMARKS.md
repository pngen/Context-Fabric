# Benchmarks

This document defines the methodology and reporting rules for the Context
Fabric benchmark suite.

## Methodology

- **Deterministic input bytes.** Object content comes from fixed seeds through
  the project PRNG. Timing remains dependent on the host and current load.
- **Configuration before results.** Each case prints its name, byte size, and
  whether CUDA suites were requested before measured output.
- **Bounded defaults.** The default object size is 16 MiB. `--quick` reduces
  object sizes and iteration counts. A larger `--size` is an explicit request
  by the caller.
- **Owned runtime state.** Runtime cases create a fresh in-process coordinator
  and `NodeRuntime` instances, communicate over real TCP sockets, enforce a
  10-second registration deadline, and remove their unique temporary state on
  teardown. These are not separate OS processes; multi-process coverage comes
  from the bounded `context_fabric_cli_fabric_local` CTest.
- **Real CUDA or failure.** `--cuda` runs against a real device allocation and
  returns a failing status when the build or host cannot provide CUDA. It does
  not silently substitute host memory or report a skipped suite as success.
- **Truthful suite status.** Every printed success corresponds to code that
  ran. Failed invariants and exceptions make the overall runner return
  nonzero. The suite does not fabricate comparisons with external systems.

## Reporting rules

- Timed cases use `std::chrono::steady_clock` with sub-millisecond resolution.
- Throughput is `bytes / measured seconds`; a zero/invalid interval fails the
  corresponding case instead of producing infinity.
- Synthetic recomputation values are labeled as estimates, not measured model
  execution or speedups.
- The placement heuristic is a deterministic baseline, not a claim of
  mathematical optimality.

## Suites

| Suite | What it exercises |
| --- | --- |
| LOCAL OBJECT REUSE | Repeated deep integrity verification of the authoritative node's host-memory object. This is not application serving latency. |
| REMOTE REUSE | Coordinator-mediated deep verification of a replica on a second node after a real TCP replication. This is not transfer latency per hit. |
| REPLICATION | End-to-end replicate latency and throughput for a fresh object per iteration. |
| MIGRATION | End-to-end migration latency and throughput across several object sizes. |
| PERSIST/RESTORE | Durable write latency, restore latency, and write throughput against an owned local store. |
| PLACEMENT | Repeatability of the placement decision over a fixed candidate topology. |
| TRANSFER VS RECOMPUTE | Transfer for the configured expensive case and recompute for the configured cheap case. |
| HOT/COLD STATE | Transfer for the high-reuse case and recompute for the otherwise-identical single-use case. |
| FAILURE RECOVERY | Node loss, deterministic live-replica promotion, and verification of the promoted copy. |
| CAPACITY PRESSURE | Typed rejection of an oversized host-memory put. |
| CUDA ROUND TRIP | Host-to-device-to-host byte equality plus CRC-32C and a 64-bit SHA-256 fingerprint on the discovered CUDA device. |
| CUDA REMOTE PATH | TCP migration from another node into the CUDA node's accelerator domain followed by verification. |

## How to run

```sh
context-fabric benchmark --quick       # CLI, CPU suites
context-fabric benchmark --cuda        # CPU + required real-CUDA suites
context_fabric_bench --quick           # standalone runner
```

The values printed by the run on the target host are the authoritative
results; this repository does not embed a machine-specific performance table.
