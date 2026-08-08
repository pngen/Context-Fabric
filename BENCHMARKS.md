# Benchmarks

This document defines the exact methodology and reporting rules for the
Context Fabric benchmark suite.

## Methodology

- **Deterministic and seeded.** All object content is generated from fixed
  seeds by the project's deterministic PRNG (`generate_content`). Re-running a
  benchmark with the same configuration and machine produces the same bytes.
- **Exact configuration printed first.** Every benchmark prints its name and
  its configuration (object size, CUDA on/off) before it runs.
- **Safe bounded defaults.** The default object size is 16 MiB. `--quick`
  reduces sizes and iteration counts. No benchmark allocates dangerous
  proportions of RAM, VRAM, or disk by default. Larger runs require explicit
  overrides (`--size`).
- **Real bytes, real paths.** CPU benchmarks move actual bytes over real TCP
  sockets between real node processes (in-process fabric by default, or a
  running fabric via `--coordinator`). CUDA benchmarks use real device memory
  on the RTX 5090.
- **Every claimed number corresponds to code that ran.** There are no
  fabricated comparisons against LMCache, Mooncake, Dynamo, vLLM, Redis, Ray,
  or any external system. Context Fabric benchmarks itself against its own
  baselines only.

## Reporting rules

- Each benchmark reports measured values (latency, throughput, bytes) and the
  decision/outcome where relevant.
- Throughput is computed as `bytes / seconds` from the measured duration.
- The suite does not claim mathematical optimality for the cost-aware
  placement heuristic, and does not claim real-model speedups from synthetic
  workloads.

## Suites

| Suite | What it measures |
| --- | --- |
| LOCAL OBJECT REUSE | Latency of serving a hot object from the authoritative node's host memory; the synthetic recomputation cost it avoids. |
| REMOTE REUSE | Latency of serving a validated replica on a second node over TCP. |
| REPLICATION | End-to-end replicate latency and throughput for a fresh object per iteration. |
| MIGRATION | Migration latency and throughput at several object sizes. |
| PERSIST/RESTORE | Durable write latency, restore latency, and write throughput against the local persistent store. |
| PLACEMENT | Determinism and correctness of the placement policy over a fixed candidate topology. |
| TRANSFER VS RECOMPUTE | The planner choosing transfer for expensive state and recompute for cheap state. |
| HOT/COLD STATE | The planner preferring transfer for hot (high-reuse) state and recompute for cold state. |
| FAILURE RECOVERY | Node-loss recovery: deterministic promotion of a live replica and verification of the promoted copy. |
| CAPACITY PRESSURE | Graceful rejection of an oversized put at a bounded capacity. |
| CUDA ROUND TRIP | Host -> RTX 5090 -> host byte-exact round trip with integrity verification. |
| CUDA REMOTE PATH | Node B -> node A host -> CUDA migration with verification. |
| MULTI-PROCESS FABRIC | Covered by `context-fabric fabric local --nodes 3` and the multi-process integration test. |

## Reporting baseline (reference results)

The table below is a reference from the development machine (NVIDIA RTX 5090,
64 GB RAM, AMD Ryzen 7 9800X3D, Windows, CPU-only build). These are illustrative
runs, not speed guarantees; re-run on the target machine for authoritative
numbers.

| Suite | Quick result (approx) |
| --- | --- |
| LOCAL OBJECT REUSE | ~5 ms per deep verify of 2 MiB (includes full SHA-256 re-hash) |
| REMOTE REUSE | ~5 ms per deep verify of 2 MiB replica |
| REPLICATION | ~36 MB/s end-to-end for 4 MiB objects (stop-and-wait, single round trip) |
| MIGRATION | ~36-40 MB/s end-to-end for 1-16 MiB objects |
| PERSIST/RESTORE | sub-second for 4 MiB host<->SSD |
| PLACEMENT | deterministic; reason recorded |
| FAILURE RECOVERY | replica promoted after node loss; promoted copy verified |
| CUDA ROUND TRIP | byte-exact; device integrity verified |
| CUDA REMOTE PATH | verified migration into the CUDA node's accelerator domain |

## How to run

```sh
context-fabric benchmark --quick                 # via the CLI; CPU-only, fast
context-fabric benchmark --cuda                  # include CUDA suites
context-fabric benchmark --coordinator 127.0.0.1:9000 --cuda   # against a running fabric
context_fabric_bench --quick                     # standalone runner
```