# Changelog

All notable changes to Context Fabric are recorded here.

## [Unreleased]

### Changed

- Corrected CRC-32C finalization to the standard Castagnoli result
  (`"123456789"` -> `0xE3069283`). Because checksums are present in every
  wire frame and both durable formats, protocol, persistent-record, and
  coordinator-store format versions are all bumped from 1 to 2. Version-1
  peers and files are intentionally rejected rather than mixed silently.
- Process launching now preserves Windows arguments exactly, owns descendants
  in a kill-on-close job/process group, treats a zero-duration wait as a poll
  on every platform, and confirms bounded tree termination.
- Numeric IPv4 connects bypass name service on every platform, and Windows
  hostname lookup now shares the caller's connect deadline. POSIX hostname
  lookup retains the system resolver deadline because `getaddrinfo` has no
  portable cancellation API.
- The local-fabric launcher uses an ephemeral coordinator port without a
  bind/close race, waits for actual node-registration events, rejects zero or
  excessive node counts and unknown workloads, and cleans unique temporary
  roots after every exit path.
- CLI parsing now rejects unknown, duplicate, missing, overflowing, and invalid
  values instead of silently substituting defaults. `node --store` defaults to
  a 1 GiB persistent budget; explicit CUDA requests fail when unavailable.
- Benchmarks now fail the suite when a measured invariant fails, use
  sub-millisecond timing for throughput, own unique temporary state, and no
  longer advertise ignored external-coordinator or no-op multi-process modes.
- Standalone examples use collision-free owned temporary directories and are
  run as bounded CTest cases.
- CUDA architecture selection defaults to the native build-host GPU instead
  of nvcc's legacy compiler default; deployment architectures can be set with
  `CONTEXT_FABRIC_CUDA_ARCHS`.
- FlashTier remains an interface boundary only. Enabling the unwired adapter
  now fails at configure time instead of silently returning `nullptr`.

## [1.0.0] - 2026-08-07

Initial release implementing the v1 scope.

### Added

- **Core object model**
  - `ContextObjectId` (stable 128-bit identity, deterministic and random),
    `Generation`, `NodeId`, `NodeAddress`.
  - Workload-independent `ContextMetadata` with semantic classes, sharing and
    persistence policies, and strict compatibility checking via a
    compatibility fingerprint.
  - Explicit `LifecycleState` machine with every legal transition declared and
    enforced; illegal transitions fail with typed errors.
  - Lineage records that survive persistence and migration.
- **Transport & protocol**
  - Framed TCP transport with mandatory checksums, bounds checking,
    partial-read/write handling, timeouts, and malformed-frame rejection.
  - Versioned binary wire protocol (control + data planes) with request ids
    and typed results.
  - Same-connection duplicate suppression for mutating requests via a bounded
    `(connection, request-id)` response cache.
- **Distributed authority**
  - Single configured metadata coordinator with durable atomic snapshot
    commits, coordinator epochs, node sessions, and lease epochs/tokens.
  - Read/write leases with expiry, relinquishment, conflict denial,
    and stale-lease rejection.
  - Deterministic recovery on node loss, node restart, and coordinator
    restart; replica promotion only through authority transfer.
- **Operations**
  - Object create (with client upload), write (generation advance under a
    write lease), inspect, list, delete, lineage.
  - Replication, migration (failure-safe with an explicit commit boundary,
    including same-node domain migration), persist, restore, verify.
- **Storage**
  - Bounded `HostMemoryBackend` with high-water accounting.
  - Versioned, checksummed `LocalPersistentBackend` with atomic commit and
    corruption quarantine.
  - CUDA accelerator backend with runtime capability discovery, private stream
    allocation, host/device transfers, round-trip integrity verification, and a
    device-native checksum kernel.
- **Planning**
  - Deterministic placement engine (baseline + cost-aware) with stable
    tie-breaking and auditable decision reasons.
  - Transfer-versus-recompute planner with configurable weights and reuse
    counts.
- **Telemetry**: local JSON Lines, versioned schema.
- **CLI**: `context-fabric` with strict argument parsing, typed exit codes, and
  subcommands for coordinator, node, fabric launcher, objects, leases,
  replication, migration, persistence, verification, placement, decisions, and
  benchmarks.
- **Local fabric launcher**: `context-fabric fabric local --nodes N` runs real
  coordinator + node OS processes over localhost TCP, runs a workload, and
  shuts everything down cleanly.
- **Tests**: native CPU and optional CUDA tests covering identity,
  compatibility, the state machine, protocol framing and fuzzing, storage and
  corruption, placement, leases, authority, replication, migration,
  persistence, recovery, same-connection duplicate suppression, and the CUDA
  backend.
- **Benchmarks**: deterministic seeded suite (local reuse, remote reuse,
  replication, migration, persist/restore, placement, transfer-vs-recompute,
  hot/cold, failure recovery, capacity pressure, CUDA round trip, CUDA remote
  path).
- **Examples**: minimal object, local multi-node, migrate state, persistent
  state, CUDA state, reuse vs recompute.

### Fixed

- Header checksum validation (computed with the crc field zeroed).
- Pending-op use-after-free when aborting/committing from the pending map.
- Object-id argument-evaluation-order bug in deserializers.
- `ObjectId::to_hex` buffer overflow.
- Persistent-store recovery directory-iterator invalidation.
- Illegal `Available -> Recovering` transition under node disconnect.
- Verify op missing `created_ms`, causing premature sweep timeout.
- Same-node migrate and self-load restore commit conditions.
- Windows socket shutdown race (use `shutdown(SD_BOTH)` to wake blocked
  sends/recvs).
- nvcc / MSVC `/FS` flag conflict for the CUDA target.
- Intermittent `std::terminate` (fail-fast) in examples that raced node
  registration: an immediate create/restore/migrate/replicate could run before
  the in-process node registered with the coordinator, throwing `NotFound` out
  of `main`. Examples now retry while the node registers.
- `fabric local --nodes 1` out-of-bounds access on the node-id vector when the
  workload unconditionally used the third node slot.
- Spawned coordinator/node child processes could open their own console
  windows; children now use `CREATE_NO_WINDOW`.
- CLI benchmark command now actually runs the benchmark suite (previously a
  stub), accepts `--quick`/`--cuda`/`--size`, and builds cleanly even when
  `CONTEXT_FABRIC_BUILD_BENCHMARKS` is OFF.

### Known limitations (v1)

- Single configured coordinator, not replicated consensus (documented in
  ARCHITECTURE.md).
- Validation on one machine with multiple real processes over localhost TCP;
  multi-host validation not yet performed.
- Trusted-cluster trust model; no authentication/encryption yet.
- Transfer throughput uses stop-and-wait chunked blocks (sufficient for local
  validation; pipelining/generic-IO is future work).
