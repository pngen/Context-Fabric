# Architecture

This document is written for a systems engineer who wants to understand every
core invariant, the control plane, the data plane, the protocol, recovery, and
the exact guarantees Context Fabric provides.

## 1. Problem and scope

Context Fabric manages reusable computational state as a first-class
distributed systems object. It answers: where should accumulated reusable
state live, who owns it, which copies are valid, how should it move, when
should it persist, and when is moving cheaper than recomputing.

The core is workload-independent. KV cache is one supported semantic class
among many. Framework/model adapters live above the core.

## 2. Object model

### 2.1 Identity

`ContextObjectId` is 128 bits (`hi:64`, `lo:64`). It is:
- globally unique for practical runtime purposes (random 128-bit or a
  deterministic hash of `namespace@name`);
- stable across migration and restart (never a process-local pointer or a
  transient handle);
- serializable, comparable, hashable, printable.

`Generation` is a monotonically increasing 64-bit version. An object is
addressable as `(ContextObjectId, Generation)`. Every mutation creates or
advances a generation. At most one writable authoritative generation exists at
a time.

### 2.2 Metadata and compatibility

`ContextMetadata` carries the semantic class and extensible compatibility
descriptors: producer/runtime, model id, model revision/hash, tokenizer,
attention/layout identity, tensor shape/dtype, quantization, architecture
namespace, and user tags. It also carries logical size, physical byte size,
sharing policy, persistence policy, mutability, lifetime, and planning hints
(recompute estimate, resource cost, reuse probability, pinned locality).

A precomputed `compatibility_fingerprint` is used for fast strict comparison.
Two objects that differ in any required compatibility descriptor are never
silently reused.

Sharing policy is explicit: `Private`, `NamespaceShared`, `GloballyShareable`.
Private state must not become globally reusable merely because content hashes
match.

### 2.3 Lifecycle state machine

`LifecycleState`: `Creating, Available, LeasedRead, LeasedWrite, Replicating,
Migrating, Persisting, Restoring, Invalidating, Recovering, Corrupt, Expired,
Released`.

`LifecycleAuthority` is the only permitted mutator of lifecycle state. Every
legal transition is declared in a table; illegal transitions throw
`CfError(State)`. The table is unit-tested directly.

Examples of enforced invariants:
- `Creating -> Available` (publish), `Creating -> Released` (abort).
- `Replicating/Migrating/Persisting/Restoring -> Available` (commit or abort).
- `Available -> Recovering` (authority lost when a node dies).
- `Recovering -> Available` (authority restored) or `Recovering -> Restoring`.
- `Released` is terminal.

## 3. Coordinator (metadata authority)

### 3.1 Role

The coordinator is the single configured authority for the local fabric. It
tracks node membership, capabilities, object metadata, authoritative location,
replica locations, persistence locations, leases, generations, and health.

### 3.2 Epochs and stale authority

- `coordinator_epoch`: a 32-bit epoch, bumped on every coordinator restart,
  persisted in the durable store.
- `node session`: each registration gets a `session_id` and `session_epoch`;
  heartbeats must cite them.
- Every lease token embeds `(coordinator_epoch, lease_generation, lease_id)`.
  A lease is honored only when its epoch equals the current coordinator epoch
  and its lease generation equals the object's current lease generation.
  Therefore a stale node or a pre-restart lease can never authorize mutation.

### 3.3 Durability

The coordinator persists a versioned snapshot (`CoordinatorStore`) atomically
via staging file + rename. Only *committed* state is written. In-flight
operations are never durable; on restart they are deterministically aborted to
the last committed state. This is what makes crash recovery deterministic.

### 3.4 Guarantees (and non-guarantees)

Provided:
- exactly one writable authority per object at a time;
- lease tokens are epoch-bound and stale leases are rejected;
- conflicting owners are rejected with typed errors;
- durable metadata is recovered deterministically;
- in-flight operations are never committed partially.

Not provided (by design, documented):
- Byzantine fault tolerance;
- automatic failover / multi-region consensus;
- authentication or encryption (the v1 trust model is an explicit
  trusted-cluster network).

The protocol is designed so a Raft/etcd-style consensus backend can replace
the coordinator without changing ContextObject semantics.

### 3.5 Concurrency

- One accept thread, bounded per-connection reader threads, a bounded driver
  pool, and one sweeper.
- All coordinator state is guarded by `state_mutex_`; connection maps by
  `conn_mutex_` (a leaf lock). The idempotency cache has its own mutex; the
  cache-hit handler copies the cached payload out before releasing the lock, so
  no lock is ever re-entered.
- Multi-node operations are serialized per object via an explicit
  active-operation marker; a second operation on the same object is rejected
  with `Conflict`.

## 4. Protocol

### 4.1 Framing

Every message is a framed binary record over TCP:

```
offset  size  field
0       4     magic 0x41524643 ("CFRA")
4       2     protocol version
6       2     flags (checksum flag mandatory)
8       2     message type
10      2     reserved
12      8     request id
20      8     node id (sender)
28      4     payload length
32      4     payload crc32c
36      4     header crc32c (over bytes 0..40 with the crc field zeroed)
40      N     payload
```

- All integers big-endian; lengths are bounded (`256 MiB` payload cap).
- The header checksum is computed over the header with the crc field zeroed,
  so no length field can be modified undetected.
- `recv_frame` never trusts a length from the wire; it validates magic,
  version, checksum flag, payload cap, then the header checksum, then drains
  the exact payload and verifies its CRC.
- Partial reads and partial writes are handled explicitly (loops over
  `recv`/`send`); one `send()` is never assumed to be one message.

### 4.2 Message families

- Control plane (client/node -> coordinator): hello, node register/heartbeat/
  shutdown, object create/write/inspect/list/delete, lineage, lease
  acquire/release, replicate/migrate/persist/restore/verify, placement and
  decision queries, node-shutdown request.
- Instructions (coordinator -> node): `InstructSend`, `InstructRecv`,
  `InstructVerify`, `InstructAbort`, `InstructPromote`, `InstructDispose`,
  `InstructShutdown`.
- Data plane (node <-> node, client -> node): `DataBlock` / `DataBlockAck`
  chunked transfer with stop-and-wait acks and end-to-end CRC-32C plus a
  64-bit SHA-256 fingerprint.

### 4.3 Idempotency

Mutating requests carry a request id. The coordinator caches responses for the
last 4096 `(connection, request_id)` pairs; a duplicate on that same live TCP
connection is served from the cache and is not re-executed. Connection identity
is not stable across reconnects, so a request retried after a lost connection
is not deduplicated by this cache.

## 5. Data plane and transfers

A transfer is a stop-and-wait stream of `DataBlock` frames. Each block carries
the op token, object identity, generation, sequence, total count, total byte
size, and the expected end-to-end CRC-32C/SHA-256 fingerprint. The receiver:
1. reads the first block and waits (bounded) for the pending-receive
   registration;
2. verifies identity, then the capacity bound;
3. accumulates bytes, detects out-of-order or duplicate blocks;
4. on completion computes CRC-32C plus the first 64 bits of SHA-256 and
   compares both against the expected values;
5. only on success stores the copy and reports `DataResult(ok)` to the
   coordinator;
6. never publishes a corrupted copy.

A failed or interrupted transfer is discarded; the coordinator learns of
failure from a `DataResult(fail)` or an operation timeout, and aborts the
operation deterministically.

## 6. Operations

### 6.1 Object create

The client requests creation with metadata and an owner node. The coordinator
creates the record in `Creating`, sends an `InstructRecv` (role
`Authoritative`) to the owner, and returns the id, generation, the owner's
data-plane address, and an op token. The client uploads the bytes; the node
verifies and stores them; the coordinator commits `Creating -> Available` with
the received integrity. Creation is asynchronous for the coordinator but the
public API waits for the commit.

### 6.2 Replicate

The coordinator validates the object, target capability, capacity, and that a
replica does not already exist there. It sends `InstructRecv` (role
`Replica`) to the target and `InstructSend` to the source. The source sends
real bytes; the target verifies and stores; the coordinator commits by adding
the target to `replicas` and returning to `Available`. Retries are
deduplicated only when replayed with the same request id on the same live
connection, or otherwise rejected (`AlreadyExists`).

### 6.3 Migrate

Authority transfer with a safety boundary:
1. The coordinator reserves the destination and marks the object `Migrating`.
2. The destination receives (role `Authoritative`) and verifies the bytes.
3. The coordinator atomically switches the authoritative location, bumps the
   object's lease generation (invalidating old lease tokens), and only then
   sends `InstructDispose` to the old source.
4. If anything fails before the authority switch, the source remains valid and
   the operation aborts to `Available`.

Same-node domain migration (host <-> accelerator) is supported; it is a
single-participant transfer and the coordinator commits on the single report.

### 6.4 Persist / Restore

Persist: the coordinator instructs a node to write its authoritative host copy
to its own durable store (self-load, role `Persistent`). The record embeds the
metadata and checksums.

Restore: the coordinator selects a live node holding a durable copy and a
destination. If they are the same node, the node self-loads from its durable
store into host memory (role `Authoritative`); otherwise a normal transfer
occurs. Restore is also the recovery path for objects in `Recovering`.

### 6.5 Verify

The coordinator instructs a node holding a copy to verify its bytes against
the recorded integrity (deep verification reads the physical bytes). This
exercises the host, persistent, and accelerator residency paths.

### 6.6 Write (generation advance)

A write requires a valid, unexpired write lease whose epoch and lease
generation match. The expected next generation must equal the current
generation + 1. The coordinator advances the generation, updates the byte
size, sends an `InstructRecv` for the new generation, and commits the new
integrity.

## 7. Node runtime

A node is an independent process with:
- a host-memory backend (bounded, high-water tracked);
- an optional local-persistent backend (versioned, checksummed, recoverable);
- an optional accelerator backend (CUDA);
- a data-plane listener and a bounded worker pool;
- a control connection to the coordinator (register, heartbeat, instructions);
- its own telemetry stream.

The node's object store tracks which physical copies it holds and their role
(authoritative / replica / persistent / accelerator-resident) and integrity.
Copies are never silently promoted; promotion happens only through an explicit
`InstructPromote` from the coordinator after a deterministic recovery.

## 8. Persistence format

Versioned on-disk record (`version 1`):

```
[0,4)   magic       0x424F4643 "CFOB"
[4,8)   version
[8,12)  header_len  (64 + metadata_len)
[12,16) flags
[16,24) object_id.hi
[24,32) object_id.lo
[32,40) generation
[40,48) data_size
[48,52) data_crc32c
[52,56) header_crc32c (over [0, header_len) with this field zeroed)
[56,64) metadata_len
[64,64+metadata_len) serialized ContextMetadata
then data_size bytes of content
```

Records are committed atomically (staging file + rename). Reads validate
magic, version, header bounds, header checksum, then data checksum. Corrupt
records are quarantined, never accepted. Node identity is encoded as hex in
file names; no path traversal is possible through protocol metadata.

## 9. Crash recovery

- **Coordinator restart**: the epoch bumps; every prior lease is dead; durable
  object records are reconciled: in-flight lifecycle states are aborted to
  `Available` (`Creating` to `Released`); objects whose authoritative node is
  not registered are marked `Recovering`. A node that re-registers restores
  authority for objects it still holds.
- **Node disconnect**: the coordinator aborts in-flight operations that
  reference the node, revokes its leases (bumping lease generations), and for
  objects authoritative on the dead node deterministically promotes the
  lowest-node-id live replica (after validating integrity) or marks the object
  `Recovering`.
- **Node persistent store**: `recover()` scans, validates, and quarantines
  corruption and stale staging files at startup.
- **Incomplete migration/replication** can never create two writable
  authorities: the coordinator's commit is the single authority switch, and it
  happens only after the destination verifies.

## 10. Placement

Context Fabric placement chooses where state should live. It is deterministic:
same state + same metrics + same hints -> same decision, with stable tie-break
by `(score, node_id, domain)`.

- `baseline`: prefer the current location; otherwise most free headroom.
- `cost-aware`: score = transfer time + capacity pressure + reuse term, with a
  stay bonus. The cost-aware heuristic is intentionally simple and is not
  claimed to be mathematically optimal.

Inputs include node capability/headroom, object size, semantic class,
reuse probability, persistence policy, and measured bandwidth/latency. Every
decision records its inputs and reason through telemetry.

## 11. Transfer vs recompute

`TransferVsRecompute` takes application-supplied estimates (recompute time,
recompute resource cost) and path bandwidth, and decides whether fetching
existing state is cheaper than recomputing it, under configurable
time/resource weights and expected reuse count. Every decision records its
reason (e.g. `transfer: cost=... <= recompute cost=...`). The runtime never
executes model recomputation itself; it plans.

## 12. Security and robustness

- Strict frame parsing with length bounds, integer-overflow checks, resource
  limits, and timeouts.
- Filesystem path containment for node stores (hex-encoded identities).
- No unchecked deserialization; the reader is bounds-checked on every field.
- No secret material in telemetry by default.
- Trust model: an explicit trusted-cluster network. Session/identity fields
  are present in the protocol; authenticated/encrypted transports are a
  designed extension point, not a claim of zero-trust.

## 13. FlashTier adapter boundary

`storage/flashtier_adapter.h` defines the narrow interface a node would use to
delegate node-local physical residency to FlashTier (VRAM / pinned host RAM /
NVMe). Context Fabric builds and runs without FlashTier; the default factory
returns `nullptr`. No concrete adapter is wired in this repository, and
requesting `CONTEXT_FABRIC_ENABLE_FLASHTIER=ON` fails during configuration.

## 14. Shutdown

Shutdown is deterministic: a shutdown request stops accepting, wakes blocked
sockets via `shutdown(SD_BOTH)` (reliable on Windows, where closing a socket
from another thread does not reliably wake a blocked send/recv), joins all
worker/control/accept/sweep threads, closes backends, and flushes telemetry.
No detached threads; every thread is owned and joined.

## 15. Testing philosophy

The test suite uses a lightweight project-owned harness. Tests cover object
identity, generations, metadata serialization, compatibility, state
transitions (including illegal ones), coordinator epochs, node registration,
leases (acquire/release/expire/conflict/stale), ownership transfer,
replication, invalidation, migration and migration failure, persistence,
corruption detection, lineage, deduplication safety, namespace isolation,
placement determinism, transfer/recompute decisions, protocol framing,
partial I/O, malformed frames, same-connection duplicate requests, capacity enforcement, node
disconnect, node restart, coordinator restart, telemetry schema, graceful
shutdown, and the CUDA backend. A bounded CTest invokes the `fabric local`
launcher, which runs a real coordinator and node OS processes over TCP; the
remaining integration tests use in-process runtime instances over real TCP.
