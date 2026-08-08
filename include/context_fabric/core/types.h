#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

namespace ctf {

// ---------------------------------------------------------------------------
// Semantic class of a reusable computational-state object.
//
// The runtime is workload-independent: KV cache is only one supported class.
// Framework/model-specific adapters map their state onto these classes.
// ---------------------------------------------------------------------------
enum class SemanticClass : uint32_t {
    Generic = 0,
    KvCache,
    PrefixState,
    RecurrentState,
    Tensor,
    Embedding,
    RetrievalState,
    AgentState,
    ToolArtifact,
    Checkpoint,
    ApplicationDefined,
};

[[nodiscard]] const char* semantic_class_name(SemanticClass c) noexcept;
[[nodiscard]] std::optional<SemanticClass> semantic_class_from_name(const std::string& name) noexcept;

// ---------------------------------------------------------------------------
// Sharing policy controls how freely an object's physical copy may be reused.
// Privacy-sensitive state must not become globally reusable merely because
// content hashes happen to match.
// ---------------------------------------------------------------------------
enum class SharingPolicy : uint32_t {
    Private = 0,          // only the owning namespace may reference it
    NamespaceShared,      // reusable within the same explicit namespace
    GloballyShareable,    // reusable anywhere (opt-in only)
};

[[nodiscard]] const char* sharing_policy_name(SharingPolicy p) noexcept;
[[nodiscard]] std::optional<SharingPolicy> sharing_policy_from_name(const std::string& name) noexcept;

// ---------------------------------------------------------------------------
// Persistence policy for an object.
// ---------------------------------------------------------------------------
enum class PersistencePolicy : uint32_t {
    Volatile = 0,       // never persist; loss on node death is acceptable
    Ephemeral,          // persist only while a node holds it
    Persistent,         // persist to durable storage; restore on demand
};

[[nodiscard]] const char* persistence_policy_name(PersistencePolicy p) noexcept;
[[nodiscard]] std::optional<PersistencePolicy> persistence_policy_from_name(const std::string& name) noexcept;

// ---------------------------------------------------------------------------
// Storage domain: where a physical copy resides.
// ---------------------------------------------------------------------------
enum class StorageDomain : uint32_t {
    Unknown = 0,
    AcceleratorLocal,   // accelerator-attached memory (e.g. CUDA device)
    HostMemory,         // ordinary process heap
    LocalPersistent,    // durable local storage (NVMe/SSD)
    RemoteMemory,       // memory on another node
    RemotePersistent,   // durable storage on another node
};

[[nodiscard]] const char* storage_domain_name(StorageDomain d) noexcept;
[[nodiscard]] std::optional<StorageDomain> storage_domain_from_name(const std::string& name) noexcept;

// ---------------------------------------------------------------------------
// Object lifecycle state.
// The state machine lives in state_machine.h; this enum is the single source
// of truth for the set of states.
// ---------------------------------------------------------------------------
enum class LifecycleState : uint32_t {
    Creating = 0,
    Available,
    LeasedRead,
    LeasedWrite,
    Replicating,
    Migrating,
    Persisting,
    Restoring,
    Invalidating,
    Recovering,
    Corrupt,
    Expired,
    Released,
};

[[nodiscard]] const char* lifecycle_state_name(LifecycleState s) noexcept;
[[nodiscard]] std::optional<LifecycleState> lifecycle_state_from_name(const std::string& name) noexcept;

// ---------------------------------------------------------------------------
// Copy role: how a physical copy participates in the object's coherence.
// ---------------------------------------------------------------------------
enum class CopyRole : uint32_t {
    None = 0,
    Authoritative,   // writable generation owner; the only writable copy
    Replica,         // validated read-only replica of an exact generation
    Persistent,      // durable backing copy (not a serving replica)
    Transient,       // transfer staging buffer, not a published copy
};

[[nodiscard]] const char* copy_role_name(CopyRole r) noexcept;

// Lease mode.
enum class LeaseMode : uint32_t {
    None = 0,
    Read,
    Write,
};

[[nodiscard]] const char* lease_mode_name(LeaseMode m) noexcept;

// ---------------------------------------------------------------------------
// Node capability flags.
// ---------------------------------------------------------------------------
struct NodeCapabilities {
    bool host_memory = false;
    bool local_persistent = false;
    bool accelerator = false;      // any accelerator domain
    bool cuda = false;             // specifically CUDA
    uint64_t max_host_bytes = 0;   // 0 = unbounded (not recommended)
    uint64_t max_persistent_bytes = 0;
    uint64_t max_accelerator_bytes = 0;

    [[nodiscard]] bool empty() const noexcept {
        return !host_memory && !local_persistent && !accelerator && !cuda;
    }
};

// Compact capability bitmap for the wire protocol.
inline constexpr uint32_t kCapHostMemory = 1u << 0;
inline constexpr uint32_t kCapLocalPersistent = 1u << 1;
inline constexpr uint32_t kCapAccelerator = 1u << 2;
inline constexpr uint32_t kCapCuda = 1u << 3;

[[nodiscard]] uint32_t capabilities_to_bits(const NodeCapabilities& c) noexcept;
[[nodiscard]] NodeCapabilities capabilities_from_bits(uint32_t bits) noexcept;

// ---------------------------------------------------------------------------
// Location: a physical residency point = node + storage domain.
// ---------------------------------------------------------------------------
struct Location {
    uint64_t node_id = 0;
    StorageDomain domain = StorageDomain::Unknown;

    [[nodiscard]] bool valid() const noexcept {
        return node_id != 0 && domain != StorageDomain::Unknown;
    }
    [[nodiscard]] std::string to_string() const;
};

// ---------------------------------------------------------------------------
// Integrity identity: content hash of a specific generation's bytes.
// ---------------------------------------------------------------------------
struct IntegrityId {
    uint64_t crc32c = 0;   // streaming/fast checksum
    uint64_t sha = 0;      // first 8 bytes of SHA-256 (strong fingerprint)
    uint64_t bytes = 0;    // number of bytes hashed

    [[nodiscard]] bool operator==(const IntegrityId&) const noexcept = default;
};

// ---------------------------------------------------------------------------
// Transfer and recompute cost estimates (planning inputs, not facts).
// ---------------------------------------------------------------------------
struct TransferEstimate {
    double expected_seconds = 0.0;
    double expected_bytes = 0.0;
    double resource_cost = 0.0;  // abstract cost units
};

struct RecomputeEstimate {
    double expected_seconds = 0.0;
    double resource_cost = 0.0;  // abstract cost units
};

// ---------------------------------------------------------------------------
// Placement decision output.
// ---------------------------------------------------------------------------
struct PlacementDecision {
    Location location;
    double score = 0.0;
    std::string policy;      // policy name that produced this decision
    std::string reason;      // human- and machine-readable decision reason
    TransferEstimate transfer;
    RecomputeEstimate recompute;
    bool chose_transfer = false;
};

}  // namespace ctf
