#include "context_fabric/core/types.h"

#include <cstring>

#include "context_fabric/core/object_id.h"

namespace ctf {

const char* semantic_class_name(SemanticClass c) noexcept {
    switch (c) {
        case SemanticClass::Generic: return "generic";
        case SemanticClass::KvCache: return "kv-cache";
        case SemanticClass::PrefixState: return "prefix-state";
        case SemanticClass::RecurrentState: return "recurrent-state";
        case SemanticClass::Tensor: return "tensor";
        case SemanticClass::Embedding: return "embedding";
        case SemanticClass::RetrievalState: return "retrieval-state";
        case SemanticClass::AgentState: return "agent-state";
        case SemanticClass::ToolArtifact: return "tool-artifact";
        case SemanticClass::Checkpoint: return "checkpoint";
        case SemanticClass::ApplicationDefined: return "application-defined";
    }
    return "unknown";
}

std::optional<SemanticClass> semantic_class_from_name(const std::string& name) noexcept {
    if (name == "generic") return SemanticClass::Generic;
    if (name == "kv-cache") return SemanticClass::KvCache;
    if (name == "prefix-state") return SemanticClass::PrefixState;
    if (name == "recurrent-state") return SemanticClass::RecurrentState;
    if (name == "tensor") return SemanticClass::Tensor;
    if (name == "embedding") return SemanticClass::Embedding;
    if (name == "retrieval-state") return SemanticClass::RetrievalState;
    if (name == "agent-state") return SemanticClass::AgentState;
    if (name == "tool-artifact") return SemanticClass::ToolArtifact;
    if (name == "checkpoint") return SemanticClass::Checkpoint;
    if (name == "application-defined") return SemanticClass::ApplicationDefined;
    return std::nullopt;
}

const char* sharing_policy_name(SharingPolicy p) noexcept {
    switch (p) {
        case SharingPolicy::Private: return "private";
        case SharingPolicy::NamespaceShared: return "namespace-shared";
        case SharingPolicy::GloballyShareable: return "globally-shareable";
    }
    return "unknown";
}

std::optional<SharingPolicy> sharing_policy_from_name(const std::string& name) noexcept {
    if (name == "private") return SharingPolicy::Private;
    if (name == "namespace-shared") return SharingPolicy::NamespaceShared;
    if (name == "globally-shareable") return SharingPolicy::GloballyShareable;
    return std::nullopt;
}

const char* persistence_policy_name(PersistencePolicy p) noexcept {
    switch (p) {
        case PersistencePolicy::Volatile: return "volatile";
        case PersistencePolicy::Ephemeral: return "ephemeral";
        case PersistencePolicy::Persistent: return "persistent";
    }
    return "unknown";
}

std::optional<PersistencePolicy> persistence_policy_from_name(const std::string& name) noexcept {
    if (name == "volatile") return PersistencePolicy::Volatile;
    if (name == "ephemeral") return PersistencePolicy::Ephemeral;
    if (name == "persistent") return PersistencePolicy::Persistent;
    return std::nullopt;
}

const char* storage_domain_name(StorageDomain d) noexcept {
    switch (d) {
        case StorageDomain::Unknown: return "unknown";
        case StorageDomain::AcceleratorLocal: return "accelerator-local";
        case StorageDomain::HostMemory: return "host-memory";
        case StorageDomain::LocalPersistent: return "local-persistent";
        case StorageDomain::RemoteMemory: return "remote-memory";
        case StorageDomain::RemotePersistent: return "remote-persistent";
    }
    return "unknown";
}

std::optional<StorageDomain> storage_domain_from_name(const std::string& name) noexcept {
    if (name == "accelerator-local") return StorageDomain::AcceleratorLocal;
    if (name == "host-memory") return StorageDomain::HostMemory;
    if (name == "local-persistent") return StorageDomain::LocalPersistent;
    if (name == "remote-memory") return StorageDomain::RemoteMemory;
    if (name == "remote-persistent") return StorageDomain::RemotePersistent;
    return std::nullopt;
}

const char* lifecycle_state_name(LifecycleState s) noexcept {
    switch (s) {
        case LifecycleState::Creating: return "creating";
        case LifecycleState::Available: return "available";
        case LifecycleState::LeasedRead: return "leased-read";
        case LifecycleState::LeasedWrite: return "leased-write";
        case LifecycleState::Replicating: return "replicating";
        case LifecycleState::Migrating: return "migrating";
        case LifecycleState::Persisting: return "persisting";
        case LifecycleState::Restoring: return "restoring";
        case LifecycleState::Invalidating: return "invalidating";
        case LifecycleState::Recovering: return "recovering";
        case LifecycleState::Corrupt: return "corrupt";
        case LifecycleState::Expired: return "expired";
        case LifecycleState::Released: return "released";
    }
    return "unknown";
}

std::optional<LifecycleState> lifecycle_state_from_name(const std::string& name) noexcept {
    if (name == "creating") return LifecycleState::Creating;
    if (name == "available") return LifecycleState::Available;
    if (name == "leased-read") return LifecycleState::LeasedRead;
    if (name == "leased-write") return LifecycleState::LeasedWrite;
    if (name == "replicating") return LifecycleState::Replicating;
    if (name == "migrating") return LifecycleState::Migrating;
    if (name == "persisting") return LifecycleState::Persisting;
    if (name == "restoring") return LifecycleState::Restoring;
    if (name == "invalidating") return LifecycleState::Invalidating;
    if (name == "recovering") return LifecycleState::Recovering;
    if (name == "corrupt") return LifecycleState::Corrupt;
    if (name == "expired") return LifecycleState::Expired;
    if (name == "released") return LifecycleState::Released;
    return std::nullopt;
}

const char* copy_role_name(CopyRole r) noexcept {
    switch (r) {
        case CopyRole::None: return "none";
        case CopyRole::Authoritative: return "authoritative";
        case CopyRole::Replica: return "replica";
        case CopyRole::Persistent: return "persistent";
        case CopyRole::Transient: return "transient";
    }
    return "unknown";
}

const char* lease_mode_name(LeaseMode m) noexcept {
    switch (m) {
        case LeaseMode::None: return "none";
        case LeaseMode::Read: return "read";
        case LeaseMode::Write: return "write";
    }
    return "unknown";
}

uint32_t capabilities_to_bits(const NodeCapabilities& c) noexcept {
    uint32_t bits = 0;
    if (c.host_memory) bits |= kCapHostMemory;
    if (c.local_persistent) bits |= kCapLocalPersistent;
    if (c.accelerator) bits |= kCapAccelerator;
    if (c.cuda) bits |= kCapCuda;
    return bits;
}

NodeCapabilities capabilities_from_bits(uint32_t bits) noexcept {
    NodeCapabilities c;
    c.host_memory = (bits & kCapHostMemory) != 0;
    c.local_persistent = (bits & kCapLocalPersistent) != 0;
    c.accelerator = (bits & kCapAccelerator) != 0;
    c.cuda = (bits & kCapCuda) != 0;
    return c;
}

std::string Location::to_string() const {
    char buf[64];
    snprintf(buf, sizeof(buf), "%llu:%s", static_cast<unsigned long long>(node_id),
             storage_domain_name(domain));
    return std::string(buf);
}

}  // namespace ctf