#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"
#include "context_fabric/core/types.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/storage/storage_backend.h"
#include "context_fabric/storage/host_memory_backend.h"
#include "context_fabric/storage/persistent_backend.h"
#include "context_fabric/storage/accelerator_backend.h"

namespace ctf {

// ---------------------------------------------------------------------------
// NodeObjectStore: tracks which physical copies this node holds and their
// role (authoritative / replica / persistent) and integrity identity. The
// backends hold the bytes; this store holds the role metadata.
// ---------------------------------------------------------------------------
struct LocalCopy {
    CopyRole role = CopyRole::None;
    StorageDomain domain = StorageDomain::Unknown;
    IntegrityId integrity;
};

// A resident accelerator allocation (device pointer + size).
struct AccelResidency {
    uint8_t* device = nullptr;
    uint64_t size = 0;
};

class NodeObjectStore {
public:
    NodeObjectStore() = default;

    void set_host(HostMemoryBackend* host) { host_ = host; }
    void set_persistent(LocalPersistentBackend* persistent) { persistent_ = persistent; }
    void set_accelerator(AcceleratorBackend* accelerator) { accelerator_ = accelerator; }

    // Store bytes into the given domain and record the copy role.
    void store(const ObjectId& id, const Generation& gen, CopyRole role, StorageDomain domain,
               const ContextMetadata* metadata, const uint8_t* data, uint64_t size);

    // Retain a device allocation as an accelerator-resident copy.
    void retain_accelerator(const ObjectId& id, const Generation& gen, CopyRole role,
                            uint8_t* device, uint64_t size, const IntegrityId& integrity);

    // Read bytes for a transfer from whatever backend holds the copy.
    std::vector<uint8_t> read(const ObjectId& id, const Generation& gen) const;

    // Verify a resident copy against the store's recorded integrity.
    void verify(const ObjectId& id, const Generation& gen, uint64_t expected_crc, uint64_t expected_sha,
                bool deep = true, uint64_t expected_bytes = 0);

    void set_role(const ObjectId& id, const Generation& gen, CopyRole role);
    void remove(const ObjectId& id, const Generation& gen);
    void remove_domain(const ObjectId& id, const Generation& gen, StorageDomain domain);

    // Forget backend associations during runtime teardown. Accelerator
    // allocations are released; persistent records are deliberately retained.
    void reset();

    [[nodiscard]] bool has_copy(const ObjectId& id, const Generation& gen) const;
    [[nodiscard]] bool has_role(const ObjectId& id, const Generation& gen, CopyRole role) const;
    [[nodiscard]] CopyRole role_of(const ObjectId& id, const Generation& gen) const;
    [[nodiscard]] std::vector<CopyKey> all_copies() const;

    // Account usage.
    [[nodiscard]] uint64_t host_used() const;
    [[nodiscard]] uint64_t persistent_used() const;
    [[nodiscard]] uint64_t accelerator_used() const;

private:
    mutable std::mutex mutex_;
    using DomainCopies = std::map<StorageDomain, LocalCopy>;
    std::unordered_map<CopyKey, DomainCopies> copies_;
    std::unordered_map<CopyKey, AccelResidency> accelerator_resident_;
    HostMemoryBackend* host_ = nullptr;
    LocalPersistentBackend* persistent_ = nullptr;
    AcceleratorBackend* accelerator_ = nullptr;
};

}  // namespace ctf
