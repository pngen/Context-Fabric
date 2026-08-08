#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"
#include "context_fabric/core/types.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Storage backend interface. A node exposes one or more of these domains:
// HostMemory, LocalPersistent, AcceleratorLocal.
//
// Every backend tracks used bytes and high-water marks, enforces an explicit
// capacity bound, and reports availability. Unsupported capabilities report
// NotSupported rather than pretending success.
// ---------------------------------------------------------------------------
class StorageBackend {
public:
    virtual ~StorageBackend() = default;

    [[nodiscard]] virtual StorageDomain domain() const noexcept = 0;
    [[nodiscard]] virtual const char* name() const noexcept = 0;
    [[nodiscard]] virtual bool available() const noexcept = 0;

    // Capacity bound (0 = unbounded is not permitted for node backends).
    [[nodiscard]] virtual uint64_t capacity_bytes() const noexcept = 0;
    [[nodiscard]] virtual uint64_t used_bytes() const noexcept = 0;
    [[nodiscard]] virtual uint64_t high_water_bytes() const noexcept = 0;

    // Store a physical copy of (object, generation) with integrity verification.
    // Throws CfError(Capacity/Integrity/Persistence).
    virtual void put(const ObjectId& id, const Generation& gen, const uint8_t* data, uint64_t size) = 0;

    // Retrieve a physical copy. Throws CfError(NotFound) if absent.
    virtual std::vector<uint8_t> get(const ObjectId& id, const Generation& gen) = 0;

    // Verify the resident copy's checksum. Returns false instead of throwing
    // when the copy is absent; throws CfError(Integrity) if corrupt.
    virtual void verify(const ObjectId& id, const Generation& gen, uint64_t expected_crc,
                        uint64_t expected_sha) = 0;

    [[nodiscard]] virtual bool contains(const ObjectId& id, const Generation& gen) const = 0;

    virtual void remove(const ObjectId& id, const Generation& gen) = 0;
    virtual void clear() = 0;
    virtual void shutdown() = 0;
};

// Convenience: object/content key for a resident copy.
struct CopyKey {
    ObjectId id;
    Generation gen;

    friend bool operator==(const CopyKey& a, const CopyKey& b) noexcept {
        return a.id == b.id && a.gen == b.gen;
    }
    friend bool operator<(const CopyKey& a, const CopyKey& b) noexcept {
        if (a.id != b.id) return a.id < b.id;
        return a.gen < b.gen;
    }
};

}  // namespace ctf

namespace std {
template <>
struct hash<ctf::CopyKey> {
    size_t operator()(const ctf::CopyKey& k) const noexcept {
        return static_cast<size_t>(k.id.hi() ^ k.id.lo() ^ (k.gen.value() * 0x9E3779B97F4A7C15ull));
    }
};
}  // namespace std