#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <memory>

#include "context_fabric/storage/storage_backend.h"

namespace ctf {

// ---------------------------------------------------------------------------
// HostMemoryBackend: physical copies resident in the node process heap.
// Bounded by an explicit capacity; tracks used bytes and high-water marks.
// ---------------------------------------------------------------------------
class HostMemoryBackend : public StorageBackend {
public:
    explicit HostMemoryBackend(uint64_t capacity_bytes);

    StorageDomain domain() const noexcept override { return StorageDomain::HostMemory; }
    const char* name() const noexcept override { return "host-memory"; }
    bool available() const noexcept override { return true; }

    uint64_t capacity_bytes() const noexcept override;
    uint64_t used_bytes() const noexcept override;
    uint64_t high_water_bytes() const noexcept override;

    void put(const ObjectId& id, const Generation& gen, const uint8_t* data, uint64_t size) override;
    std::vector<uint8_t> get(const ObjectId& id, const Generation& gen) override;
    void verify(const ObjectId& id, const Generation& gen, uint64_t expected_crc,
                uint64_t expected_sha) override;
    bool contains(const ObjectId& id, const Generation& gen) const override;
    void remove(const ObjectId& id, const Generation& gen) override;
    void clear() override;
    void shutdown() override;

private:
    mutable std::mutex mutex_;
    std::unordered_map<CopyKey, std::vector<uint8_t>> store_;
    uint64_t capacity_;
    uint64_t used_ = 0;
    uint64_t high_water_ = 0;
};

}  // namespace ctf