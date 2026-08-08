#include "context_fabric/storage/host_memory_backend.h"

#include <algorithm>

#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"

namespace ctf {

HostMemoryBackend::HostMemoryBackend(uint64_t capacity_bytes) : capacity_(capacity_bytes) {
    if (capacity_ == 0) throw_error(ErrKind::InvalidArgument, "host memory backend requires a nonzero capacity");
}

uint64_t HostMemoryBackend::capacity_bytes() const noexcept {
    return capacity_;
}
uint64_t HostMemoryBackend::used_bytes() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_;
}
uint64_t HostMemoryBackend::high_water_bytes() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return high_water_;
}

void HostMemoryBackend::put(const ObjectId& id, const Generation& gen, const uint8_t* data, uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    CopyKey key{id, gen};
    auto it = store_.find(key);
    if (it != store_.end()) {
        // Overwrite in place: adjust accounting.
        used_ -= it->second.size();
        it->second.assign(data, data + size);
        used_ += size;
    } else {
        if (used_ + size > capacity_) {
            throw_error(ErrKind::Capacity, "host memory capacity exceeded");
        }
        store_[key] = std::vector<uint8_t>(data, data + size);
        used_ += size;
    }
    high_water_ = std::max(high_water_, used_);
}

std::vector<uint8_t> HostMemoryBackend::get(const ObjectId& id, const Generation& gen) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = store_.find(CopyKey{id, gen});
    if (it == store_.end()) throw_error(ErrKind::NotFound, "object copy not resident in host memory");
    return it->second;
}

void HostMemoryBackend::verify(const ObjectId& id, const Generation& gen, uint64_t expected_crc,
                               uint64_t expected_sha) {
    auto data = get(id, gen);
    IntegrityId idg = compute_integrity(data.data(), data.size());
    if (idg.crc32c != expected_crc || idg.sha != expected_sha) {
        throw_error(ErrKind::Integrity, "host memory copy checksum mismatch");
    }
}

bool HostMemoryBackend::contains(const ObjectId& id, const Generation& gen) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return store_.find(CopyKey{id, gen}) != store_.end();
}

void HostMemoryBackend::remove(const ObjectId& id, const Generation& gen) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = store_.find(CopyKey{id, gen});
    if (it == store_.end()) return;
    used_ -= it->second.size();
    store_.erase(it);
}

void HostMemoryBackend::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    store_.clear();
    used_ = 0;
}

void HostMemoryBackend::shutdown() {
    clear();
}

}  // namespace ctf