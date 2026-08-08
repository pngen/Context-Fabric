#include "context_fabric/storage/host_memory_backend.h"

#include <algorithm>
#include <limits>
#include <new>
#include <stdexcept>

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
    if (!id.valid() || !gen.valid()) {
        throw_error(ErrKind::InvalidArgument, "host memory copy requires a valid identity");
    }
    if (size > static_cast<uint64_t>((std::numeric_limits<size_t>::max)())) {
        throw_error(ErrKind::Capacity, "host memory object exceeds addressable size");
    }
    if (size != 0 && data == nullptr) {
        throw_error(ErrKind::InvalidArgument, "non-empty host memory object has null data");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    CopyKey key{id, gen};
    auto it = store_.find(key);
    const uint64_t old_size = it == store_.end() ? 0 : static_cast<uint64_t>(it->second.size());
    const uint64_t retained = used_ - old_size;
    if (size > capacity_ - retained) {
        throw_error(ErrKind::Capacity, "host memory capacity exceeded");
    }

    // Construct before changing either the resident value or accounting, so
    // allocation failure leaves the old copy and counters intact.
    try {
        std::vector<uint8_t> replacement;
        if (size != 0) {
            replacement.assign(data, data + static_cast<size_t>(size));
        }
        if (it != store_.end()) {
            it->second = std::move(replacement);
        } else {
            store_.emplace(key, std::move(replacement));
        }
    } catch (const std::bad_alloc&) {
        throw_error(ErrKind::Capacity, "host memory allocation failed");
    } catch (const std::length_error&) {
        throw_error(ErrKind::Capacity, "host memory object exceeds addressable size");
    }
    used_ = retained + size;
    high_water_ = (std::max)(high_water_, used_);
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
