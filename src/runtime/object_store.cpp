#include "context_fabric/runtime/object_store.h"

#include <algorithm>

#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"

namespace ctf {

void NodeObjectStore::store(const ObjectId& id, const Generation& gen, CopyRole role, StorageDomain domain,
                            const ContextMetadata* metadata, const uint8_t* data, uint64_t size) {
    switch (domain) {
        case StorageDomain::HostMemory: {
            if (!host_) throw_error(ErrKind::NotSupported, "node has no host memory backend");
            host_->put(id, gen, data, size);
            break;
        }
        case StorageDomain::LocalPersistent: {
            if (!persistent_) throw_error(ErrKind::NotSupported, "node has no persistent backend");
            if (metadata != nullptr) {
                persistent_->put_with_metadata(id, gen, *metadata, data, size);
            } else {
                persistent_->put(id, gen, data, size);
            }
            break;
        }
        case StorageDomain::AcceleratorLocal:
            // Accelerator residency is managed explicitly by the runtime
            // (host staging + device upload); byte storage lives in host first.
            throw_error(ErrKind::NotSupported, "accelerator residency requires explicit staging");
            break;
        default:
            throw_error(ErrKind::InvalidArgument, "unsupported store domain");
    }

    IntegrityId idg = compute_integrity(data, size);
    std::lock_guard<std::mutex> lock(mutex_);
    LocalCopy& c = copies_[CopyKey{id, gen}];
    c.role = role;
    c.domain = domain;
    c.integrity = idg;
}

std::vector<uint8_t> NodeObjectStore::read(const ObjectId& id, const Generation& gen) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = copies_.find(CopyKey{id, gen});
    if (it == copies_.end()) throw_error(ErrKind::NotFound, "no local copy to read");
    const LocalCopy& c = it->second;
    switch (c.domain) {
        case StorageDomain::HostMemory:
            if (!host_) break;
            return host_->get(id, gen);
        case StorageDomain::LocalPersistent:
            if (!persistent_) break;
            return persistent_->get(id, gen);
        case StorageDomain::AcceleratorLocal:
            if (!accelerator_) break;
            throw_error(ErrKind::NotSupported, "accelerator reads require explicit staging");
        default:
            break;
    }
    throw_error(ErrKind::NotFound, "copy domain unavailable");
}

void NodeObjectStore::verify(const ObjectId& id, const Generation& gen, uint64_t expected_crc,
                             uint64_t expected_sha) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = copies_.find(CopyKey{id, gen});
    if (it == copies_.end()) throw_error(ErrKind::NotFound, "no local copy to verify");
    const LocalCopy& c = it->second;
    IntegrityId recorded = c.integrity;
    if (recorded.crc32c != expected_crc || recorded.sha != expected_sha) {
        throw_error(ErrKind::Integrity, "recorded integrity does not match expected");
    }
    // Deep verification reads the physical bytes from the backend.
    std::vector<uint8_t> bytes;
    switch (c.domain) {
        case StorageDomain::HostMemory:
            bytes = host_->get(id, gen);
            break;
        case StorageDomain::LocalPersistent:
            bytes = persistent_->get(id, gen);
            break;
        case StorageDomain::AcceleratorLocal:
            throw_error(ErrKind::NotSupported, "deep accelerator verification requires explicit staging");
        default:
            throw_error(ErrKind::NotFound, "copy domain unavailable");
    }
    IntegrityId actual = compute_integrity(bytes.data(), bytes.size());
    if (actual.crc32c != expected_crc || actual.sha != expected_sha) {
        throw_error(ErrKind::Integrity, "physical copy checksum mismatch");
    }
}

void NodeObjectStore::set_role(const ObjectId& id, const Generation& gen, CopyRole role) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = copies_.find(CopyKey{id, gen});
    if (it == copies_.end()) return;
    it->second.role = role;
}

void NodeObjectStore::remove(const ObjectId& id, const Generation& gen) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = copies_.find(CopyKey{id, gen});
    if (it == copies_.end()) return;
    StorageDomain d = it->second.domain;
    switch (d) {
        case StorageDomain::HostMemory:
            if (host_) host_->remove(id, gen);
            break;
        case StorageDomain::LocalPersistent:
            if (persistent_) persistent_->remove(id, gen);
            break;
        case StorageDomain::AcceleratorLocal:
            break;
        default:
            break;
    }
    copies_.erase(it);
}

void NodeObjectStore::remove_domain(const ObjectId& id, const Generation& gen, StorageDomain domain) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = copies_.find(CopyKey{id, gen});
    if (it == copies_.end()) return;
    if (it->second.domain != domain) return;
    switch (domain) {
        case StorageDomain::HostMemory:
            if (host_) host_->remove(id, gen);
            break;
        case StorageDomain::LocalPersistent:
            if (persistent_) persistent_->remove(id, gen);
            break;
        default:
            break;
    }
    copies_.erase(it);
}

bool NodeObjectStore::has_copy(const ObjectId& id, const Generation& gen) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return copies_.find(CopyKey{id, gen}) != copies_.end();
}

bool NodeObjectStore::has_role(const ObjectId& id, const Generation& gen, CopyRole role) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = copies_.find(CopyKey{id, gen});
    return it != copies_.end() && it->second.role == role;
}

CopyRole NodeObjectStore::role_of(const ObjectId& id, const Generation& gen) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = copies_.find(CopyKey{id, gen});
    if (it == copies_.end()) return CopyRole::None;
    return it->second.role;
}

std::vector<CopyKey> NodeObjectStore::all_copies() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<CopyKey> keys;
    keys.reserve(copies_.size());
    for (const auto& [k, v] : copies_) keys.push_back(k);
    return keys;
}

uint64_t NodeObjectStore::host_used() const {
    return host_ ? host_->used_bytes() : 0;
}
uint64_t NodeObjectStore::persistent_used() const {
    return persistent_ ? persistent_->used_bytes() : 0;
}
uint64_t NodeObjectStore::accelerator_used() const {
    return accelerator_ ? accelerator_->used_bytes() : 0;
}

}  // namespace ctf