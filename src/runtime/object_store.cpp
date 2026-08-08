#include "context_fabric/runtime/object_store.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <new>

#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"

namespace ctf {

namespace {

void validate_copy_identity(const ObjectId& id, const Generation& gen, CopyRole role,
                            StorageDomain domain) {
    if (!id.valid() || !gen.valid()) throw_error(ErrKind::InvalidArgument, "copy identity must be valid");
    if (role != CopyRole::Authoritative && role != CopyRole::Replica && role != CopyRole::Persistent &&
        role != CopyRole::Transient) {
        throw_error(ErrKind::InvalidArgument, "copy role is not storable");
    }
    if (domain != StorageDomain::HostMemory && domain != StorageDomain::LocalPersistent &&
        domain != StorageDomain::AcceleratorLocal) {
        throw_error(ErrKind::InvalidArgument, "copy domain is not local storage");
    }
    if (role == CopyRole::Persistent && domain != StorageDomain::LocalPersistent) {
        throw_error(ErrKind::InvalidArgument, "persistent copy role requires persistent storage");
    }
    if (role == CopyRole::Transient && domain != StorageDomain::HostMemory) {
        throw_error(ErrKind::InvalidArgument, "transient copy role requires host storage");
    }
}

}  // namespace

void NodeObjectStore::store(const ObjectId& id, const Generation& gen, CopyRole role, StorageDomain domain,
                            const ContextMetadata* metadata, const uint8_t* data, uint64_t size) {
    validate_copy_identity(id, gen, role, domain);
    if (domain == StorageDomain::AcceleratorLocal) {
        throw_error(ErrKind::NotSupported, "accelerator residency requires explicit ownership transfer");
    }
    const IntegrityId integrity = compute_integrity(data, size);

    std::lock_guard<std::mutex> lock(mutex_);
    const CopyKey key{id, gen};
    auto outer = copies_.end();
    bool inserted_outer = false;
    DomainCopies::iterator entry;
    bool inserted_entry = false;
    try {
        auto outer_result = copies_.try_emplace(key);
        outer = outer_result.first;
        inserted_outer = outer_result.second;
        for (const auto& [resident_domain, resident] : outer->second) {
            (void)resident_domain;
            if (resident.integrity != integrity) {
                throw_error(ErrKind::Integrity, "copy domains disagree on generation content");
            }
        }
        auto entry_result = outer->second.try_emplace(domain);
        entry = entry_result.first;
        inserted_entry = entry_result.second;
    } catch (const std::bad_alloc&) {
        if (inserted_outer) copies_.erase(outer);
        throw_error(ErrKind::Capacity, "cannot allocate local copy bookkeeping");
    } catch (...) {
        if (inserted_outer) copies_.erase(outer);
        throw;
    }

    try {
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
        default:
            throw_error(ErrKind::InvalidArgument, "unsupported store domain");
    }
    } catch (const std::bad_alloc&) {
        if (inserted_entry) outer->second.erase(entry);
        if (outer->second.empty()) copies_.erase(outer);
        throw_error(ErrKind::Capacity, "storage backend allocation failed");
    } catch (...) {
        if (inserted_entry) outer->second.erase(entry);
        if (outer->second.empty()) copies_.erase(outer);
        throw;
    }

    LocalCopy& c = entry->second;
    c.role = role;
    c.domain = domain;
    c.integrity = integrity;
}

std::vector<uint8_t> NodeObjectStore::read(const ObjectId& id, const Generation& gen) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const CopyKey key{id, gen};
    auto it = copies_.find(key);
    if (it == copies_.end()) throw_error(ErrKind::NotFound, "no local copy to read");

    // Prefer a serving copy over durable backing. Domain order is explicit so
    // the selected source is deterministic when several physical copies of
    // the same generation coexist on one node.
    const LocalCopy* copy = nullptr;
    for (StorageDomain domain : {StorageDomain::AcceleratorLocal, StorageDomain::HostMemory,
                                 StorageDomain::LocalPersistent}) {
        auto domain_it = it->second.find(domain);
        if (domain_it != it->second.end()) {
            copy = &domain_it->second;
            break;
        }
    }
    if (copy == nullptr) throw_error(ErrKind::NotFound, "copy domain unavailable");
    const LocalCopy& c = *copy;
    switch (c.domain) {
        case StorageDomain::HostMemory:
            if (!host_) throw_error(ErrKind::NotFound, "host copy backend unavailable");
            return host_->get(id, gen);
        case StorageDomain::LocalPersistent:
            if (!persistent_) throw_error(ErrKind::NotFound, "persistent copy backend unavailable");
            return persistent_->get(id, gen);
        case StorageDomain::AcceleratorLocal: {
            if (!accelerator_) throw_error(ErrKind::NotFound, "accelerator copy backend unavailable");
            auto ar = accelerator_resident_.find(key);
            if (ar == accelerator_resident_.end()) throw_error(ErrKind::NotFound, "accelerator copy not resident");
            std::vector<uint8_t> host(static_cast<size_t>(ar->second.size));
            accelerator_->copy_d2h(host.data(), ar->second.device, ar->second.size);
            accelerator_->synchronize();
            return host;
        }
        default:
            break;
    }
    throw_error(ErrKind::NotFound, "copy domain unavailable");
}

void NodeObjectStore::retain_accelerator(const ObjectId& id, const Generation& gen, CopyRole role,
                                         uint8_t* device, uint64_t size, const IntegrityId& integrity) {
    validate_copy_identity(id, gen, role, StorageDomain::AcceleratorLocal);
    if (!accelerator_) throw_error(ErrKind::NotSupported, "node has no accelerator backend");
    if (size != 0 && device == nullptr) {
        throw_error(ErrKind::InvalidArgument, "non-empty accelerator residency has null device pointer");
    }
    if (integrity.bytes != size) {
        throw_error(ErrKind::Integrity, "accelerator residency byte count does not match integrity identity");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const CopyKey key{id, gen};
    auto outer = copies_.end();
    bool inserted_outer = false;
    DomainCopies::iterator copy_entry;
    bool inserted_copy = false;
    decltype(accelerator_resident_)::iterator residency_entry;
    bool inserted_residency = false;
    try {
        auto outer_result = copies_.try_emplace(key);
        outer = outer_result.first;
        inserted_outer = outer_result.second;
        for (const auto& [resident_domain, resident] : outer->second) {
            (void)resident_domain;
            if (resident.integrity != integrity) {
                throw_error(ErrKind::Integrity, "copy domains disagree on generation content");
            }
        }
        auto copy_result = outer->second.try_emplace(StorageDomain::AcceleratorLocal);
        copy_entry = copy_result.first;
        inserted_copy = copy_result.second;
        auto residency_result = accelerator_resident_.try_emplace(key);
        residency_entry = residency_result.first;
        inserted_residency = residency_result.second;
    } catch (const std::bad_alloc&) {
        if (inserted_copy) outer->second.erase(copy_entry);
        if (inserted_outer) copies_.erase(outer);
        throw_error(ErrKind::Capacity, "cannot allocate accelerator residency bookkeeping");
    } catch (...) {
        if (inserted_copy) outer->second.erase(copy_entry);
        if (inserted_outer) copies_.erase(outer);
        throw;
    }

    auto old = residency_entry;
    if (old != accelerator_resident_.end() && old->second.device != nullptr && old->second.device != device) {
        // Do this before accepting ownership of the replacement. If release
        // fails, callers still own `device` and the old residency remains the
        // authoritative entry.
        try {
            accelerator_->free(old->second.device);
        } catch (...) {
            if (inserted_residency) accelerator_resident_.erase(residency_entry);
            if (inserted_copy) outer->second.erase(copy_entry);
            if (outer->second.empty()) copies_.erase(outer);
            throw;
        }
    }
    residency_entry->second = AccelResidency{device, size};
    LocalCopy& c = copy_entry->second;
    c.role = role;
    c.domain = StorageDomain::AcceleratorLocal;
    c.integrity = integrity;
}

void NodeObjectStore::verify(const ObjectId& id, const Generation& gen, uint64_t expected_crc,
                             uint64_t expected_sha, bool deep, uint64_t expected_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    const CopyKey key{id, gen};
    auto it = copies_.find(key);
    if (it == copies_.end()) throw_error(ErrKind::NotFound, "no local copy to verify");

    const LocalCopy* copy = nullptr;
    for (StorageDomain domain : {StorageDomain::AcceleratorLocal, StorageDomain::HostMemory,
                                 StorageDomain::LocalPersistent}) {
        auto domain_it = it->second.find(domain);
        if (domain_it != it->second.end()) {
            copy = &domain_it->second;
            break;
        }
    }
    if (copy == nullptr) throw_error(ErrKind::NotFound, "copy domain unavailable");
    const LocalCopy& c = *copy;
    IntegrityId recorded = c.integrity;
    if (recorded.crc32c != expected_crc || recorded.sha != expected_sha ||
        (expected_bytes != 0 && recorded.bytes != expected_bytes)) {
        throw_error(ErrKind::Integrity, "recorded integrity does not match expected");
    }
    if (!deep) return;
    // Deep verification reads the physical bytes from the backend.
    std::vector<uint8_t> bytes;
    switch (c.domain) {
        case StorageDomain::HostMemory:
            if (!host_) throw_error(ErrKind::NotFound, "host copy backend unavailable");
            bytes = host_->get(id, gen);
            break;
        case StorageDomain::LocalPersistent:
            if (!persistent_) throw_error(ErrKind::NotFound, "persistent copy backend unavailable");
            bytes = persistent_->get(id, gen);
            break;
        case StorageDomain::AcceleratorLocal: {
            if (!accelerator_) throw_error(ErrKind::NotFound, "accelerator copy backend unavailable");
            auto ar = accelerator_resident_.find(key);
            if (ar == accelerator_resident_.end()) throw_error(ErrKind::NotFound, "accelerator copy not resident");
            bytes.resize(static_cast<size_t>(ar->second.size));
            accelerator_->copy_d2h(bytes.data(), ar->second.device, ar->second.size);
            accelerator_->synchronize();
            break;
        }
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
    if (it == copies_.end()) throw_error(ErrKind::NotFound, "no local copy to update role");
    bool changed = false;
    for (auto& [domain, copy] : it->second) {
        if (domain != StorageDomain::LocalPersistent) {
            copy.role = role;
            changed = true;
        }
    }
    if (!changed) throw_error(ErrKind::State, "persistent backing cannot become a serving copy");
}

void NodeObjectStore::remove(const ObjectId& id, const Generation& gen) {
    std::lock_guard<std::mutex> lock(mutex_);
    const CopyKey key{id, gen};
    auto it = copies_.find(key);
    if (it == copies_.end()) return;
    while (!it->second.empty()) {
        auto domain_it = it->second.begin();
        const StorageDomain domain = domain_it->first;
        switch (domain) {
            case StorageDomain::HostMemory:
                if (host_) host_->remove(id, gen);
                break;
            case StorageDomain::LocalPersistent:
                if (persistent_) persistent_->remove(id, gen);
                break;
            case StorageDomain::AcceleratorLocal: {
                auto ar = accelerator_resident_.find(key);
                if (ar != accelerator_resident_.end() && accelerator_) {
                    accelerator_->free(ar->second.device);
                }
                accelerator_resident_.erase(key);
                break;
            }
            default:
                break;
        }
        // Erase each domain immediately after its backend release succeeds.
        // If a later backend fails, already-released domains are not left as
        // stale metadata and remaining domains stay retryable.
        it->second.erase(domain_it);
    }
    copies_.erase(it);
}

void NodeObjectStore::remove_domain(const ObjectId& id, const Generation& gen, StorageDomain domain) {
    std::lock_guard<std::mutex> lock(mutex_);
    const CopyKey key{id, gen};
    auto it = copies_.find(key);
    if (it == copies_.end()) return;
    auto domain_it = it->second.find(domain);
    if (domain_it == it->second.end()) return;
    switch (domain) {
        case StorageDomain::HostMemory:
            if (host_) host_->remove(id, gen);
            break;
        case StorageDomain::LocalPersistent:
            if (persistent_) persistent_->remove(id, gen);
            break;
        case StorageDomain::AcceleratorLocal: {
            auto ar = accelerator_resident_.find(key);
            if (ar != accelerator_resident_.end() && accelerator_) accelerator_->free(ar->second.device);
            accelerator_resident_.erase(key);
            break;
        }
        default:
            break;
    }
    it->second.erase(domain_it);
    if (it->second.empty()) copies_.erase(it);
}

void NodeObjectStore::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::exception_ptr first_error;
    if (accelerator_) {
        for (auto it = accelerator_resident_.begin(); it != accelerator_resident_.end();) {
            try {
                accelerator_->free(it->second.device);
                it = accelerator_resident_.erase(it);
            } catch (...) {
                if (!first_error) first_error = std::current_exception();
                ++it;
            }
        }
    }
    accelerator_resident_.clear();
    copies_.clear();
    if (first_error) std::rethrow_exception(first_error);
}

bool NodeObjectStore::has_copy(const ObjectId& id, const Generation& gen) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = copies_.find(CopyKey{id, gen});
    return it != copies_.end() && !it->second.empty();
}

bool NodeObjectStore::has_role(const ObjectId& id, const Generation& gen, CopyRole role) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = copies_.find(CopyKey{id, gen});
    if (it == copies_.end()) return false;
    return std::any_of(it->second.begin(), it->second.end(),
                       [role](const auto& entry) { return entry.second.role == role; });
}

CopyRole NodeObjectStore::role_of(const ObjectId& id, const Generation& gen) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = copies_.find(CopyKey{id, gen});
    if (it == copies_.end()) return CopyRole::None;
    for (CopyRole role : {CopyRole::Authoritative, CopyRole::Replica, CopyRole::Persistent,
                          CopyRole::Transient}) {
        if (std::any_of(it->second.begin(), it->second.end(),
                        [role](const auto& entry) { return entry.second.role == role; })) {
            return role;
        }
    }
    return CopyRole::None;
}

std::vector<CopyKey> NodeObjectStore::all_copies() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<CopyKey> keys;
    keys.reserve(copies_.size());
    for (const auto& [k, v] : copies_) {
        (void)v;
        keys.push_back(k);
    }
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
