#pragma once

#include <cstdint>
#include <string>
#include <mutex>
#include <vector>
#include <set>

#include "context_fabric/storage/storage_backend.h"
#include "context_fabric/core/metadata.h"

namespace ctf {

// ---------------------------------------------------------------------------
// LocalPersistentBackend: durable local object persistence.
//
// On-disk format is versioned and integrity-checked (see persistent_backend.cpp
// for the exact layout). Records are committed atomically by writing to a
// staging file and renaming into place. Stale staging files and corrupt
// records are quarantined at open/recovery time.
//
// All paths are contained beneath the node's store root; object ids are
// encoded as hex and never used as raw path components, so no path traversal
// is possible through protocol-supplied metadata.
// ---------------------------------------------------------------------------
class LocalPersistentBackend : public StorageBackend {
public:
    explicit LocalPersistentBackend(std::string root_path, uint64_t capacity_bytes);

    StorageDomain domain() const noexcept override { return StorageDomain::LocalPersistent; }
    const char* name() const noexcept override { return "local-persistent"; }
    bool available() const noexcept override { return available_; }

    uint64_t capacity_bytes() const noexcept override;
    uint64_t used_bytes() const noexcept override;
    uint64_t high_water_bytes() const noexcept override;

    void put(const ObjectId& id, const Generation& gen, const uint8_t* data, uint64_t size) override;

    // Persist object bytes together with their immutable metadata in one
    // atomic record so metadata survives persistence and restoration.
    void put_with_metadata(const ObjectId& id, const Generation& gen,
                           const ContextMetadata& metadata, const uint8_t* data, uint64_t size);

    std::vector<uint8_t> get(const ObjectId& id, const Generation& gen) override;
    void verify(const ObjectId& id, const Generation& gen, uint64_t expected_crc,
                uint64_t expected_sha) override;
    bool contains(const ObjectId& id, const Generation& gen) const override;
    void remove(const ObjectId& id, const Generation& gen) override;
    void clear() override;
    void shutdown() override;

    // Scans the store root, validates every record, quarantines corruption,
    // cleans stale staging files. Returns the list of (object, generation)
    // pairs that were recovered intact.
    std::vector<CopyKey> recover();

    [[nodiscard]] const std::string& root() const noexcept { return root_; }

private:
    std::string file_for(const ObjectId& id, const Generation& gen) const;
    void write_record_locked(const CopyKey& key, const std::vector<uint8_t>& metadata_bytes,
                             const uint8_t* data, uint64_t size);
    void quarantine_file(const std::string& path);
    void scan_helpers();

    mutable std::mutex mutex_;
    std::string root_;
    std::string objects_dir_;
    std::string quarantine_dir_;
    std::set<CopyKey> catalog_;
    uint64_t capacity_;
    uint64_t used_ = 0;
    uint64_t high_water_ = 0;
    bool available_ = false;
};

}  // namespace ctf