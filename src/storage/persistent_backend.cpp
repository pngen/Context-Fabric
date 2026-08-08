#include "context_fabric/storage/persistent_backend.h"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <limits>
#include <new>
#include <stdexcept>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include "context_fabric/core/error.h"
#include "context_fabric/core/clock.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/core/metadata.h"
#include "context_fabric/core/version.h"

namespace ctf {

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// On-disk record format (version 2).
//
//   [0,4)   magic       0x424F4643 "CFOB"
//   [4,8)   version     kPersistFormatVersion
//   [8,12)  header_len  total bytes before data (64 + metadata_len)
//   [12,16) flags       bit0 = has data
//   [16,24) object_id.hi
//   [24,32) object_id.lo
//   [32,40) generation
//   [40,48) data_size
//   [48,52) data_crc32c
//   [52,56) header_crc32c (crc32c over [0, header_len) with this field zeroed)
//   [56,64) metadata_len
//   [64,64+metadata_len) serialized ContextMetadata
//   then data_size bytes of content
//
// Torn/truncated records are detected via header_len/data_size bounds and the
// header checksum; content corruption via data_crc32c. Corrupt records are
// quarantined, never silently accepted.
// ---------------------------------------------------------------------------
namespace {
constexpr uint32_t kPersistMagic = 0x424F4643u;
constexpr uint32_t kFixedHeaderSize = 64;
constexpr uint32_t kFlagHasData = 1u << 0;

struct PersistHeader {
    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t header_len = 0;
    uint32_t flags = 0;
    ObjectId id;
    uint64_t generation = 0;
    uint64_t data_size = 0;
    uint32_t data_crc = 0;
    uint32_t header_crc = 0;
    uint64_t metadata_len = 0;

    void encode(uint8_t* out) const {
        auto w32 = [out](size_t off, uint32_t v) {
            out[off] = static_cast<uint8_t>(v >> 24);
            out[off + 1] = static_cast<uint8_t>((v >> 16) & 0xFF);
            out[off + 2] = static_cast<uint8_t>((v >> 8) & 0xFF);
            out[off + 3] = static_cast<uint8_t>(v & 0xFF);
        };
        auto w64 = [out](size_t off, uint64_t v) {
            for (int i = 0; i < 8; ++i) out[off + i] = static_cast<uint8_t>((v >> (8 * (7 - i))) & 0xFF);
        };
        w32(0, magic);
        w32(4, version);
        w32(8, header_len);
        w32(12, flags);
        w64(16, id.hi());
        w64(24, id.lo());
        w64(32, generation);
        w64(40, data_size);
        w32(48, data_crc);
        w32(52, header_crc);
        w64(56, metadata_len);
    }

    static bool decode(const uint8_t* in, PersistHeader& out) {
        auto r32 = [in](size_t off) {
            return (static_cast<uint32_t>(in[off]) << 24) | (static_cast<uint32_t>(in[off + 1]) << 16) |
                   (static_cast<uint32_t>(in[off + 2]) << 8) | in[off + 3];
        };
        auto r64 = [in](size_t off) {
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i) v = (v << 8) | in[off + i];
            return v;
        };
        out.magic = r32(0);
        out.version = r32(4);
        out.header_len = r32(8);
        out.flags = r32(12);
        out.id = ObjectId::from_parts(r64(16), r64(24));
        out.generation = r64(32);
        out.data_size = r64(40);
        out.data_crc = r32(48);
        out.header_crc = r32(52);
        out.metadata_len = r64(56);
        return out.magic == kPersistMagic;
    }
};

std::string hex_of(uint64_t v) {
    char buf[17];
    snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(v));
    return std::string(buf);
}

bool is_hex_digits(const std::string& s) {
    for (char c : s) {
        bool d = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        if (!d) return false;
    }
    return !s.empty();
}

void flush_file_to_disk(const fs::path& path) {
#if defined(_WIN32)
    HANDLE file = CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                              FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file == INVALID_HANDLE_VALUE) {
        throw_error(ErrKind::Persistence, "cannot open staging file for durable flush",
                    static_cast<uint64_t>(GetLastError()));
    }
    const BOOL ok = FlushFileBuffers(file);
    const DWORD error = ok ? ERROR_SUCCESS : GetLastError();
    CloseHandle(file);
    if (!ok) {
        throw_error(ErrKind::Persistence, "durable staging flush failed", static_cast<uint64_t>(error));
    }
#else
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) throw_error(ErrKind::Persistence, "cannot open staging file for durable flush");
    const int rc = ::fsync(fd);
    ::close(fd);
    if (rc != 0) throw_error(ErrKind::Persistence, "durable staging flush failed");
#endif
}

void replace_file_atomically(const fs::path& staging, const fs::path& destination) {
#if defined(_WIN32)
    if (!MoveFileExW(staging.c_str(), destination.c_str(),
                     MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        throw_error(ErrKind::Persistence, "atomic persistent record replacement failed",
                    static_cast<uint64_t>(GetLastError()));
    }
#else
    std::error_code ec;
    fs::rename(staging, destination, ec);
    if (ec) {
        throw_error(ErrKind::Persistence, "atomic persistent record replacement failed: " + ec.message());
    }
#endif
}

void flush_parent_directory(const fs::path& destination) {
#if !defined(_WIN32)
    const fs::path parent = destination.parent_path();
    int fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) throw_error(ErrKind::Persistence, "cannot open persistent directory for durable flush");
    const int rc = ::fsync(fd);
    ::close(fd);
    if (rc != 0) throw_error(ErrKind::Persistence, "durable persistent directory flush failed");
#else
    (void)destination;  // MoveFileExW(MOVEFILE_WRITE_THROUGH) provides the Windows flush boundary.
#endif
}

// Reads and fully validates a persistent record. Does not lock; callers must
// hold the backend mutex.
std::vector<uint8_t> read_record(const std::string& path, const CopyKey* expected_key = nullptr) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) throw_error(ErrKind::NotFound, "persistent record not found");

    std::error_code size_error;
    const uintmax_t physical_size = fs::file_size(path, size_error);
    if (size_error) throw_error(ErrKind::Persistence, "cannot determine persistent record size");
    if (physical_size < kFixedHeaderSize) throw_error(ErrKind::Corrupt, "truncated persistent header");

    uint8_t fixed[kFixedHeaderSize];
    in.read(reinterpret_cast<char*>(fixed), kFixedHeaderSize);
    if (in.gcount() != kFixedHeaderSize) throw_error(ErrKind::Corrupt, "truncated persistent header");

    PersistHeader h;
    if (!PersistHeader::decode(fixed, h)) throw_error(ErrKind::Corrupt, "bad persistent magic");
    if (h.version != kPersistFormatVersion) {
        throw_error(ErrKind::Corrupt, "unsupported persistent format version");
    }
    if (!h.id.valid() || h.generation == 0) throw_error(ErrKind::Corrupt, "invalid persistent identity");
    if (expected_key != nullptr &&
        (h.id != expected_key->id || h.generation != expected_key->gen.value())) {
        throw_error(ErrKind::Corrupt, "persistent record identity does not match its path");
    }
    if (h.header_len < kFixedHeaderSize) throw_error(ErrKind::Corrupt, "invalid persistent header length");
    if (h.header_len > 16u * 1024 * 1024) throw_error(ErrKind::Corrupt, "persistent header too large");
    if (h.metadata_len != static_cast<uint64_t>(h.header_len - kFixedHeaderSize)) {
        throw_error(ErrKind::Corrupt, "persistent metadata length mismatch");
    }
    if (h.flags != kFlagHasData) throw_error(ErrKind::Corrupt, "invalid persistent flags");
    if (h.data_size > static_cast<uint64_t>((std::numeric_limits<size_t>::max)())) {
        throw_error(ErrKind::Capacity, "persistent record exceeds addressable size");
    }
    if (h.data_size > static_cast<uint64_t>((std::numeric_limits<std::streamsize>::max)())) {
        throw_error(ErrKind::Capacity, "persistent record exceeds stream addressable size");
    }
    const uint64_t header_size = h.header_len;
    if (h.data_size > (std::numeric_limits<uint64_t>::max)() - header_size ||
        physical_size != static_cast<uintmax_t>(header_size + h.data_size)) {
        throw_error(ErrKind::Corrupt, "persistent record length mismatch");
    }

    std::vector<uint8_t> header;
    try {
        header.resize(h.header_len);
    } catch (const std::bad_alloc&) {
        throw_error(ErrKind::Capacity, "insufficient memory to read persistent header");
    } catch (const std::length_error&) {
        throw_error(ErrKind::Capacity, "persistent header exceeds addressable size");
    }
    in.seekg(0);
    in.read(reinterpret_cast<char*>(header.data()), static_cast<std::streamsize>(h.header_len));
    if (in.gcount() != static_cast<std::streamsize>(h.header_len)) {
        throw_error(ErrKind::Corrupt, "truncated persistent header");
    }
    PersistHeader z = h;
    z.header_crc = 0;
    uint8_t zf[kFixedHeaderSize];
    z.encode(zf);
    std::memcpy(header.data(), zf, kFixedHeaderSize);
    if (Crc32C::compute(header.data(), header.size()) != h.header_crc) {
        throw_error(ErrKind::Corrupt, "persistent header checksum mismatch");
    }

    if (h.metadata_len != 0) {
        try {
            ContextMetadata metadata;
            metadata.deserialize(header.data() + kFixedHeaderSize, static_cast<size_t>(h.metadata_len));
        } catch (const CfError&) {
            throw_error(ErrKind::Corrupt, "malformed persistent metadata");
        }
    }

    std::vector<uint8_t> data;
    try {
        data.resize(static_cast<size_t>(h.data_size));
    } catch (const std::bad_alloc&) {
        throw_error(ErrKind::Capacity, "insufficient memory to read persistent record");
    } catch (const std::length_error&) {
        throw_error(ErrKind::Capacity, "persistent record exceeds addressable size");
    }
    if (!data.empty()) {
        in.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(data.size()));
        if (in.gcount() != static_cast<std::streamsize>(data.size())) {
            throw_error(ErrKind::Corrupt, "truncated persistent data");
        }
    }
    if (Crc32C::compute(data.data(), data.size()) != h.data_crc) {
        throw_error(ErrKind::Corrupt, "persistent data checksum mismatch");
    }
    return data;
}

}  // namespace

LocalPersistentBackend::LocalPersistentBackend(std::string root_path, uint64_t capacity_bytes)
    : root_(std::move(root_path)), capacity_(capacity_bytes) {
    if (root_.empty()) throw_error(ErrKind::InvalidArgument, "persistent backend requires a store root");
    if (capacity_ == 0) {
        throw_error(ErrKind::InvalidArgument, "persistent backend requires a nonzero capacity");
    }
    objects_dir_ = (fs::path(root_) / "objects").string();
    quarantine_dir_ = (fs::path(root_) / "quarantine").string();
    try {
        fs::create_directories(objects_dir_);
        fs::create_directories(quarantine_dir_);
        available_ = true;
    } catch (const fs::filesystem_error&) {
        available_ = false;
    }
}

uint64_t LocalPersistentBackend::capacity_bytes() const noexcept { return capacity_; }
uint64_t LocalPersistentBackend::used_bytes() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_;
}
uint64_t LocalPersistentBackend::high_water_bytes() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return high_water_;
}

std::string LocalPersistentBackend::file_for(const ObjectId& id, const Generation& gen) const {
    return (fs::path(objects_dir_) / (hex_of(id.hi()) + "-" + hex_of(id.lo()) + "@" + gen.to_hex() + ".cfobj"))
        .string();
}

void LocalPersistentBackend::quarantine_file(const std::string& path) {
    std::error_code ec;
    fs::path p(path);
    fs::path target = fs::path(quarantine_dir_) /
                      (p.filename().string() + ".q" + std::to_string(now_unix_ms()) + "-" +
                       std::to_string(steady_now_ms()));
    fs::rename(p, target, ec);
    if (ec) throw_error(ErrKind::Persistence, "cannot quarantine corrupt persistent record: " + ec.message());
}

void LocalPersistentBackend::write_record_locked(const CopyKey& key,
                                                 const std::vector<uint8_t>& metadata_bytes,
                                                 const uint8_t* data, uint64_t size) {
    if (!available_) throw_error(ErrKind::Persistence, "persistent backend unavailable");
    if (!key.id.valid() || !key.gen.valid()) {
        throw_error(ErrKind::InvalidArgument, "persistent record requires a valid identity");
    }
    if (size > static_cast<uint64_t>((std::numeric_limits<size_t>::max)())) {
        throw_error(ErrKind::Capacity, "persistent object exceeds addressable size");
    }
    if (size > static_cast<uint64_t>((std::numeric_limits<std::streamsize>::max)())) {
        throw_error(ErrKind::Capacity, "persistent object exceeds stream addressable size");
    }
    if (size != 0 && data == nullptr) {
        throw_error(ErrKind::InvalidArgument, "non-empty persistent object has null data");
    }
    if (metadata_bytes.size() > 16u * 1024 * 1024) {
        throw_error(ErrKind::Capacity, "persistent metadata too large");
    }

    PersistHeader h;
    h.magic = kPersistMagic;
    h.version = kPersistFormatVersion;
    h.flags = kFlagHasData;
    h.id = key.id;
    h.generation = key.gen.value();
    h.data_size = size;
    h.data_crc = Crc32C::compute(data, size);
    h.metadata_len = static_cast<uint64_t>(metadata_bytes.size());
    if (h.metadata_len > 0xFFFFFFFFull) throw_error(ErrKind::Capacity, "persistent metadata too large");
    h.header_len = kFixedHeaderSize + static_cast<uint32_t>(metadata_bytes.size());

    if (size > (std::numeric_limits<uint64_t>::max)() - static_cast<uint64_t>(h.header_len)) {
        throw_error(ErrKind::Capacity, "persistent record size overflow");
    }
    const uint64_t record_size = static_cast<uint64_t>(h.header_len) + size;
    const std::string final_path = file_for(key.id, key.gen);

    std::vector<uint8_t> record;
    try {
        record.resize(h.header_len);
    } catch (const std::bad_alloc&) {
        throw_error(ErrKind::Capacity, "insufficient memory to build persistent record");
    } catch (const std::length_error&) {
        throw_error(ErrKind::Capacity, "persistent header exceeds addressable size");
    }
    h.encode(record.data());
    if (!metadata_bytes.empty()) {
        std::memcpy(record.data() + kFixedHeaderSize, metadata_bytes.data(), metadata_bytes.size());
    }

    // Compute header checksum over [0, header_len) with the crc field zeroed.
    PersistHeader zeroed = h;
    zeroed.header_crc = 0;
    uint8_t tmp[kFixedHeaderSize];
    zeroed.encode(tmp);
    std::memcpy(record.data(), tmp, kFixedHeaderSize);
    h.header_crc = Crc32C::compute(record.data(), record.size());
    h.encode(record.data());

    const std::string tmp_path = final_path + ".tmp";
    std::map<CopyKey, uint64_t>::iterator catalog_it;
    bool inserted = false;
    try {
        auto insertion = catalog_.try_emplace(key, 0);
        catalog_it = insertion.first;
        inserted = insertion.second;
    } catch (const std::bad_alloc&) {
        throw_error(ErrKind::Capacity, "cannot allocate persistent catalog entry");
    }
    const uint64_t old_record_size = catalog_it->second;
    if (old_record_size > used_) {
        if (inserted) catalog_.erase(catalog_it);
        throw_error(ErrKind::Corrupt, "persistent accounting is inconsistent");
    }
    const uint64_t retained = used_ - old_record_size;
    if (retained > capacity_ || record_size > capacity_ - retained) {
        if (inserted) catalog_.erase(catalog_it);
        throw_error(ErrKind::Capacity, "persistent storage capacity exceeded");
    }

    bool replaced = false;
    try {
        {
            std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
            if (!out.is_open()) throw_error(ErrKind::Persistence, "cannot open staging file");
            out.write(reinterpret_cast<const char*>(record.data()), static_cast<std::streamsize>(record.size()));
            if (size > 0) out.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
            out.flush();
            if (!out.good()) throw_error(ErrKind::Persistence, "staging write failed");
        }
        flush_file_to_disk(fs::path(tmp_path));
        replace_file_atomically(fs::path(tmp_path), fs::path(final_path));
        replaced = true;
        flush_parent_directory(fs::path(final_path));
    } catch (...) {
        std::error_code cleanup_error;
        fs::remove(tmp_path, cleanup_error);
        if (replaced) {
            // The rename committed even if the directory durability barrier
            // failed. Keep in-memory ownership/accounting truthful.
            catalog_it->second = record_size;
            used_ = retained + record_size;
            high_water_ = (std::max)(high_water_, used_);
        } else if (inserted) {
            catalog_.erase(catalog_it);
        }
        throw;
    }

    catalog_it->second = record_size;
    used_ = retained + record_size;
    high_water_ = (std::max)(high_water_, used_);
}

void LocalPersistentBackend::put(const ObjectId& id, const Generation& gen, const uint8_t* data,
                                 uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    write_record_locked(CopyKey{id, gen}, {}, data, size);
}

void LocalPersistentBackend::put_with_metadata(const ObjectId& id, const Generation& gen,
                                               const ContextMetadata& metadata, const uint8_t* data,
                                               uint64_t size) {
    std::vector<uint8_t> md;
    try {
        metadata.serialize(md);
    } catch (const std::bad_alloc&) {
        throw_error(ErrKind::Capacity, "cannot allocate persistent metadata buffer");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    write_record_locked(CopyKey{id, gen}, md, data, size);
}

std::vector<uint8_t> LocalPersistentBackend::get(const ObjectId& id, const Generation& gen) {
    std::lock_guard<std::mutex> lock(mutex_);
    const CopyKey key{id, gen};
    return read_record(file_for(id, gen), &key);
}

void LocalPersistentBackend::verify(const ObjectId& id, const Generation& gen, uint64_t expected_crc,
                                    uint64_t expected_sha) {
    auto data = get(id, gen);
    IntegrityId idg = compute_integrity(data.data(), data.size());
    if (idg.crc32c != expected_crc || idg.sha != expected_sha) {
        throw_error(ErrKind::Integrity, "persistent copy checksum mismatch");
    }
}

bool LocalPersistentBackend::contains(const ObjectId& id, const Generation& gen) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return catalog_.find(CopyKey{id, gen}) != catalog_.end();
}

void LocalPersistentBackend::remove(const ObjectId& id, const Generation& gen) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string path = file_for(id, gen);
    auto it = catalog_.find(CopyKey{id, gen});
    if (it != catalog_.end()) {
        const uint64_t size = it->second;
        std::error_code remove_error;
        const bool removed = fs::remove(fs::path(path), remove_error);
        if (remove_error) {
            throw_error(ErrKind::Persistence, "cannot remove persistent record: " + remove_error.message());
        }
        if (!removed) {
            std::error_code exists_error;
            const bool still_exists = fs::exists(path, exists_error);
            if (exists_error || still_exists) {
                throw_error(ErrKind::Persistence, "persistent record removal did not complete");
            }
        }
        catalog_.erase(it);
        used_ = size <= used_ ? used_ - size : 0;
        return;
    }
    std::error_code ec;
    fs::remove(fs::path(path), ec);
    if (ec) throw_error(ErrKind::Persistence, "cannot remove uncatalogued persistent record: " + ec.message());
}

void LocalPersistentBackend::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(objects_dir_, ec)) {
        std::error_code remove_error;
        fs::remove(entry.path(), remove_error);
        if (remove_error) {
            throw_error(ErrKind::Persistence, "cannot clear persistent record: " + remove_error.message());
        }
    }
    if (ec) throw_error(ErrKind::Persistence, "cannot enumerate persistent records: " + ec.message());
    catalog_.clear();
    used_ = 0;
}

std::vector<CopyKey> LocalPersistentBackend::recover() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<CopyKey> recovered;
    std::map<CopyKey, uint64_t> recovered_catalog;
    uint64_t recovered_used = 0;
    std::error_code ec;

    // Collect entries first; processing may rename files (quarantine), which
    // would invalidate a live directory iterator.
    std::vector<std::string> entries;
    for (const auto& entry : fs::directory_iterator(objects_dir_, ec)) {
        entries.push_back(entry.path().string());
    }
    if (ec) throw_error(ErrKind::Persistence, "cannot enumerate persistent store: " + ec.message());
    std::sort(entries.begin(), entries.end());

    // Clean stale staging files first.
    for (const auto& path : entries) {
        if (fs::path(path).extension() == ".tmp") {
            fs::remove(path, ec);
            if (ec) throw_error(ErrKind::Persistence, "cannot remove stale staging file: " + ec.message());
        }
    }

    for (const auto& path : entries) {
        if (fs::path(path).extension() != ".cfobj") continue;
        std::string name = fs::path(path).stem().string();
        auto at = name.find('@');
        if (at == std::string::npos) {
            quarantine_file(path);
            continue;
        }
        std::string idpart = name.substr(0, at);
        std::string genpart = name.substr(at + 1);
        size_t dash = idpart.find('-');
        if (idpart.size() != 33 || dash != 16 || genpart.size() != 16 ||
            !is_hex_digits(idpart.substr(0, dash)) || !is_hex_digits(idpart.substr(dash + 1)) ||
            !is_hex_digits(genpart)) {
            quarantine_file(path);
            continue;
        }
        ObjectId id;
        Generation gen;
        try {
            id = ObjectId::from_hex(idpart);
            gen = Generation::from_hex(genpart);
        } catch (const CfError&) {
            quarantine_file(path);
            continue;
        }

        try {
            const CopyKey key{id, gen};
            auto data = read_record(path, &key);
            (void)data;
        } catch (const CfError& e) {
            if (e.kind() == ErrKind::Corrupt || e.kind() == ErrKind::Protocol ||
                e.kind() == ErrKind::InvalidArgument) {
                quarantine_file(path);
                continue;
            }
            throw;
        }
        std::error_code szec;
        const uintmax_t raw_size = fs::file_size(path, szec);
        if (szec || raw_size > (std::numeric_limits<uint64_t>::max)()) {
            throw_error(ErrKind::Persistence, "cannot account recovered persistent record");
        }
        const uint64_t fsize = static_cast<uint64_t>(raw_size);
        if (fsize > (std::numeric_limits<uint64_t>::max)() - recovered_used) {
            throw_error(ErrKind::Capacity, "persistent usage accounting overflow");
        }
        const CopyKey key{id, gen};
        try {
            auto insertion = recovered_catalog.emplace(key, fsize);
            if (!insertion.second) {
                quarantine_file(path);
                continue;
            }
            recovered.emplace_back(key);
        } catch (const std::bad_alloc&) {
            throw_error(ErrKind::Capacity, "cannot allocate recovered persistent catalog");
        }
        recovered_used += fsize;
    }
    catalog_.swap(recovered_catalog);
    used_ = recovered_used;
    high_water_ = (std::max)(high_water_, used_);
    return recovered;
}

void LocalPersistentBackend::shutdown() {}

}  // namespace ctf
