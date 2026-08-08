#pragma once

#include <cstdint>
#include <string>
#include <functional>

namespace ctf {

// Stable 64-bit hash, fully deterministic across platforms and runs.
// Used for deterministic object IDs and state hashing. NOT a cryptographic
// primitive; integrity uses crc32c + sha256 elsewhere.
[[nodiscard]] uint64_t stable_hash64(const void* data, size_t len) noexcept;
[[nodiscard]] uint64_t stable_hash64_str(const std::string& s) noexcept;

// ---------------------------------------------------------------------------
// ContextObjectId: globally stable identity of a reusable computational-state
// object. 128 bits, independent of any process, host, or physical resident.
// May be created deterministically (from a canonical namespace@name string)
// or randomly. Stable across migration and across restart when restored from
// persisted metadata.
// ---------------------------------------------------------------------------
class ObjectId {
public:
    ObjectId() = default;

    // Deterministic creation from a canonical string form.
    static ObjectId deterministic(const std::string& namespace_name, const std::string& name);

    // Randomized globally-unique identity.
    static ObjectId random();

    // Parse from the canonical 32-hex-digit "XXXXXXXX-…-XXXXXXXX" form.
    static ObjectId from_hex(const std::string& hex);

    // Reconstruct from raw parts (used by deterministic serializers).
    static ObjectId from_parts(uint64_t hi, uint64_t lo) noexcept { return ObjectId(hi, lo); }

    [[nodiscard]] bool valid() const noexcept { return hi_ != 0 || lo_ != 0; }
    [[nodiscard]] uint64_t hi() const noexcept { return hi_; }
    [[nodiscard]] uint64_t lo() const noexcept { return lo_; }

    // Canonical printable form: 32 hex digits with a dash in the middle.
    [[nodiscard]] std::string to_hex() const;

    friend bool operator==(const ObjectId& a, const ObjectId& b) noexcept {
        return a.hi_ == b.hi_ && a.lo_ == b.lo_;
    }
    friend bool operator!=(const ObjectId& a, const ObjectId& b) noexcept {
        return !(a == b);
    }
    friend bool operator<(const ObjectId& a, const ObjectId& b) noexcept {
        return a.hi_ < b.hi_ || (a.hi_ == b.hi_ && a.lo_ < b.lo_);
    }

private:
    ObjectId(uint64_t hi, uint64_t lo) noexcept : hi_(hi), lo_(lo) {}

    uint64_t hi_ = 0;
    uint64_t lo_ = 0;
};

}  // namespace ctf

namespace std {
template <>
struct hash<ctf::ObjectId> {
    size_t operator()(const ctf::ObjectId& id) const noexcept {
        return static_cast<size_t>(id.hi() ^ (id.lo() * 0x9E3779B97F4A7C15ull));
    }
};
}  // namespace std