#include "context_fabric/core/object_id.h"

#include <cstdio>
#include <cstring>
#include <random>

#include "context_fabric/core/error.h"

namespace ctf {

// SplitMix64: stable, portable, deterministic 64-bit mixing.
static uint64_t splitmix64(uint64_t& x) noexcept {
    uint64_t z = (x += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

uint64_t stable_hash64(const void* data, size_t len) noexcept {
    // FNV-1a over the bytes, then two rounds of splitmix to spread the bits.
    const unsigned char* p = static_cast<const unsigned char*>(data);
    uint64_t h = 14695981039346656037ull;
    for (size_t i = 0; i < len; ++i) {
        h ^= p[i];
        h *= 1099511628211ull;
    }
    uint64_t mix = h ^ static_cast<uint64_t>(len);
    splitmix64(mix);
    return splitmix64(mix);
}

uint64_t stable_hash64_str(const std::string& s) noexcept {
    return stable_hash64(s.data(), s.size());
}

ObjectId ObjectId::deterministic(const std::string& namespace_name, const std::string& name) {
    std::string canonical = namespace_name;
    canonical.push_back('@');
    canonical += name;
    uint64_t h = stable_hash64_str(canonical);
    return ObjectId(h, stable_hash64_str(canonical + "|cf"));
}

ObjectId ObjectId::random() {
    std::random_device rd;
    uint64_t a = (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
    uint64_t b = (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
    return ObjectId(a, b);
}

static uint8_t hex_nibble(char c) {
    if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
    if (c >= 'a' && c <= 'f') return static_cast<uint8_t>(c - 'a' + 10);
    if (c >= 'A' && c <= 'F') return static_cast<uint8_t>(c - 'A' + 10);
    return 0xFF;
}

ObjectId ObjectId::from_hex(const std::string& hex) {
    std::string clean;
    clean.reserve(hex.size());
    for (char c : hex) {
        if (c == '-' || c == ' ') continue;
        clean.push_back(c);
    }
    if (clean.size() != 32) throw_error(ErrKind::InvalidArgument, "Object id must be 32 hex digits");
    uint64_t hi = 0;
    uint64_t lo = 0;
    for (size_t i = 0; i < 16; ++i) {
        uint8_t a = hex_nibble(clean[i]);
        uint8_t b = hex_nibble(clean[i + 16]);
        if (a == 0xFF || b == 0xFF) throw_error(ErrKind::InvalidArgument, "non-hex digit in object id");
        hi = (hi << 4) | a;
        lo = (lo << 4) | b;
    }
    return ObjectId(hi, lo);
}

std::string ObjectId::to_hex() const {
    char buf[34] = {0};
    snprintf(buf, sizeof(buf), "%016llx-%016llx", static_cast<unsigned long long>(hi_),
             static_cast<unsigned long long>(lo_));
    return std::string(buf);
}

}  // namespace ctf