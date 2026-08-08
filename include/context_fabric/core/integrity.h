#pragma once

#include <cstdint>
#include <array>
#include <vector>
#include <string>

#include "context_fabric/core/types.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Integrity primitives.
//  - crc32c: CRC-32C (castagnoli), accelerated with SSE4.2 on x86 when
//    available; used as the fast streaming checksum at every boundary.
//  - sha256: full SHA-256 for strong full verification; the public IntegrityId
//    exposes the first 8 bytes as a fingerprint.
// Content generation is deterministic and seeded (see prng.h).
// ---------------------------------------------------------------------------
class Crc32C {
public:
    Crc32C() = default;
    void update(const void* data, size_t len);
    [[nodiscard]] uint32_t value() const noexcept { return crc_; }
    static uint32_t compute(const void* data, size_t len);

private:
    uint32_t crc_ = 0xFFFFFFFFu;
};

class Sha256 {
public:
    Sha256() = default;
    void update(const void* data, size_t len);
    std::array<uint8_t, 32> finalize();
    [[nodiscard]] uint64_t fingerprint() const;   // first 8 bytes of digest
    static std::array<uint8_t, 32> compute(const void* data, size_t len);

private:
    void process_block(const uint8_t* block);
    uint32_t h_[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                      0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    std::array<uint8_t, 64> buf_{};
    size_t buf_len_ = 0;
    uint64_t total_len_ = 0;
    bool finished_ = false;
};

// Streaming integrity tracker combining both digests over one buffer.
struct IntegrityBuilder {
    void update(const void* data, size_t len);
    [[nodiscard]] uint64_t crc32c() const noexcept { return crc_.value(); }
    [[nodiscard]] uint64_t sha_fingerprint() const { return sha_.fingerprint(); }
    [[nodiscard]] uint64_t bytes_hashed() const noexcept { return bytes_; }

    // Finalize and produce the combined IntegrityId. Safe to call once, after
    // all updates.
    IntegrityId finish();

    Crc32C crc_;
    Sha256 sha_;
    uint64_t bytes_ = 0;
};

// One-shot combined digest over a buffer.
[[nodiscard]] IntegrityId compute_integrity(const void* data, size_t len);

}  // namespace ctf