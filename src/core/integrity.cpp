#include "context_fabric/core/integrity.h"

#include <cstring>

#include "context_fabric/core/error.h"

#if defined(__x86_64__) || defined(_M_X64)
#define CF_HAS_X86 1
#include <nmmintrin.h>
#endif

namespace ctf {

// ---------------------------------------------------------------------------
// CRC-32C (castagnoli), reflected polynomial 0x82F63B78.
// ---------------------------------------------------------------------------
namespace {

static uint32_t crc32c_table[256];
static bool crc_table_ready = false;

void build_table() noexcept {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int k = 0; k < 8; ++k) {
            crc = (crc & 1) ? (crc >> 1) ^ 0x82F63B78u : (crc >> 1);
        }
        crc32c_table[i] = crc;
    }
    crc_table_ready = true;
}

uint32_t crc32c_soft(const uint8_t* p, size_t len, uint32_t crc) noexcept {
    if (!crc_table_ready) build_table();
    for (size_t i = 0; i < len; ++i) {
        crc = crc32c_table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc;
}

}  // namespace

void Crc32C::update(const void* data, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
#if defined(CF_HAS_X86)
    // Hardware-accelerated path (SSE4.2 crc32 instruction).
    size_t i = 0;
    while (len - i >= 8) {
        crc_ = static_cast<uint32_t>(_mm_crc32_u64(crc_, *reinterpret_cast<const uint64_t*>(p + i)));
        i += 8;
    }
    while (len - i >= 4) {
        crc_ = _mm_crc32_u32(crc_, *reinterpret_cast<const uint32_t*>(p + i));
        i += 4;
    }
    while (len - i >= 1) {
        crc_ = _mm_crc32_u8(crc_, p[i]);
        ++i;
    }
#else
    crc_ = crc32c_soft(p, len, crc_);
#endif
}

uint32_t Crc32C::compute(const void* data, size_t len) {
    Crc32C c;
    c.update(data, len);
    return c.value();
}

// ---------------------------------------------------------------------------
// SHA-256 (FIPS 180-4). Compact, dependency-free implementation.
// ---------------------------------------------------------------------------
static const uint32_t kShaK[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

static const uint32_t kShaInit[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                                     0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};

static uint32_t rotr(uint32_t x, int n) noexcept { return (x >> n) | (x << (32 - n)); }

void Sha256::process_block(const uint8_t* block) {
    uint32_t w[64];
    for (int i = 0; i < 16; ++i) {
        w[i] = (static_cast<uint32_t>(block[i * 4]) << 24) |
               (static_cast<uint32_t>(block[i * 4 + 1]) << 16) |
               (static_cast<uint32_t>(block[i * 4 + 2]) << 8) |
               static_cast<uint32_t>(block[i * 4 + 3]);
    }
    for (int i = 16; i < 64; ++i) {
        uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
        uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3];
    uint32_t e = h_[4], f = h_[5], g = h_[6], h = h_[7];
    for (int i = 0; i < 64; ++i) {
        uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
        uint32_t ch = (e & f) ^ (~e & g);
        uint32_t temp1 = h + S1 + ch + kShaK[i] + w[i];
        uint32_t S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
        uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
        uint32_t temp2 = S0 + maj;
        h = g; g = f; f = e; e = d + temp1;
        d = c; c = b; b = a; a = temp1 + temp2;
    }
    h_[0] += a; h_[1] += b; h_[2] += c; h_[3] += d;
    h_[4] += e; h_[5] += f; h_[6] += g; h_[7] += h;
}

void Sha256::update(const void* data, size_t len) {
    if (finished_) throw_error(ErrKind::State, "sha256 already finalized");
    const uint8_t* p = static_cast<const uint8_t*>(data);
    total_len_ += len;

    if (buf_len_ > 0) {
        size_t need = 64 - buf_len_;
        size_t take = len < need ? len : need;
        std::memcpy(buf_.data() + buf_len_, p, take);
        buf_len_ += take;
        p += take;
        len -= take;
        if (buf_len_ == 64) {
            process_block(buf_.data());
            buf_len_ = 0;
        }
    }
    while (len >= 64) {
        process_block(p);
        p += 64;
        len -= 64;
    }
    if (len > 0) {
        std::memcpy(buf_.data() + buf_len_, p, len);
        buf_len_ += len;
    }
}

std::array<uint8_t, 32> Sha256::finalize() {
    if (finished_) {
        std::array<uint8_t, 32> out{};
        for (int i = 0; i < 8; ++i) {
            out[i * 4] = static_cast<uint8_t>(h_[i] >> 24);
            out[i * 4 + 1] = static_cast<uint8_t>(h_[i] >> 16);
            out[i * 4 + 2] = static_cast<uint8_t>(h_[i] >> 8);
            out[i * 4 + 3] = static_cast<uint8_t>(h_[i]);
        }
        return out;
    }
    uint64_t bit_len = total_len_ * 8;
    uint8_t pad = 0x80;
    update(&pad, 1);
    uint8_t zero = 0;
    while (buf_len_ != 56) {
        if (buf_len_ == 64) {  // process a full block, then continue padding
            process_block(buf_.data());
            buf_len_ = 0;
        }
        update(&zero, 1);
    }
    uint8_t len_bytes[8];
    for (int i = 0; i < 8; ++i) len_bytes[i] = static_cast<uint8_t>(bit_len >> (8 * (7 - i)));
    update(len_bytes, 8);
    finished_ = true;

    std::array<uint8_t, 32> out{};
    for (int i = 0; i < 8; ++i) {
        out[i * 4] = static_cast<uint8_t>(h_[i] >> 24);
        out[i * 4 + 1] = static_cast<uint8_t>(h_[i] >> 16);
        out[i * 4 + 2] = static_cast<uint8_t>(h_[i] >> 8);
        out[i * 4 + 3] = static_cast<uint8_t>(h_[i]);
    }
    return out;
}

uint64_t Sha256::fingerprint() const {
    // Returns first 8 bytes of the current digest (only meaningful after finalize).
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint64_t>(h_[i] >> 24);
    return v;
}

std::array<uint8_t, 32> Sha256::compute(const void* data, size_t len) {
    Sha256 s;
    s.update(data, len);
    return s.finalize();
}

void IntegrityBuilder::update(const void* data, size_t len) {
    crc_.update(data, len);
    sha_.update(data, len);
    bytes_ += len;
}

namespace {

uint64_t compute_sha_fingerprint(Sha256& s) {
    auto digest = s.finalize();
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | digest[i];
    return v;
}

}  // namespace

IntegrityId IntegrityBuilder::finish() {
    IntegrityId id;
    id.crc32c = crc_.value();
    id.sha = compute_sha_fingerprint(sha_);
    id.bytes = static_cast<uint32_t>(bytes_);
    return id;
}

IntegrityId compute_integrity(const void* data, size_t len) {
    IntegrityBuilder b;
    b.update(data, len);
    return b.finish();
}

}  // namespace ctf