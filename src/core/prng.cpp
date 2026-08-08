#include "context_fabric/core/prng.h"

#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"

namespace ctf {

void Prng::seed_state(uint64_t seed) noexcept {
    // SplitMix64 expansion of the seed into the four state words.
    uint64_t x = seed;
    auto step = [&x]() noexcept {
        uint64_t z = (x += 0x9E3779B97F4A7C15ull);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
        return z ^ (z >> 31);
    };
    s_[0] = step();
    s_[1] = step();
    s_[2] = step();
    s_[3] = step();
    if ((s_[0] | s_[1] | s_[2] | s_[3]) == 0) s_[0] = 1;
}

uint64_t Prng::next_u64() noexcept {
    uint64_t mul = s_[1] * 5;
    const uint64_t result = ((mul << 7) | (mul >> 57)) * 9;
    const uint64_t t = s_[1] << 17;
    s_[2] ^= s_[0];
    s_[3] ^= s_[1];
    s_[1] ^= s_[2];
    s_[0] ^= s_[3];
    s_[2] ^= t;
    s_[3] = (s_[3] << 45) | (s_[3] >> 19);
    return result;
}

void fill_deterministic(uint64_t seed, uint64_t offset, uint8_t* out, size_t n) {
    if (n != 0 && out == nullptr) {
        throw_error(ErrKind::InvalidArgument, "deterministic fill requires a non-null destination");
    }
    Prng rng(seed ^ (offset * 0x9E3779B97F4A7C15ull));
    size_t i = 0;
    while (i + 8 <= n) {
        const uint64_t v = rng.next_u64();
        // Fixed little-endian byte order preserves identical generated
        // content on every host architecture.
        for (size_t byte = 0; byte < 8; ++byte) {
            out[i + byte] = static_cast<uint8_t>(v >> (byte * 8));
        }
        i += 8;
    }
    while (i < n) {
        out[i] = rng.next_u8();
        ++i;
    }
}

std::vector<uint8_t> generate_content(uint64_t seed, uint64_t pattern, size_t size) {
    std::vector<uint8_t> v(size);
    fill_deterministic(seed ^ pattern, 0, v.data(), v.size());
    return v;
}

}  // namespace ctf
