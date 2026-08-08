#pragma once

#include <cstdint>
#include <vector>

namespace ctf {

// ---------------------------------------------------------------------------
// Deterministic seeded PRNG (xoshiro256**) and seeded content generation.
// All benchmark/validation object content is generated from these so that
// byte-correctness checks are reproducible across runs and machines.
// ---------------------------------------------------------------------------
class Prng {
public:
    explicit Prng(uint64_t seed) noexcept { seed_state(seed); }

    void seed_state(uint64_t seed) noexcept;
    uint64_t next_u64() noexcept;
    uint32_t next_u32() noexcept { return static_cast<uint32_t>(next_u64() >> 32); }
    uint8_t next_u8() noexcept { return static_cast<uint8_t>(next_u64() >> 56); }
    double next_f64() noexcept {  // [0,1)
        return static_cast<double>(next_u64() >> 11) * (1.0 / 9007199254740992.0);
    }

private:
    uint64_t s_[4] = {};
    static void rotl(uint64_t& x, int k) noexcept { x = (x << k) | (x >> (64 - k)); }
};

// Fill `out` with deterministic bytes derived from (seed, offset).
void fill_deterministic(uint64_t seed, uint64_t offset, uint8_t* out, size_t n);

// Deterministic content generator used by tests/benchmarks/validation.
// Pattern is a secondary discriminator so different "objects" with the same
// seed still differ.
std::vector<uint8_t> generate_content(uint64_t seed, uint64_t pattern, size_t size);

}  // namespace ctf