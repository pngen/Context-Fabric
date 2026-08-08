#pragma once

#include <cstdint>
#include <string>
#include <chrono>

namespace ctf {

// ---------------------------------------------------------------------------
// Time helpers.
// ---------------------------------------------------------------------------
[[nodiscard]] uint64_t now_unix_ms() noexcept;
[[nodiscard]] uint64_t steady_now_ms() noexcept;

using SteadyClock = std::chrono::steady_clock;
using SteadyPoint = SteadyClock::time_point;

[[nodiscard]] inline SteadyPoint steady_deadline(uint64_t timeout_ms) {
    return SteadyClock::now() + std::chrono::milliseconds(timeout_ms);
}

// ---------------------------------------------------------------------------
// Strict byte-size parsing and formatting.
// Accepts plain integers and SI/IEC suffixes: B, KB/KiB, MB/MiB, GB/GiB, TB/TiB.
// Case-insensitive. Returns false on malformed input.
// ---------------------------------------------------------------------------
[[nodiscard]] bool parse_byte_size(const std::string& s, uint64_t& out);
[[nodiscard]] std::string format_bytes(uint64_t bytes);
[[nodiscard]] std::string format_bytes_si(double bytes);

}  // namespace ctf