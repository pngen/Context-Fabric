#include "context_fabric/core/clock.h"

#include <chrono>
#include <cctype>
#include <cstdlib>

namespace ctf {

uint64_t now_unix_ms() noexcept {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
}

uint64_t steady_now_ms() noexcept {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count());
}

bool parse_byte_size(const std::string& s, uint64_t& out) {
    if (s.empty()) return false;
    size_t i = 0;
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
    if (i >= s.size() || !std::isdigit(static_cast<unsigned char>(s[i]))) return false;

    uint64_t value = 0;
    bool overflow = false;
    while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) {
        uint64_t d = static_cast<uint64_t>(s[i] - '0');
        if (value > (UINT64_MAX - d) / 10) {
            overflow = true;
            break;
        }
        value = value * 10 + d;
        ++i;
    }
    if (overflow) return false;

    // Optional fractional part (e.g. "1.5GiB")
    double frac = 0.0;
    if (i < s.size() && s[i] == '.') {
        ++i;
        double place = 0.1;
        while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) {
            frac += static_cast<double>(s[i] - '0') * place;
            place *= 0.1;
            ++i;
        }
    }

    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
    if (i >= s.size()) {
        out = value;
        return true;
    }

    std::string unit;
    unit.reserve(4);
    while (i < s.size() && !std::isspace(static_cast<unsigned char>(s[i]))) unit.push_back(s[i++]);
    for (auto& c : unit) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

    uint64_t mult = 1;
    double frac_mult = 1.0;
    if (unit == "B") { mult = 1; frac_mult = 1.0; }
    else if (unit == "KB") { mult = 1000; frac_mult = 1000.0; }
    else if (unit == "MB") { mult = 1000ULL * 1000; frac_mult = 1e6; }
    else if (unit == "GB") { mult = 1000ULL * 1000 * 1000; frac_mult = 1e9; }
    else if (unit == "TB") { mult = 1000ULL * 1000 * 1000 * 1000; frac_mult = 1e12; }
    else if (unit == "KIB") { mult = 1024; frac_mult = 1024.0; }
    else if (unit == "MIB") { mult = 1024ULL * 1024; frac_mult = 1024.0 * 1024; }
    else if (unit == "GIB") { mult = 1024ULL * 1024 * 1024; frac_mult = 1024.0 * 1024 * 1024; }
    else if (unit == "TIB") { mult = 1024ULL * 1024 * 1024 * 1024; frac_mult = 1024.0 * 1024 * 1024 * 1024; }
    else return false;

    if (value > (UINT64_MAX / mult)) return false;
    uint64_t base = value * mult;
    double extra = frac * frac_mult;
    if (extra > static_cast<double>(UINT64_MAX - base)) return false;
    out = base + static_cast<uint64_t>(extra);
    return true;
}

std::string format_bytes(uint64_t bytes) {
    const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    double v = static_cast<double>(bytes);
    int idx = 0;
    while (v >= 1024.0 && idx < 4) {
        v /= 1024.0;
        ++idx;
    }
    char buf[32];
    if (idx == 0) {
        snprintf(buf, sizeof(buf), "%llu B", static_cast<unsigned long long>(bytes));
    } else {
        snprintf(buf, sizeof(buf), "%.2f %s", v, units[idx]);
    }
    return std::string(buf);
}

std::string format_bytes_si(double bytes) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    double v = bytes;
    int idx = 0;
    while (v >= 1000.0 && idx < 4) {
        v /= 1000.0;
        ++idx;
    }
    char buf[32];
    snprintf(buf, sizeof(buf), "%.2f %s", v, units[idx]);
    return std::string(buf);
}

}  // namespace ctf