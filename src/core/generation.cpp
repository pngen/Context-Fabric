#include "context_fabric/core/generation.h"

#include <cstdio>

#include "context_fabric/core/error.h"

namespace ctf {

Generation Generation::from_hex(const std::string& hex) {
    if (hex.empty() || hex.size() > 16) {
        throw_error(ErrKind::InvalidArgument, "invalid generation hex");
    }
    uint64_t v = 0;
    for (char c : hex) {
        uint64_t d;
        if (c >= '0' && c <= '9') d = static_cast<uint64_t>(c - '0');
        else if (c >= 'a' && c <= 'f') d = static_cast<uint64_t>(c - 'a' + 10);
        else if (c >= 'A' && c <= 'F') d = static_cast<uint64_t>(c - 'A' + 10);
        else throw_error(ErrKind::InvalidArgument, "invalid generation hex");
        v = (v << 4) | d;
    }
    if (v == 0) throw_error(ErrKind::InvalidArgument, "generation must be nonzero");
    return Generation(v);
}

std::string Generation::to_hex() const {
    char buf[17] = {0};
    snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(value_));
    return std::string(buf);
}

}  // namespace ctf