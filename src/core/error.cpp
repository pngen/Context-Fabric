#include "context_fabric/core/error.h"

namespace ctf {

const char* err_kind_name(ErrKind kind) noexcept {
    switch (kind) {
        case ErrKind::None: return "none";
        case ErrKind::InvalidArgument: return "invalid-argument";
        case ErrKind::Protocol: return "protocol";
        case ErrKind::NotSupported: return "not-supported";
        case ErrKind::NotFound: return "not-found";
        case ErrKind::AlreadyExists: return "already-exists";
        case ErrKind::Conflict: return "conflict";
        case ErrKind::StaleLease: return "stale-lease";
        case ErrKind::State: return "state";
        case ErrKind::Compatibility: return "compatibility";
        case ErrKind::Integrity: return "integrity";
        case ErrKind::Capacity: return "capacity";
        case ErrKind::Timeout: return "timeout";
        case ErrKind::Transport: return "transport";
        case ErrKind::Persistence: return "persistence";
        case ErrKind::Corrupt: return "corrupt";
        case ErrKind::Cuda: return "cuda";
        case ErrKind::Shutdown: return "shutdown";
        case ErrKind::Internal: return "internal";
    }
    return "unknown";
}

std::string CfError::code_string() const {
    std::string s = err_kind_name(kind_);
    if (code_ != 0) {
        s += ":";
        s += std::to_string(code_);
    }
    return s;
}

}  // namespace ctf