#pragma once

#include <stdexcept>
#include <string>
#include <cstdint>

namespace ctf {

// Typed error kinds. Every failure path in the runtime resolves to one of
// these so callers and the CLI can map them to deterministic outcomes.
enum class ErrKind : uint32_t {
    None = 0,
    InvalidArgument,   // caller supplied an invalid argument
    Protocol,          // malformed/unsupported protocol frame
    NotSupported,      // capability not implemented on this configuration
    NotFound,          // object/node/etc. does not exist
    AlreadyExists,     // creation collided with existing identity
    Conflict,          // ownership/lease/authority conflict
    StaleLease,        // lease token no longer authorizes the operation
    State,             // illegal lifecycle/state transition
    Compatibility,     // incompatible metadata for reuse
    Integrity,         // checksum/content verification failed
    Capacity,          // memory/storage/queue/capacity limit exceeded
    Timeout,           // operation timed out
    Transport,         // socket/transport failure
    Persistence,       // persistent store read/write failure
    Corrupt,           // corrupt record detected
    Cuda,              // CUDA runtime error
    Shutdown,          // system is shutting down
    Internal,          // unexpected internal failure
};

[[nodiscard]] const char* err_kind_name(ErrKind kind) noexcept;

class CfError : public std::runtime_error {
public:
    CfError(ErrKind kind, const std::string& message, uint64_t code = 0)
        : std::runtime_error(message), kind_(kind), code_(code) {}

    [[nodiscard]] ErrKind kind() const noexcept { return kind_; }
    [[nodiscard]] uint64_t code() const noexcept { return code_; }

    // Machine-readable description like "integrity:checksum-mismatch".
    [[nodiscard]] std::string code_string() const;

private:
    ErrKind kind_;
    uint64_t code_;
};

[[noreturn]] inline void throw_error(ErrKind kind, const std::string& message, uint64_t code = 0) {
    throw CfError(kind, message, code);
}

}  // namespace ctf