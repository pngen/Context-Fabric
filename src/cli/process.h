#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace ctf {

// ---------------------------------------------------------------------------
// Minimal cross-platform process spawning for the local fabric launcher.
// ---------------------------------------------------------------------------
struct ProcessHandle {
    void* handle = nullptr;  // platform handle
    uint64_t pid = 0;
    [[nodiscard]] bool valid() const noexcept { return handle != nullptr; }
};

// Spawn `exe` with `args` (args excludes argv[0]) and optional stdout/stderr
// redirection to `log_path`. Returns an invalid handle on failure.
ProcessHandle spawn_process(const std::string& exe, const std::vector<std::string>& args,
                            const std::string& log_path, const std::string& workdir);

// Wait for process exit up to `timeout_ms`. Returns -1 on timeout, else the
// exit code.
int wait_process(ProcessHandle& p, uint64_t timeout_ms);

// Terminate the process (best-effort).
void terminate_process(ProcessHandle& p);

// Cross-platform current process id helper.
[[nodiscard]] uint64_t current_pid();

}  // namespace ctf