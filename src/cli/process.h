#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace ctf {

// ---------------------------------------------------------------------------
// Minimal cross-platform process spawning for the local fabric launcher.
// ---------------------------------------------------------------------------
struct ProcessHandle {
    ProcessHandle() = default;
    ~ProcessHandle();

    ProcessHandle(const ProcessHandle&) = delete;
    ProcessHandle& operator=(const ProcessHandle&) = delete;
    ProcessHandle(ProcessHandle&& other) noexcept;
    ProcessHandle& operator=(ProcessHandle&& other) noexcept;

    void* handle = nullptr;       // process handle on Windows; encoded pid on POSIX
    void* containment = nullptr;  // kill-on-close job on Windows
    uint64_t pid = 0;
    [[nodiscard]] bool valid() const noexcept { return handle != nullptr; }
};

// Spawn `exe` with `args` (args excludes argv[0]) and optional stdout/stderr
// redirection to `log_path`. Returns an invalid handle on failure.
ProcessHandle spawn_process(const std::string& exe, const std::vector<std::string>& args,
                            const std::string& log_path, const std::string& workdir);

// Wait for process exit up to `timeout_ms`. A zero timeout is a non-blocking
// poll on every platform. Returns -1 on timeout, -2 on a wait/signalled-exit
// failure, or the non-negative process exit code. A completed wait releases
// the handle.
int wait_process(ProcessHandle& p, uint64_t timeout_ms);

// Terminate the complete owned process tree and wait at most `timeout_ms` for
// it to disappear. Always releases the handle; returns true only when tree
// exit was confirmed.
bool terminate_process(ProcessHandle& p, uint64_t timeout_ms = 5000);

// Cross-platform current process id helper.
[[nodiscard]] uint64_t current_pid();

// Absolute path of the running executable, or an empty string when the
// platform cannot discover it.
[[nodiscard]] std::string current_executable_path();

}  // namespace ctf
