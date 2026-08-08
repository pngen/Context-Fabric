#include "process.h"

#include <cstdio>
#include <string>
#include <vector>

#include "context_fabric/core/error.h"

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#endif

namespace ctf {

uint64_t current_pid() {
#if defined(_WIN32)
    return static_cast<uint64_t>(GetCurrentProcessId());
#else
    return static_cast<uint64_t>(getpid());
#endif
}

ProcessHandle spawn_process(const std::string& exe, const std::vector<std::string>& args,
                            const std::string& log_path, const std::string& workdir) {
    ProcessHandle p;
#if defined(_WIN32)
    std::string cmdline = "\"" + exe + "\"";
    for (const auto& a : args) cmdline += " \"" + a + "\"";

    SECURITY_ATTRIBUTES sa;
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;
    sa.lpSecurityDescriptor = nullptr;
    HANDLE log_handle = INVALID_HANDLE_VALUE;
    bool have_log = false;
    if (!log_path.empty()) {
        log_handle = CreateFileA(log_path.c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, &sa,
                                 CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        have_log = log_handle != INVALID_HANDLE_VALUE;
    }

    STARTUPINFOA si;
    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);
    si.dwFlags = STARTF_USESTDHANDLES;
    HANDLE stdin_h = GetStdHandle(STD_INPUT_HANDLE);
    HANDLE stdout_h = have_log ? log_handle : GetStdHandle(STD_OUTPUT_HANDLE);
    HANDLE stderr_h = have_log ? log_handle : GetStdHandle(STD_ERROR_HANDLE);
    if (stdin_h == nullptr || stdin_h == INVALID_HANDLE_VALUE) {
        // Create a valid null handle for the child.
        SECURITY_ATTRIBUTES sa2;
        sa2.nLength = sizeof(sa2);
        sa2.bInheritHandle = TRUE;
        sa2.lpSecurityDescriptor = nullptr;
        stdin_h = CreateFileA("NUL", GENERIC_READ, FILE_SHARE_READ, &sa2, OPEN_EXISTING, 0, nullptr);
    }
    if (stdout_h == nullptr || stdout_h == INVALID_HANDLE_VALUE) {
        TCHAR nulpath[] = TEXT("NUL");
        stdout_h = CreateFileA("NUL", GENERIC_WRITE, FILE_SHARE_WRITE, nullptr, OPEN_EXISTING, 0, nullptr);
        stderr_h = stdout_h;
        (void)nulpath;
    }
    si.hStdInput = stdin_h;
    si.hStdOutput = stdout_h;
    si.hStdError = stderr_h;

    PROCESS_INFORMATION pi;
    ZeroMemory(&pi, sizeof(pi));
    std::string cdir = workdir.empty() ? std::string() : workdir;
    BOOL ok = CreateProcessA(exe.c_str(), &cmdline[0], nullptr, nullptr, TRUE, 0, nullptr,
                             cdir.empty() ? nullptr : cdir.c_str(), &si, &pi);
    if (have_log) CloseHandle(log_handle);
    if (!ok) {
        fprintf(stderr, "spawn_process: CreateProcessA failed with error %lu\n", (unsigned long)GetLastError());
        return p;
    }
    CloseHandle(pi.hThread);
    p.handle = pi.hProcess;
    p.pid = static_cast<uint64_t>(pi.dwProcessId);
#else
    pid_t pid = fork();
    if (pid < 0) return p;
    if (pid == 0) {
        // Child.
        if (!log_path.empty()) {
            int fd = open(log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd >= 0) {
                dup2(fd, STDOUT_FILENO);
                dup2(fd, STDERR_FILENO);
                close(fd);
            }
        }
        if (!workdir.empty()) chdir(workdir.c_str());
        std::vector<std::string> argv0;
        argv0.push_back(exe);
        std::vector<char*> cargs;
        for (auto& a : argv0) cargs.push_back(&a[0]);
        for (auto& a : args) cargs.push_back(&a[0]);
        cargs.push_back(nullptr);
        execv(exe.c_str(), cargs.data());
        _exit(127);
    }
    p.handle = reinterpret_cast<void*>(static_cast<uintptr_t>(pid));
    p.pid = static_cast<uint64_t>(pid);
#endif
    return p;
}

int wait_process(ProcessHandle& p, uint64_t timeout_ms) {
    if (!p.valid()) return -1;
#if defined(_WIN32)
    DWORD wait = WaitForSingleObject(static_cast<HANDLE>(p.handle), static_cast<DWORD>(timeout_ms));
    if (wait == WAIT_TIMEOUT) return -1;
    DWORD code = 1;
    GetExitCodeProcess(static_cast<HANDLE>(p.handle), &code);
    CloseHandle(static_cast<HANDLE>(p.handle));
    p.handle = nullptr;
    return static_cast<int>(code);
#else
    int status = 0;
    uint64_t waited = 0;
    while (waited < timeout_ms || timeout_ms == 0) {
        pid_t r = waitpid(static_cast<pid_t>(reinterpret_cast<uintptr_t>(p.handle)), &status, WNOHANG);
        if (r == static_cast<pid_t>(reinterpret_cast<uintptr_t>(p.handle))) {
            p.handle = nullptr;
            if (WIFEXITED(status)) return WEXITSTATUS(status);
            return -1;
        }
        if (r < 0) return -1;
        usleep(10000);
        waited += 10;
    }
    return -1;
#endif
}

void terminate_process(ProcessHandle& p) {
    if (!p.valid()) return;
#if defined(_WIN32)
    TerminateProcess(static_cast<HANDLE>(p.handle), 1);
    CloseHandle(static_cast<HANDLE>(p.handle));
#else
    kill(static_cast<pid_t>(reinterpret_cast<uintptr_t>(p.handle)), SIGKILL);
    waitpid(static_cast<pid_t>(reinterpret_cast<uintptr_t>(p.handle)), nullptr, 0);
#endif
    p.handle = nullptr;
}

}  // namespace ctf