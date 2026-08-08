#include "process.h"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#else
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
#endif

namespace ctf {

namespace {

constexpr int kWaitTimeout = -1;
constexpr int kWaitFailed = -2;

#if defined(_WIN32)

std::wstring native_path(const std::string& path) {
    return std::filesystem::path(path).wstring();
}

// Quote one argument according to the CommandLineToArgvW/MSVC argv rules.
// In particular, backslashes immediately before a quote (or the closing
// quote) must be doubled.
std::wstring quote_windows_arg(const std::wstring& arg) {
    if (arg.empty()) return L"\"\"";
    if (arg.find_first_of(L" \t\n\v\"") == std::wstring::npos) return arg;

    std::wstring out = L"\"";
    size_t backslashes = 0;
    for (wchar_t c : arg) {
        if (c == L'\\') {
            ++backslashes;
            continue;
        }
        if (c == L'\"') {
            out.append(backslashes * 2 + 1, L'\\');
            out.push_back(c);
            backslashes = 0;
            continue;
        }
        out.append(backslashes, L'\\');
        backslashes = 0;
        out.push_back(c);
    }
    out.append(backslashes * 2, L'\\');
    out.push_back(L'\"');
    return out;
}

HANDLE inheritable_standard_handle(DWORD which, DWORD null_access) {
    HANDLE source = GetStdHandle(which);
    HANDLE duplicate = INVALID_HANDLE_VALUE;
    if (source != nullptr && source != INVALID_HANDLE_VALUE &&
        DuplicateHandle(GetCurrentProcess(), source, GetCurrentProcess(), &duplicate, 0, TRUE,
                        DUPLICATE_SAME_ACCESS)) {
        return duplicate;
    }

    SECURITY_ATTRIBUTES sa{};
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;
    return CreateFileW(L"NUL", null_access, FILE_SHARE_READ | FILE_SHARE_WRITE, &sa, OPEN_EXISTING,
                       FILE_ATTRIBUTE_NORMAL, nullptr);
}

void close_windows_handle(void*& value) noexcept {
    if (value != nullptr) CloseHandle(static_cast<HANDLE>(value));
    value = nullptr;
}

bool windows_job_empty(HANDLE job) {
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION info{};
    return QueryInformationJobObject(job, JobObjectBasicAccountingInformation, &info, sizeof(info),
                                     nullptr) != FALSE &&
           info.ActiveProcesses == 0;
}

#else

pid_t process_pid(const ProcessHandle& p) {
    return static_cast<pid_t>(reinterpret_cast<uintptr_t>(p.handle));
}

bool process_group_exists(pid_t pgid) {
    if (pgid <= 0) return false;
    if (kill(-pgid, 0) == 0) return true;
    return errno == EPERM;
}

void set_cloexec(int fd) {
    int flags = fcntl(fd, F_GETFD);
    if (flags >= 0) (void)fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
}

#endif

}  // namespace

ProcessHandle::~ProcessHandle() {
    if (valid()) (void)terminate_process(*this);
}

ProcessHandle::ProcessHandle(ProcessHandle&& other) noexcept
    : handle(other.handle), containment(other.containment), pid(other.pid) {
    other.handle = nullptr;
    other.containment = nullptr;
    other.pid = 0;
}

ProcessHandle& ProcessHandle::operator=(ProcessHandle&& other) noexcept {
    if (this == &other) return *this;
    if (valid()) (void)terminate_process(*this);
    handle = other.handle;
    containment = other.containment;
    pid = other.pid;
    other.handle = nullptr;
    other.containment = nullptr;
    other.pid = 0;
    return *this;
}

uint64_t current_pid() {
#if defined(_WIN32)
    return static_cast<uint64_t>(GetCurrentProcessId());
#else
    return static_cast<uint64_t>(getpid());
#endif
}

std::string current_executable_path() {
#if defined(_WIN32)
    std::vector<wchar_t> buffer(512);
    for (;;) {
        DWORD n = GetModuleFileNameW(nullptr, buffer.data(), static_cast<DWORD>(buffer.size()));
        if (n == 0) return {};
        if (n < buffer.size() - 1) {
            return std::filesystem::path(std::wstring(buffer.data(), n)).string();
        }
        if (buffer.size() >= 32768) return {};
        buffer.resize(std::min<size_t>(buffer.size() * 2, 32768));
    }
#elif defined(__APPLE__)
    uint32_t size = 0;
    (void)_NSGetExecutablePath(nullptr, &size);
    if (size == 0) return {};
    std::vector<char> buffer(size);
    if (_NSGetExecutablePath(buffer.data(), &size) != 0) return {};
    std::error_code ec;
    auto canonical = std::filesystem::canonical(buffer.data(), ec);
    return ec ? std::string(buffer.data()) : canonical.string();
#elif defined(__linux__)
    std::vector<char> buffer(512);
    for (;;) {
        ssize_t n = readlink("/proc/self/exe", buffer.data(), buffer.size());
        if (n < 0) return {};
        if (static_cast<size_t>(n) < buffer.size()) return std::string(buffer.data(), static_cast<size_t>(n));
        if (buffer.size() >= 1024 * 1024) return {};
        buffer.resize(buffer.size() * 2);
    }
#else
    return {};
#endif
}

ProcessHandle spawn_process(const std::string& exe, const std::vector<std::string>& args,
                            const std::string& log_path, const std::string& workdir) {
    ProcessHandle p;
    if (exe.empty()) {
        std::fprintf(stderr, "spawn_process: executable path is empty\n");
        return p;
    }
#if defined(_WIN32)
    std::wstring exe_w;
    std::wstring workdir_w;
    std::wstring log_w;
    try {
        exe_w = native_path(exe);
        if (!workdir.empty()) workdir_w = native_path(workdir);
        if (!log_path.empty()) log_w = native_path(log_path);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "spawn_process: invalid path encoding: %s\n", e.what());
        return p;
    }

    std::wstring cmdline = quote_windows_arg(exe_w);
    for (const auto& a : args) {
        std::wstring arg_w;
        try {
            arg_w = std::filesystem::path(a).wstring();
        } catch (const std::exception& e) {
            std::fprintf(stderr, "spawn_process: invalid argument encoding: %s\n", e.what());
            return p;
        }
        cmdline.push_back(L' ');
        cmdline += quote_windows_arg(arg_w);
    }
    std::vector<wchar_t> mutable_cmdline(cmdline.begin(), cmdline.end());
    mutable_cmdline.push_back(L'\0');

    SECURITY_ATTRIBUTES sa{};
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;
    HANDLE log_handle = INVALID_HANDLE_VALUE;
    if (!log_path.empty()) {
        log_handle = CreateFileW(log_w.c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, &sa,
                                 CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (log_handle == INVALID_HANDLE_VALUE) {
            std::fprintf(stderr, "spawn_process: cannot open log (error %lu)\n",
                         static_cast<unsigned long>(GetLastError()));
            return p;
        }
    }

    HANDLE stdin_h = inheritable_standard_handle(STD_INPUT_HANDLE, GENERIC_READ);
    HANDLE stdout_h = log_handle != INVALID_HANDLE_VALUE
                          ? log_handle
                          : inheritable_standard_handle(STD_OUTPUT_HANDLE, GENERIC_WRITE);
    HANDLE stderr_h = log_handle != INVALID_HANDLE_VALUE
                          ? log_handle
                          : inheritable_standard_handle(STD_ERROR_HANDLE, GENERIC_WRITE);
    if (stdin_h == INVALID_HANDLE_VALUE || stdout_h == INVALID_HANDLE_VALUE ||
        stderr_h == INVALID_HANDLE_VALUE) {
        if (stdin_h != INVALID_HANDLE_VALUE) CloseHandle(stdin_h);
        if (log_handle == INVALID_HANDLE_VALUE && stdout_h != INVALID_HANDLE_VALUE) CloseHandle(stdout_h);
        if (log_handle == INVALID_HANDLE_VALUE && stderr_h != INVALID_HANDLE_VALUE) CloseHandle(stderr_h);
        if (log_handle != INVALID_HANDLE_VALUE) CloseHandle(log_handle);
        std::fprintf(stderr, "spawn_process: cannot prepare standard handles (error %lu)\n",
                     static_cast<unsigned long>(GetLastError()));
        return p;
    }

    std::vector<HANDLE> inherited = {stdin_h, stdout_h, stderr_h};
    std::sort(inherited.begin(), inherited.end());
    inherited.erase(std::unique(inherited.begin(), inherited.end()), inherited.end());

    SIZE_T attributes_size = 0;
    (void)InitializeProcThreadAttributeList(nullptr, 1, 0, &attributes_size);
    std::vector<unsigned char> attributes_storage(attributes_size);
    auto* attributes = reinterpret_cast<LPPROC_THREAD_ATTRIBUTE_LIST>(attributes_storage.data());
    bool attributes_initialized = !attributes_storage.empty() &&
                                  InitializeProcThreadAttributeList(attributes, 1, 0, &attributes_size) != FALSE;
    bool attributes_ready = false;
    if (attributes_initialized) {
        attributes_ready =
            UpdateProcThreadAttribute(attributes, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, inherited.data(),
                                      inherited.size() * sizeof(HANDLE), nullptr, nullptr) != FALSE;
    }

    HANDLE job = CreateJobObjectW(nullptr, nullptr);
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits{};
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    bool job_ready = job != nullptr &&
                     SetInformationJobObject(job, JobObjectExtendedLimitInformation, &limits,
                                             sizeof(limits)) != FALSE;

    STARTUPINFOEXW si{};
    si.StartupInfo.cb = sizeof(si);
    si.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    si.StartupInfo.hStdInput = stdin_h;
    si.StartupInfo.hStdOutput = stdout_h;
    si.StartupInfo.hStdError = stderr_h;
    si.lpAttributeList = attributes;

    PROCESS_INFORMATION pi{};
    BOOL ok = FALSE;
    if (attributes_ready && job_ready) {
        constexpr DWORD flags = CREATE_NO_WINDOW | CREATE_SUSPENDED | EXTENDED_STARTUPINFO_PRESENT;
        ok = CreateProcessW(exe_w.c_str(), mutable_cmdline.data(), nullptr, nullptr, TRUE, flags, nullptr,
                            workdir_w.empty() ? nullptr : workdir_w.c_str(), &si.StartupInfo, &pi);
    }
    DWORD create_error = ok ? ERROR_SUCCESS : GetLastError();

    if (attributes_initialized) DeleteProcThreadAttributeList(attributes);
    CloseHandle(stdin_h);
    if (log_handle == INVALID_HANDLE_VALUE) {
        CloseHandle(stdout_h);
        CloseHandle(stderr_h);
    } else {
        CloseHandle(log_handle);
    }

    if (!ok) {
        if (job != nullptr) CloseHandle(job);
        std::fprintf(stderr, "spawn_process: CreateProcessW failed with error %lu\n",
                     static_cast<unsigned long>(create_error));
        return p;
    }

    if (!AssignProcessToJobObject(job, pi.hProcess)) {
        DWORD assign_error = GetLastError();
        (void)TerminateProcess(pi.hProcess, 1);
        (void)WaitForSingleObject(pi.hProcess, 5000);
        CloseHandle(pi.hThread);
        CloseHandle(pi.hProcess);
        CloseHandle(job);
        std::fprintf(stderr, "spawn_process: cannot assign child to ownership job (error %lu)\n",
                     static_cast<unsigned long>(assign_error));
        return p;
    }
    if (ResumeThread(pi.hThread) == static_cast<DWORD>(-1)) {
        (void)TerminateJobObject(job, 1);
        (void)WaitForSingleObject(pi.hProcess, 5000);
        CloseHandle(pi.hThread);
        CloseHandle(pi.hProcess);
        CloseHandle(job);
        std::fprintf(stderr, "spawn_process: cannot resume child (error %lu)\n",
                     static_cast<unsigned long>(GetLastError()));
        return p;
    }

    CloseHandle(pi.hThread);
    p.handle = pi.hProcess;
    p.containment = job;
    p.pid = static_cast<uint64_t>(pi.dwProcessId);
#else
    int error_pipe[2] = {-1, -1};
    if (pipe(error_pipe) != 0) {
        std::fprintf(stderr, "spawn_process: pipe failed: %s\n", std::strerror(errno));
        return p;
    }
    set_cloexec(error_pipe[0]);
    set_cloexec(error_pipe[1]);

    int log_fd = -1;
    if (!log_path.empty()) {
        log_fd = open(log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (log_fd < 0) {
            std::fprintf(stderr, "spawn_process: cannot open log: %s\n", std::strerror(errno));
            close(error_pipe[0]);
            close(error_pipe[1]);
            return p;
        }
    }

    // Build argv before fork: the child must avoid allocator and libc state
    // that may be locked by another thread between fork and exec.
    std::vector<std::string> owned_args;
    owned_args.reserve(args.size() + 1);
    owned_args.push_back(exe);
    owned_args.insert(owned_args.end(), args.begin(), args.end());
    std::vector<char*> cargs;
    cargs.reserve(owned_args.size() + 1);
    for (auto& a : owned_args) cargs.push_back(a.data());
    cargs.push_back(nullptr);
    bool search_path = exe.find('/') == std::string::npos;

    pid_t pid = fork();
    if (pid < 0) {
        std::fprintf(stderr, "spawn_process: fork failed: %s\n", std::strerror(errno));
        if (log_fd >= 0) close(log_fd);
        close(error_pipe[0]);
        close(error_pipe[1]);
        return p;
    }
    if (pid == 0) {
        close(error_pipe[0]);
        auto fail_exec = [&](int error) {
            (void)write(error_pipe[1], &error, sizeof(error));
            _exit(126);
        };
        if (setpgid(0, 0) != 0) fail_exec(errno);
        if (log_fd >= 0) {
            if (dup2(log_fd, STDOUT_FILENO) < 0 || dup2(log_fd, STDERR_FILENO) < 0) fail_exec(errno);
            close(log_fd);
        }
        if (!workdir.empty() && chdir(workdir.c_str()) != 0) fail_exec(errno);
        if (search_path) {
            execvp(exe.c_str(), cargs.data());
        } else {
            execv(exe.c_str(), cargs.data());
        }
        fail_exec(errno);
    }

    close(error_pipe[1]);
    if (log_fd >= 0) close(log_fd);
    if (setpgid(pid, pid) != 0 && errno != EACCES && errno != EEXIST) {
        std::fprintf(stderr, "spawn_process: setpgid failed: %s\n", std::strerror(errno));
    }

    p.handle = reinterpret_cast<void*>(static_cast<uintptr_t>(pid));
    p.pid = static_cast<uint64_t>(pid);

    pollfd ready{};
    ready.fd = error_pipe[0];
    ready.events = POLLIN | POLLHUP;
    int poll_result = poll(&ready, 1, 5000);
    int child_error = 0;
    ssize_t received = poll_result > 0 ? read(error_pipe[0], &child_error, sizeof(child_error)) : -1;
    close(error_pipe[0]);
    if (poll_result <= 0 || received > 0) {
        if (poll_result == 0) {
            std::fprintf(stderr, "spawn_process: child setup timed out\n");
        } else if (received > 0) {
            std::fprintf(stderr, "spawn_process: child setup failed: %s\n", std::strerror(child_error));
        } else {
            std::fprintf(stderr, "spawn_process: child setup handshake failed\n");
        }
        (void)terminate_process(p);
        return ProcessHandle{};
    }
#endif
    return p;
}

int wait_process(ProcessHandle& p, uint64_t timeout_ms) {
    if (!p.valid()) return kWaitFailed;
#if defined(_WIN32)
    DWORD bounded_timeout = timeout_ms > MAXDWORD ? MAXDWORD : static_cast<DWORD>(timeout_ms);
    DWORD wait = WaitForSingleObject(static_cast<HANDLE>(p.handle), bounded_timeout);
    if (wait == WAIT_TIMEOUT) return kWaitTimeout;
    if (wait != WAIT_OBJECT_0) return kWaitFailed;
    DWORD code = 0;
    bool have_code = GetExitCodeProcess(static_cast<HANDLE>(p.handle), &code) != FALSE;
    bool tree_clean = true;
    if (p.containment != nullptr && !windows_job_empty(static_cast<HANDLE>(p.containment))) {
        tree_clean = false;
        if (TerminateJobObject(static_cast<HANDLE>(p.containment), 1)) {
            auto cleanup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < cleanup_deadline) {
                if (windows_job_empty(static_cast<HANDLE>(p.containment))) {
                    tree_clean = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }
    close_windows_handle(p.handle);
    close_windows_handle(p.containment);
    p.pid = 0;
    if (!have_code || !tree_clean || code > static_cast<DWORD>(std::numeric_limits<int>::max())) {
        return kWaitFailed;
    }
    return static_cast<int>(code);
#else
    int status = 0;
    pid_t pid = process_pid(p);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    for (;;) {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            bool tree_clean = true;
            if (process_group_exists(pid)) {
                tree_clean = false;
                (void)kill(-pid, SIGKILL);
                auto cleanup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                while (std::chrono::steady_clock::now() < cleanup_deadline) {
                    if (!process_group_exists(pid)) {
                        tree_clean = true;
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
            p.handle = nullptr;
            p.pid = 0;
            if (tree_clean && WIFEXITED(status)) return WEXITSTATUS(status);
            return kWaitFailed;
        }
        if (r < 0) {
            if (errno == EINTR) continue;
            return kWaitFailed;
        }
        if (timeout_ms == 0 || std::chrono::steady_clock::now() >= deadline) return kWaitTimeout;
        auto remaining = deadline - std::chrono::steady_clock::now();
        std::this_thread::sleep_for(std::min(remaining, std::chrono::milliseconds(10)));
    }
#endif
}

bool terminate_process(ProcessHandle& p, uint64_t timeout_ms) {
    if (!p.valid()) return true;
#if defined(_WIN32)
    HANDLE process = static_cast<HANDLE>(p.handle);
    HANDLE job = static_cast<HANDLE>(p.containment);
    bool requested = job != nullptr ? TerminateJobObject(job, 1) != FALSE
                                    : TerminateProcess(process, 1) != FALSE;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    bool exited = false;
    do {
        DWORD wait = WaitForSingleObject(process, 0);
        bool leader_exited = wait == WAIT_OBJECT_0;
        bool tree_exited = job == nullptr || windows_job_empty(job);
        if (leader_exited && tree_exited) {
            exited = true;
            break;
        }
        if (timeout_ms == 0 || std::chrono::steady_clock::now() >= deadline) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (true);
    close_windows_handle(p.handle);
    close_windows_handle(p.containment);
#else
    pid_t pid = process_pid(p);
    bool requested = kill(-pid, SIGTERM) == 0 || errno == ESRCH;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms / 2);
    bool leader_reaped = false;
    int status = 0;
    do {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid || (r < 0 && errno == ECHILD)) leader_reaped = true;
        if (leader_reaped && !process_group_exists(pid)) break;
        if (timeout_ms == 0 || std::chrono::steady_clock::now() >= deadline) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (true);

    if (process_group_exists(pid)) (void)kill(-pid, SIGKILL);
    auto final_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms - timeout_ms / 2);
    do {
        if (!leader_reaped) {
            pid_t r = waitpid(pid, &status, WNOHANG);
            if (r == pid || (r < 0 && errno == ECHILD)) leader_reaped = true;
        }
        if (leader_reaped && !process_group_exists(pid)) break;
        if (timeout_ms == 0 || std::chrono::steady_clock::now() >= final_deadline) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (true);
    bool exited = leader_reaped && !process_group_exists(pid);
#endif
    p.handle = nullptr;
    p.containment = nullptr;
    p.pid = 0;
    return requested && exited;
}

}  // namespace ctf
