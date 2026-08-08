#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "cli_args.h"
#include "process.h"

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <cerrno>
#include <csignal>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace {

int failures = 0;

void check(bool condition, const char* expression) {
    if (condition) return;
    ++failures;
    std::fprintf(stderr, "FAIL: %s\n", expression);
}

ctf::Args make_args(std::initializer_list<std::string> values) {
    std::vector<std::string> storage(values);
    std::vector<char*> pointers;
    for (auto& value : storage) pointers.push_back(value.data());
    return ctf::Args(static_cast<int>(pointers.size()), pointers.data());
}

std::string encode_args(int argc, char** argv, int first) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string out;
    for (int i = first; i < argc; ++i) {
        out += std::to_string(i - first);
        out.push_back(':');
        for (const unsigned char c : std::string(argv[i])) {
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0x0f]);
        }
        out.push_back('|');
    }
    return out;
}

bool process_is_gone(uint64_t pid) {
#if defined(_WIN32)
    HANDLE process = OpenProcess(SYNCHRONIZE, FALSE, static_cast<DWORD>(pid));
    if (process == nullptr) return true;
    bool gone = WaitForSingleObject(process, 0) == WAIT_OBJECT_0;
    CloseHandle(process);
    return gone;
#else
    if (kill(static_cast<pid_t>(pid), 0) == 0) return false;
    return errno == ESRCH;
#endif
}

int run_tree_child(const std::string& pid_path) {
#if defined(_WIN32)
    std::vector<wchar_t> exe(32768);
    DWORD length = GetModuleFileNameW(nullptr, exe.data(), static_cast<DWORD>(exe.size()));
    if (length == 0 || length >= exe.size()) return 20;
    std::wstring command = L"\"" + std::wstring(exe.data(), length) + L"\" --child-grandchild";
    std::vector<wchar_t> mutable_command(command.begin(), command.end());
    mutable_command.push_back(L'\0');
    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    PROCESS_INFORMATION child{};
    if (!CreateProcessW(exe.data(), mutable_command.data(), nullptr, nullptr, FALSE, CREATE_NO_WINDOW, nullptr,
                        nullptr, &startup, &child)) {
        return 21;
    }
    CloseHandle(child.hThread);
    CloseHandle(child.hProcess);
    std::ofstream(pid_path, std::ios::trunc) << child.dwProcessId << '\n';
    for (;;) Sleep(1000);
#else
    pid_t child = fork();
    if (child < 0) return 20;
    if (child == 0) {
        for (;;) pause();
    }
    std::ofstream(pid_path, std::ios::trunc) << child << '\n';
    for (;;) pause();
#endif
}

}  // namespace

int main(int argc, char** argv) {
    if (argc >= 2 && std::string(argv[1]) == "--child-exit") {
        return argc >= 3 ? std::atoi(argv[2]) : 19;
    }
    if (argc >= 2 && std::string(argv[1]) == "--child-args") {
        std::string encoded = encode_args(argc, argv, 2);
        std::fwrite(encoded.data(), 1, encoded.size(), stdout);
        std::fflush(stdout);
        return 0;
    }
    if (argc >= 2 && std::string(argv[1]) == "--child-sleep") {
        std::this_thread::sleep_for(std::chrono::seconds(60));
        return 0;
    }
    if (argc >= 2 && std::string(argv[1]) == "--child-grandchild") {
        std::this_thread::sleep_for(std::chrono::seconds(60));
        return 0;
    }
    if (argc >= 3 && std::string(argv[1]) == "--child-tree") return run_tree_child(argv[2]);

    std::string self = ctf::current_executable_path();
    check(!self.empty(), "current executable path is discoverable");
    if (self.empty()) return 1;

    check(ctf::Args::parse_u64("18446744073709551615").has_value(), "uint64 maximum parses");
    check(!ctf::Args::parse_u64("18446744073709551616").has_value(), "uint64 overflow is rejected");
    check(ctf::Args::parse_i64("-9223372036854775808") == (std::numeric_limits<int64_t>::min)(),
          "int64 minimum parses");
    check(!ctf::Args::parse_i64("9223372036854775808").has_value(), "positive int64 overflow is rejected");
    check(!ctf::Args::parse_i64("-9223372036854775809").has_value(), "negative int64 overflow is rejected");
    {
        auto parsed = make_args({"tool", "command", "--count=4", "--flag"});
        check(parsed.next() == "command", "command token is consumed");
        check(!parsed.validate_remaining({"count"}, {"flag"}).has_value(), "valid options pass strict parsing");
        check(parsed.value("count") == "4", "equals-form option value is available");
    }
    {
        auto parsed = make_args({"tool", "command", "--count", "--flag"});
        (void)parsed.next();
        check(parsed.validate_remaining({"count"}, {"flag"}).has_value(), "missing option value is rejected");
    }
    {
        auto parsed = make_args({"tool", "command", "--count", "1", "--count=2"});
        (void)parsed.next();
        check(parsed.validate_remaining({"count"}).has_value(), "duplicate options are rejected");
    }
    {
        auto parsed = make_args({"tool", "command", "--unknown"});
        (void)parsed.next();
        check(parsed.validate_remaining({}).has_value(), "unknown options are rejected");
    }

    fs::path temp = fs::temp_directory_path() /
                    ("context-fabric-process-test-" + std::to_string(ctf::current_pid()) + "-" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::error_code ec;
    check(fs::create_directory(temp, ec), "unique process-test directory is created");
    if (ec) return 1;

    {
        auto missing = ctf::spawn_process((temp / "missing-executable").string(), {}, "", temp.string());
        check(!missing.valid(), "missing executable fails synchronously");
    }
    {
        auto bad_workdir = ctf::spawn_process(self, {"--child-exit", "0"}, "", (temp / "missing-dir").string());
        check(!bad_workdir.valid(), "missing workdir fails synchronously");
    }
    {
        auto child = ctf::spawn_process(self, {"--child-exit", "7"}, "", temp.string());
        check(child.valid(), "exit-code child spawns");
        check(ctf::wait_process(child, 5000) == 7, "exit code is preserved");
        check(!child.valid(), "completed wait releases handle");
    }
    {
        std::vector<std::string> values = {"plain", "with space", "quote\"inside", "trailing\\", ""};
        std::vector<std::string> args = {"--child-args"};
        args.insert(args.end(), values.begin(), values.end());
        fs::path log = temp / "args.log";
        auto child = ctf::spawn_process(self, args, log.string(), temp.string());
        check(child.valid(), "argument child spawns");
        check(ctf::wait_process(child, 5000) == 0, "argument child exits cleanly");
        std::ifstream input(log, std::ios::binary);
        std::string actual((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
        std::vector<std::string> expected_argv = {"ignored", "ignored"};
        expected_argv.insert(expected_argv.end(), values.begin(), values.end());
        std::vector<char*> expected_ptrs;
        for (auto& value : expected_argv) expected_ptrs.push_back(value.data());
        std::string expected = encode_args(static_cast<int>(expected_ptrs.size()), expected_ptrs.data(), 2);
        if (actual != expected) {
            std::fprintf(stderr, "argument log expected:\n%sargument log actual:\n%s", expected.c_str(),
                         actual.c_str());
        }
        check(actual == expected, "spaces, quotes, trailing slashes, and empty arguments round-trip");
    }
    {
        auto child = ctf::spawn_process(self, {"--child-sleep"}, "", temp.string());
        check(child.valid(), "sleeping child spawns");
        uint64_t pid = child.pid;
        auto start = std::chrono::steady_clock::now();
        check(ctf::wait_process(child, 0) == -1, "zero-time wait is a poll");
        check(std::chrono::steady_clock::now() - start < std::chrono::milliseconds(500),
              "zero-time wait is non-blocking");
        check(ctf::terminate_process(child, 5000), "sleeping child termination is confirmed");
        check(process_is_gone(pid), "terminated child has no live process");
    }
    {
        fs::path pid_file = temp / "grandchild.pid";
        auto child = ctf::spawn_process(self, {"--child-tree", pid_file.string()}, "", temp.string());
        check(child.valid(), "tree child spawns");
        uint64_t grandchild_pid = 0;
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (std::chrono::steady_clock::now() < deadline && grandchild_pid == 0) {
            std::ifstream input(pid_file);
            input >> grandchild_pid;
            if (grandchild_pid == 0) std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        check(grandchild_pid != 0, "grandchild pid is reported");
        check(ctf::terminate_process(child, 5000), "owned process tree termination is confirmed");
        if (grandchild_pid != 0) check(process_is_gone(grandchild_pid), "owned grandchild is not orphaned");
    }

    fs::remove_all(temp, ec);
    check(!ec, "process-test directory is removed");
    std::printf("process tests: %s\n", failures == 0 ? "PASS" : "FAIL");
    return failures == 0 ? 0 : 1;
}
