#include "context_fabric/authority/coordinator_store.h"

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include "context_fabric/core/error.h"

namespace ctf {

namespace fs = std::filesystem;

namespace {

constexpr uintmax_t kMaxCoordinatorStoreBytes = 1024ull * 1024 * 1024;
std::atomic<uint64_t> g_staging_sequence{1};

uint64_t process_id() noexcept {
#if defined(_WIN32)
    return static_cast<uint64_t>(GetCurrentProcessId());
#else
    return static_cast<uint64_t>(::getpid());
#endif
}

void sync_file(const fs::path& path) {
#if defined(_WIN32)
    HANDLE handle = CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                                FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        throw_error(ErrKind::Persistence, "cannot open coordinator staging file for sync", GetLastError());
    }
    const BOOL ok = FlushFileBuffers(handle);
    const DWORD error = ok ? ERROR_SUCCESS : GetLastError();
    CloseHandle(handle);
    if (!ok) throw_error(ErrKind::Persistence, "coordinator staging file sync failed", error);
#else
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) throw_error(ErrKind::Persistence, "cannot open coordinator staging file for sync", errno);
    const int rc = ::fsync(fd);
    const int saved_errno = errno;
    ::close(fd);
    if (rc != 0) throw_error(ErrKind::Persistence, "coordinator staging file sync failed", saved_errno);
#endif
}

void atomic_replace(const fs::path& source, const fs::path& destination) {
#if defined(_WIN32)
    if (!MoveFileExW(source.c_str(), destination.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        throw_error(ErrKind::Persistence, "coordinator atomic replacement failed", GetLastError());
    }
#else
    if (::rename(source.c_str(), destination.c_str()) != 0) {
        throw_error(ErrKind::Persistence, "coordinator atomic replacement failed", errno);
    }
    fs::path parent = destination.parent_path();
    if (parent.empty()) parent = ".";
    int dirfd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
    if (dirfd < 0) throw_error(ErrKind::Persistence, "cannot open coordinator store directory for sync", errno);
    const int rc = ::fsync(dirfd);
    const int saved_errno = errno;
    ::close(dirfd);
    if (rc != 0) throw_error(ErrKind::Persistence, "coordinator store directory sync failed", saved_errno);
#endif
}

}  // namespace

CoordinatorStore::CoordinatorStore(std::string path) : path_(std::move(path)) {}

bool CoordinatorStore::exists() const {
    std::error_code ec;
    const bool present = fs::exists(path_, ec);
    if (ec) throw_error(ErrKind::Persistence, "cannot inspect coordinator store", ec.value());
    return present;
}

CoordinatorSnapshot CoordinatorStore::load() {
    std::error_code ec;
    const uintmax_t size = fs::file_size(path_, ec);
    if (ec) throw_error(ErrKind::Persistence, "cannot stat coordinator store", ec.value());
    if (size == 0 || size > kMaxCoordinatorStoreBytes ||
        size > static_cast<uintmax_t>(std::numeric_limits<size_t>::max())) {
        throw_error(ErrKind::Corrupt, "coordinator store size is invalid", static_cast<uint64_t>(size));
    }
    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) throw_error(ErrKind::Persistence, "cannot open coordinator store");
    std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (!in.eof() && in.fail()) throw_error(ErrKind::Persistence, "coordinator store read failed");
    return deserialize_coordinator_snapshot(bytes.data(), bytes.size());
}

void CoordinatorStore::save(const CoordinatorSnapshot& s) {
    std::vector<uint8_t> bytes;
    serialize_coordinator_snapshot(s, bytes);
    const fs::path destination(path_);
    const fs::path tmp(path_ + ".tmp." + std::to_string(process_id()) + "." +
                       std::to_string(g_staging_sequence.fetch_add(1)));
    try {
        {
            std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
            if (!out.is_open()) throw_error(ErrKind::Persistence, "cannot open coordinator staging file");
            out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
            out.flush();
            if (!out.good()) throw_error(ErrKind::Persistence, "coordinator snapshot write failed");
            out.close();
            if (out.fail()) throw_error(ErrKind::Persistence, "coordinator staging file close failed");
        }
        sync_file(tmp);
        atomic_replace(tmp, destination);
    } catch (...) {
        std::error_code ec;
        fs::remove(tmp, ec);
        throw;
    }
}

}  // namespace ctf
