// Shared helpers for the Context Fabric examples.
#pragma once

#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "context_fabric/api/context_fabric.h"

namespace ctf {

class ExampleTempDirectory {
public:
    explicit ExampleTempDirectory(std::string_view purpose) {
        path_ = std::filesystem::temp_directory_path() /
                ("context-fabric-example-" + std::string(purpose) + "-" + ObjectId::random().to_hex());
        std::error_code ec;
        if (!std::filesystem::create_directory(path_, ec) || ec) {
            throw CfError(ErrKind::Persistence, "cannot create unique example directory: " + ec.message());
        }
    }

    ~ExampleTempDirectory() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    ExampleTempDirectory(const ExampleTempDirectory&) = delete;
    ExampleTempDirectory& operator=(const ExampleTempDirectory&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

inline bool parse_example_endpoint(const std::string& text, std::string& host, uint16_t& port) {
    size_t colon = text.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == text.size()) return false;
    std::string_view digits(text.data() + colon + 1, text.size() - colon - 1);
    uint32_t value = 0;
    auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size() || value == 0 || value > 65535) {
        return false;
    }
    host = text.substr(0, colon);
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
    }
    if (host.empty()) return false;
    port = static_cast<uint16_t>(value);
    return true;
}

namespace detail {

// Retries an operation while it fails with NotFound, which happens when
// an in-process node has not yet finished registering with the coordinator.
inline auto retry_while_registering(const std::function<void()>& op, const char* what) -> void {
    for (int i = 0; i < 200; ++i) {
        try {
            op();
            return;
        } catch (const CfError& e) {
            if (e.kind() != ErrKind::NotFound) throw;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    throw CfError(ErrKind::Timeout, std::string(what) + " did not complete in time");
}

}  // namespace detail

// Creates an object, retrying while the target node is still registering with
// the coordinator. In-process examples start a coordinator and node in the
// same process; the node registers asynchronously, so an immediate create can
// race registration and fail with NotFound ("owner node not online").
inline ObjectId create_object_wait_registered(ContextFabric& cf, const ContextMetadata& md,
                                              uint64_t owner_node, const std::vector<uint8_t>& content) {
    ObjectId id;
    detail::retry_while_registering(
        [&] { id = cf.create_object(md, owner_node, content); }, "object creation");
    return id;
}

// Restores an object, retrying while the restarting node re-registers with the
// coordinator. After a coordinator + node restart, restore can race the node's
// registration and fail with NotFound ("no live node holds a persistent copy").
inline Location restore_wait_registered(ContextFabric& cf, const ObjectId& id, uint64_t to_node) {
    Location loc;
    detail::retry_while_registering(
        [&] { loc = cf.restore(id, to_node); }, "restore");
    return loc;
}

// Replicates an object, retrying while the target node is still registering.
inline Location replicate_wait_registered(ContextFabric& cf, const ObjectId& id, uint64_t target_node) {
    Location loc;
    detail::retry_while_registering(
        [&] { loc = cf.replicate(id, target_node); }, "replication");
    return loc;
}

// Migrates an object, retrying while the target node is still registering.
inline Location migrate_wait_registered(ContextFabric& cf, const ObjectId& id, uint64_t target_node) {
    Location loc;
    detail::retry_while_registering(
        [&] { loc = cf.migrate(id, target_node); }, "migration");
    return loc;
}

}  // namespace ctf
