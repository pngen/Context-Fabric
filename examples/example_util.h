// Shared helpers for the Context Fabric examples.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <thread>
#include <vector>

#include "context_fabric/api/context_fabric.h"

namespace ctf {

namespace detail {

// Retries an operation while it fails with NotFound/State, which happens when
// an in-process node has not yet finished registering with the coordinator.
inline auto retry_while_registrating(const std::function<void()>& op, const char* what) -> void {
    for (int i = 0; i < 200; ++i) {
        try {
            op();
            return;
        } catch (const CfError& e) {
            if (e.kind() != ErrKind::NotFound && e.kind() != ErrKind::State) throw;
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
    detail::retry_while_registrating(
        [&] { id = cf.create_object(md, owner_node, content); }, "object creation");
    return id;
}

// Restores an object, retrying while the restarting node re-registers with the
// coordinator. After a coordinator + node restart, restore can race the node's
// registration and fail with NotFound ("no live node holds a persistent copy").
inline Location restore_wait_registered(ContextFabric& cf, const ObjectId& id, uint64_t to_node) {
    Location loc;
    detail::retry_while_registrating(
        [&] { loc = cf.restore(id, to_node); }, "restore");
    return loc;
}

// Replicates an object, retrying while the target node is still registering.
inline Location replicate_wait_registered(ContextFabric& cf, const ObjectId& id, uint64_t target_node) {
    Location loc;
    detail::retry_while_registrating(
        [&] { loc = cf.replicate(id, target_node); }, "replication");
    return loc;
}

// Migrates an object, retrying while the target node is still registering.
inline Location migrate_wait_registered(ContextFabric& cf, const ObjectId& id, uint64_t target_node) {
    Location loc;
    detail::retry_while_registrating(
        [&] { loc = cf.migrate(id, target_node); }, "migration");
    return loc;
}

}  // namespace ctf