#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/core/types.h"
#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"
#include "context_fabric/core/metadata.h"
#include "context_fabric/core/lease.h"
#include "context_fabric/core/lineage.h"
#include "context_fabric/authority/client.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Public API: a clean, high-level facade over the distributed runtime.
//
// No socket handles, CUDA types, or OS-specific types leak through this API.
// Ownership is explicit: the caller owns the ContextFabric object and must
// call disconnect() (or let the destructor run) to release resources.
// ---------------------------------------------------------------------------
class ContextFabric {
public:
    ContextFabric() = default;
    ~ContextFabric() { disconnect(); }

    ContextFabric(const ContextFabric&) = delete;
    ContextFabric& operator=(const ContextFabric&) = delete;

    // Connect to the coordinator's control plane.
    bool connect(const std::string& host, uint16_t port, uint64_t timeout_ms = 10000);
    void disconnect();
    [[nodiscard]] bool connected() const noexcept { return session_.connected(); }
    [[nodiscard]] uint32_t epoch() const noexcept { return session_.coordinator_epoch(); }

    // Reference to the underlying client session (advanced use).
    [[nodiscard]] ClientSession& session() noexcept { return session_; }
    [[nodiscard]] const ClientSession& session() const noexcept { return session_; }

    // ---- object lifecycle ----
    ObjectId create_object(const ContextMetadata& metadata, uint64_t owner_node,
                           const std::vector<uint8_t>& data, const ObjectId& parent = ObjectId(),
                           const std::string& derivation_op = "create");

    // Advance an object to a new generation under a write lease.
    Generation write_generation(const ObjectId& id, const Lease& write_lease,
                                const std::vector<uint8_t>& data);

    ObjectInspectResp inspect(const ObjectId& id);
    ObjectListResp list(const std::string& namespace_filter = "");
    LineageQueryResp lineage(const ObjectId& id);
    void remove(const ObjectId& id, bool force = false);

    // ---- leases ----
    Lease acquire_read_lease(const ObjectId& id, uint64_t holder_node, uint64_t duration_ms = 0);
    Lease acquire_write_lease(const ObjectId& id, uint64_t holder_node, uint64_t duration_ms = 0);
    void release_lease(const ObjectId& id, uint64_t holder_node, uint64_t lease_id);

    // ---- movement ----
    Location replicate(const ObjectId& id, uint64_t target_node);
    Location migrate(const ObjectId& id, uint64_t target_node, StorageDomain domain = StorageDomain::HostMemory,
                     bool keep_source = false);
    uint64_t persist(const ObjectId& id, uint64_t on_node = 0);
    Location restore(const ObjectId& id, uint64_t to_node = 0);

    // ---- verification ----
    VerifyResp verify(const ObjectId& id, uint64_t on_node = 0, bool deep = false);

    // ---- planning ----
    PlacementDecision query_placement(const ContextMetadata& metadata, uint64_t requester_node,
                                      uint64_t current_node = 0);
    PlacementDecision decide_transfer_vs_recompute(const ContextMetadata& metadata, uint64_t current_node,
                                                   uint64_t target_node, double measured_bandwidth_bps);

private:
    ClientSession session_;
};

}  // namespace ctf