#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/core/types.h"
#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"
#include "context_fabric/core/node.h"
#include "context_fabric/core/metadata.h"
#include "context_fabric/core/lease.h"
#include "context_fabric/protocol/messages.h"
#include "context_fabric/transport/tcp_transport.h"

namespace ctf {

// ---------------------------------------------------------------------------
// ClientSession: control-plane client for the coordinator.
//
// Used by the CLI, the public API, benchmarks, and tests. Every call is
// request/response with a unique request id; the response is type-checked and
// its request id must match. Retries of mutating calls are idempotent at the
// coordinator (request-id cache).
// ---------------------------------------------------------------------------
class ClientSession {
public:
    ClientSession() = default;
    ~ClientSession() { close(); }

    ClientSession(const ClientSession&) = delete;
    ClientSession& operator=(const ClientSession&) = delete;

    bool connect(const std::string& host, uint16_t port, uint64_t timeout_ms = 10000);
    void close();
    [[nodiscard]] bool connected() const noexcept { return conn_ && conn_->is_open(); }

    void set_timeouts(uint64_t read_ms, uint64_t write_ms);
    [[nodiscard]] uint32_t coordinator_epoch() const noexcept { return epoch_; }
    [[nodiscard]] uint64_t next_request_id() noexcept { return ++req_seq_; }

    // Low-level: send a request and receive the matching response.
    RpcResult call(MessageType req_type, MessageType resp_type, const std::vector<uint8_t>& payload,
                   std::vector<uint8_t>& resp_payload);

    // Low-level with an explicit request id (used to test idempotent replay).
    RpcResult send_request_with_id(MessageType req_type, MessageType resp_type,
                                   const std::vector<uint8_t>& payload, std::vector<uint8_t>& resp_payload,
                                   uint64_t request_id);

    // Version negotiation.
    uint32_t hello();

    // Typed operations.
    ObjectCreateResp create_object(const ObjectId& id, const ContextMetadata& metadata, uint64_t owner_node,
                                   const ObjectId& parent, const std::string& derivation_op);
    void upload_data(const NodeAddress& addr, uint64_t op_token, const ObjectId& id, const Generation& gen,
                     const std::vector<uint8_t>& data);
    ObjectInspectResp inspect(const ObjectId& id);
    ObjectListResp list(const std::string& namespace_filter);
    LineageQueryResp lineage(const ObjectId& id);
    ObjectDeleteResp remove(const ObjectId& id, bool force);
    LeaseAcquireResp lease_acquire(const ObjectId& id, uint64_t holder_node, LeaseMode mode,
                                   uint64_t duration_ms);
    LeaseReleaseResp lease_release(const ObjectId& id, uint64_t holder_node, uint64_t lease_id);
    ObjectWriteResp write_generation(const ObjectId& id, const Generation& expected_next, uint64_t lease_id,
                                     const std::vector<uint8_t>& data);
    ReplicateResp replicate(const ObjectId& id, uint64_t target_node);
    MigrateResp migrate(const ObjectId& id, uint64_t target_node, StorageDomain domain, bool keep_source);
    PersistResp persist(const ObjectId& id, uint64_t on_node);
    RestoreResp restore(const ObjectId& id, uint64_t to_node);
    VerifyResp verify(const ObjectId& id, uint64_t on_node, bool deep);
    PlacementQueryResp placement(const ContextMetadata& metadata, uint64_t requester_node,
                                 uint64_t current_node);
    DecisionQueryResp decision(const ContextMetadata& metadata, uint64_t current_node,
                               uint64_t target_node, double bandwidth_bps);

    // Ask the coordinator to gracefully shut down a node.
    void shutdown_node(uint64_t node_id);

    // Upload chunks (the client's data-plane sender).
    void send_blocks(Socket& peer, uint64_t op_token, const ObjectId& id, const Generation& gen,
                     const std::vector<uint8_t>& data);

private:
    std::unique_ptr<TransportConnection> conn_;
    uint64_t req_seq_ = 1;
    uint64_t read_timeout_ms_ = 30000;
    uint64_t write_timeout_ms_ = 30000;
    uint32_t epoch_ = 0;
};

}  // namespace ctf