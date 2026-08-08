#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/core/error.h"
#include "context_fabric/core/serializer.h"
#include "context_fabric/core/types.h"
#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"
#include "context_fabric/core/node.h"
#include "context_fabric/core/metadata.h"
#include "context_fabric/core/lease.h"
#include "context_fabric/core/lineage.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Message types. The same framing carries control-plane (client/each node <->>
// coordinator) and data-plane (node <-> node, client -> node) traffic.
// ---------------------------------------------------------------------------
enum MType : uint16_t {
    kMsgNone = 0,

    // Version negotiation.
    kMsgHello = 1,
    kMsgHelloResp = 2,

    // Errors are carried inline in responses via RpcResult; no dedicated
    // error message type is needed.

    // Control plane: node registration / sessions.
    kMsgNodeRegister = 10,
    kMsgNodeRegisterResp = 11,
    kMsgNodeHeartbeat = 12,
    kMsgNodeHeartbeatResp = 13,
    kMsgNodeShutdown = 14,
    kMsgNodeShutdownResp = 15,

    // Control plane: objects.
    kMsgObjectCreate = 20,
    kMsgObjectCreateResp = 21,
    kMsgObjectInspect = 22,
    kMsgObjectInspectResp = 23,
    kMsgObjectList = 24,
    kMsgObjectListResp = 25,
    kMsgLineageQuery = 26,
    kMsgLineageQueryResp = 27,
    kMsgObjectDelete = 28,
    kMsgObjectDeleteResp = 29,

    // Control plane: leases.
    kMsgLeaseAcquire = 30,
    kMsgLeaseAcquireResp = 31,
    kMsgLeaseRelease = 32,
    kMsgLeaseReleaseResp = 33,

    // Control plane: operations.
    kMsgReplicate = 40,
    kMsgReplicateResp = 41,
    kMsgMigrate = 42,
    kMsgMigrateResp = 43,
    kMsgPersist = 44,
    kMsgPersistResp = 45,
    kMsgRestore = 46,
    kMsgRestoreResp = 47,
    kMsgVerify = 48,
    kMsgVerifyResp = 49,
    kMsgPlacementQuery = 50,
    kMsgPlacementQueryResp = 51,
    kMsgDecisionQuery = 52,   // transfer-vs-recompute
    kMsgDecisionQueryResp = 53,
    kMsgObjectWrite = 54,     // advance an object to a new generation
    kMsgObjectWriteResp = 55,
    kMsgCoordShutdownNode = 80,     // client -> coordinator: shut down a node
    kMsgCoordShutdownNodeResp = 81,

    // Control plane: coordinator -> node operation instructions.
    kMsgInstructSend = 60,
    kMsgInstructRecv = 61,
    kMsgDataResult = 62,
    kMsgDataResultResp = 63,
    kMsgInstructAbort = 64,
    kMsgInstructVerify = 65,
    kMsgInstructResp = 66,
    kMsgInstructPromote = 67,
    kMsgInstructDispose = 68,
    kMsgInstructShutdown = 69,

    // Data plane: block transfer.
    kMsgDataBlock = 70,
    kMsgDataBlockAck = 71,
    kMsgDataAbort = 72,
};

// ---------------------------------------------------------------------------
// RpcResult: every request/response carries a typed result.
// ---------------------------------------------------------------------------
struct RpcResult {
    bool ok = true;
    ErrKind err = ErrKind::None;
    uint64_t err_code = 0;
    std::string message;

    static RpcResult success() { return RpcResult{}; }
    static RpcResult failure(ErrKind k, const std::string& msg, uint64_t code = 0) {
        RpcResult r;
        r.ok = false;
        r.err = k;
        r.err_code = code;
        r.message = msg;
        return r;
    }
    [[nodiscard]] bool failed() const noexcept { return !ok; }
};

void serialize_result(Writer& w, const RpcResult& r);
RpcResult deserialize_result(Reader& r);

// Convenience: serialize result into a payload vector.
std::vector<uint8_t> result_payload(const RpcResult& r);

// ---------------------------------------------------------------------------
// Payload structs. Each has serialize(Writer&) and deserialize(Reader&).
// ---------------------------------------------------------------------------
struct HelloReq {
    uint32_t protocol_version = 0;
    std::string client_name;
};
struct HelloResp {
    uint32_t protocol_version = 0;
    uint32_t coordinator_epoch = 0;
    std::string coordinator_id;
};

struct NodeRegisterReq {
    uint64_t node_id = 0;
    std::string label;
    NodeAddress addr;
    uint32_t capabilities = 0;
    uint64_t max_host_bytes = 0;
    uint64_t max_persistent_bytes = 0;
    uint64_t max_accelerator_bytes = 0;
};
struct NodeRegisterResp {
    RpcResult result;
    uint64_t session_id = 0;
    uint64_t session_epoch = 0;
    uint32_t coordinator_epoch = 0;
    uint64_t heartbeat_interval_ms = 0;
};

struct NodeHeartbeatReq {
    uint64_t node_id = 0;
    uint64_t session_id = 0;
    uint64_t session_epoch = 0;
};
struct NodeHeartbeatResp {
    RpcResult result;
    uint64_t next_interval_ms = 0;
};

struct NodeShutdownReq {
    uint64_t node_id = 0;
    uint64_t session_id = 0;
};
struct NodeShutdownResp {
    RpcResult result;
};

struct ObjectCreateReq {
    ContextMetadata metadata;
    ObjectId object_id;      // zero => coordinator assigns
    ObjectId parent;         // zero => root
    uint64_t owner_node = 0;
    std::string derivation_op;  // linearization op label
};
struct ObjectCreateResp {
    RpcResult result;
    ObjectId object_id;
    Generation generation;
    NodeAddress data_addr;   // where the creator uploads content
    uint64_t op_token = 0;
    uint32_t coordinator_epoch = 0;
};

struct ObjectInspectReq {
    ObjectId object_id;
};
// Full snapshot of coordinator-side object state (see ObjectRecord).
struct ObjectInspectResp {
    RpcResult result;
    uint8_t has = 0;
    ContextMetadata metadata;
    ObjectId object_id;
    Generation generation;
    uint64_t lease_generation = 0;
    uint32_t lifecycle = 0;
    uint64_t authoritative_node = 0;
    uint32_t authoritative_domain = 0;
    uint64_t authoritative_generation = 0;
    std::vector<uint64_t> replica_nodes;
    std::vector<uint64_t> persistent_nodes;
    uint64_t owner_node = 0;
    uint64_t created_unix_ms = 0;
    std::vector<LineageEntry> lineage;
};

struct ObjectListReq {
    std::string namespace_filter;  // empty = all
};
struct ObjectListResp {
    RpcResult result;
    std::vector<ObjectId> ids;
};

struct LineageQueryReq {
    ObjectId object_id;
};
struct LineageQueryResp {
    RpcResult result;
    std::vector<LineageEntry> lineage;
};

struct ObjectDeleteReq {
    ObjectId object_id;
    bool force = false;
};
struct ObjectDeleteResp {
    RpcResult result;
};

struct LeaseAcquireReq {
    ObjectId object_id;
    uint64_t holder_node = 0;
    uint32_t mode = 0;  // LeaseMode
    uint64_t duration_ms = 0;  // 0 = default
};
struct LeaseAcquireResp {
    RpcResult result;
    uint8_t has_lease = 0;
    Lease lease;
};

struct LeaseReleaseReq {
    ObjectId object_id;
    uint64_t holder_node = 0;
    uint64_t lease_id = 0;
};
struct LeaseReleaseResp {
    RpcResult result;
};

struct ReplicateReq {
    ObjectId object_id;
    uint64_t target_node = 0;
    uint64_t repl_count = 1;
};
struct ReplicateResp {
    RpcResult result;
    Location created_at;
};

struct MigrateReq {
    ObjectId object_id;
    uint64_t target_node = 0;
    uint32_t target_domain = 0;  // default HostMemory
    bool keep_source = false;    // keep a read-only replica at the source
};
struct MigrateResp {
    RpcResult result;
    Location moved_to;
};

struct PersistReq {
    ObjectId object_id;
    uint64_t on_node = 0;  // 0 = authoritative node
};
struct PersistResp {
    RpcResult result;
    uint64_t node = 0;
};

struct RestoreReq {
    ObjectId object_id;
    uint64_t to_node = 0;  // 0 = authoritative node
};
struct RestoreResp {
    RpcResult result;
    Location restored_to;
};

struct VerifyReq {
    ObjectId object_id;
    uint64_t on_node = 0;  // 0 = authoritative node
    uint8_t deep = 0;
};
struct VerifyResp {
    RpcResult result;
    bool ok = false;
    uint64_t bytes = 0;
    uint64_t crc32c = 0;
    uint64_t sha = 0;
    uint64_t duration_ms = 0;
};

struct PlacementQueryReq {
    ContextMetadata metadata;
    uint64_t requester_node = 0;
    uint64_t current_node = 0;  // 0 = none
};
struct PlacementQueryResp {
    RpcResult result;
    PlacementDecision decision;
};

struct DecisionQueryReq {
    ContextMetadata metadata;
    uint64_t current_node = 0;
    uint64_t target_node = 0;
    double measured_bandwidth_bps = 0.0;
};
struct DecisionQueryResp {
    RpcResult result;
    PlacementDecision decision;
};

struct ObjectWriteReq {
    ObjectId object_id;
    Generation expected_next;   // must equal current generation + 1
    uint64_t lease_id = 0;      // a valid, unexpired write lease
    uint64_t byte_size = 0;
    uint64_t crc32c = 0;
    uint64_t sha = 0;
};
struct ObjectWriteResp {
    RpcResult result;
    Generation generation;
    NodeAddress data_addr;
    uint64_t op_token = 0;
};

// --- coordinator -> node operation instructions ---
struct InstructSendReq {
    uint64_t op_token = 0;
    ObjectId object_id;
    Generation generation;
    uint64_t dest_node = 0;
    NodeAddress dest_addr;
    uint64_t byte_size = 0;
    uint64_t crc32c = 0;
    uint64_t sha = 0;
    uint8_t keep_as_replica = 0;   // keep source copy as a read-only replica
    uint8_t delete_after_send = 0; // delete source copy after a successful send
};
struct InstructRecvReq {
    uint64_t op_token = 0;
    ObjectId object_id;
    Generation generation;
    uint64_t source_node = 0;   // 0 = client upload; own node = load own persistent
    NodeAddress source_addr;
    uint64_t byte_size = 0;
    uint64_t crc32c = 0;
    uint64_t sha = 0;
    uint32_t role = 0;      // CopyRole at destination
    uint32_t domain = 0;    // StorageDomain at destination
    ContextMetadata metadata;
};
struct InstructAbortReq {
    uint64_t op_token = 0;
    ObjectId object_id;
    Generation generation;
};
struct InstructVerifyReq {
    uint64_t op_token = 0;
    ObjectId object_id;
    Generation generation;
    uint64_t byte_size = 0;
    uint64_t crc32c = 0;
    uint64_t sha = 0;
    uint8_t deep = 0;
};
struct InstructPromoteReq {
    ObjectId object_id;
    Generation generation;
};
struct InstructDisposeReq {
    uint64_t op_token = 0;
    ObjectId object_id;
    Generation generation;
    uint8_t keep_as_replica = 0;  // demote to replica instead of deleting
};
struct InstructShutdownReq {
    uint64_t node_id = 0;
};
struct CoordShutdownNodeReq {
    uint64_t node_id = 0;
};
struct CoordShutdownNodeResp {
    RpcResult result;
};
struct InstructResp {
    RpcResult result;
};
struct DataResultMsg {
    uint64_t op_token = 0;
    ObjectId object_id;
    Generation generation;
    RpcResult result;
    uint64_t bytes_moved = 0;
    uint64_t duration_ms = 0;
    uint64_t crc32c = 0;
    uint64_t sha = 0;
};
struct DataResultResp {
    RpcResult result;
};

// --- data plane ---
struct DataBlockMsg {
    uint64_t op_token = 0;
    ObjectId object_id;
    Generation generation;
    uint64_t seq = 0;
    uint64_t total_chunks = 0;
    uint64_t byte_size = 0;
    uint64_t crc32c = 0;
    uint64_t sha = 0;
    std::vector<uint8_t> data;
};
struct DataBlockAckMsg {
    uint64_t op_token = 0;
    ObjectId object_id;
    Generation generation;
    uint64_t seq = 0;
    RpcResult result;
};
struct DataAbortMsg {
    uint64_t op_token = 0;
    ObjectId object_id;
    Generation generation;
    RpcResult result;
};

// ---------------------------------------------------------------------------
// Serialization.
// ---------------------------------------------------------------------------
void serialize_hello(Writer& w, const HelloReq& r);
void serialize_hello_resp(Writer& w, const HelloResp& r);
void serialize_node_register(Writer& w, const NodeRegisterReq& r);
void serialize_node_register_resp(Writer& w, const NodeRegisterResp& r);
void serialize_node_heartbeat(Writer& w, const NodeHeartbeatReq& r);
void serialize_node_heartbeat_resp(Writer& w, const NodeHeartbeatResp& r);
void serialize_node_shutdown(Writer& w, const NodeShutdownReq& r);
void serialize_node_shutdown_resp(Writer& w, const NodeShutdownResp& r);
void serialize_object_create(Writer& w, const ObjectCreateReq& r);
void serialize_object_create_resp(Writer& w, const ObjectCreateResp& r);
void serialize_object_inspect(Writer& w, const ObjectInspectReq& r);
void serialize_object_inspect_resp(Writer& w, const ObjectInspectResp& r);
void serialize_object_list(Writer& w, const ObjectListReq& r);
void serialize_object_list_resp(Writer& w, const ObjectListResp& r);
void serialize_lineage_query(Writer& w, const LineageQueryReq& r);
void serialize_lineage_query_resp(Writer& w, const LineageQueryResp& r);
void serialize_object_delete(Writer& w, const ObjectDeleteReq& r);
void serialize_object_delete_resp(Writer& w, const ObjectDeleteResp& r);
void serialize_lease_acquire(Writer& w, const LeaseAcquireReq& r);
void serialize_lease_acquire_resp(Writer& w, const LeaseAcquireResp& r);
void serialize_lease_release(Writer& w, const LeaseReleaseReq& r);
void serialize_lease_release_resp(Writer& w, const LeaseReleaseResp& r);
void serialize_replicate(Writer& w, const ReplicateReq& r);
void serialize_replicate_resp(Writer& w, const ReplicateResp& r);
void serialize_migrate(Writer& w, const MigrateReq& r);
void serialize_migrate_resp(Writer& w, const MigrateResp& r);
void serialize_persist(Writer& w, const PersistReq& r);
void serialize_persist_resp(Writer& w, const PersistResp& r);
void serialize_restore(Writer& w, const RestoreReq& r);
void serialize_restore_resp(Writer& w, const RestoreResp& r);
void serialize_verify(Writer& w, const VerifyReq& r);
void serialize_verify_resp(Writer& w, const VerifyResp& r);
void serialize_placement_query(Writer& w, const PlacementQueryReq& r);
void serialize_placement_query_resp(Writer& w, const PlacementQueryResp& r);
void serialize_decision_query(Writer& w, const DecisionQueryReq& r);
void serialize_decision_query_resp(Writer& w, const DecisionQueryResp& r);
void serialize_object_write(Writer& w, const ObjectWriteReq& r);
void serialize_object_write_resp(Writer& w, const ObjectWriteResp& r);
void serialize_instruct_send(Writer& w, const InstructSendReq& r);
void serialize_instruct_recv(Writer& w, const InstructRecvReq& r);
void serialize_instruct_abort(Writer& w, const InstructAbortReq& r);
void serialize_instruct_verify(Writer& w, const InstructVerifyReq& r);
void serialize_instruct_promote(Writer& w, const InstructPromoteReq& r);
void serialize_instruct_dispose(Writer& w, const InstructDisposeReq& r);
void serialize_instruct_shutdown(Writer& w, const InstructShutdownReq& r);
void serialize_coord_shutdown_node(Writer& w, const CoordShutdownNodeReq& r);
void serialize_coord_shutdown_node_resp(Writer& w, const CoordShutdownNodeResp& r);
void serialize_instruct_resp(Writer& w, const InstructResp& r);
void serialize_data_result(Writer& w, const DataResultMsg& r);
void serialize_data_result_resp(Writer& w, const DataResultResp& r);
void serialize_data_block(Writer& w, const DataBlockMsg& r);
void serialize_data_block_ack(Writer& w, const DataBlockAckMsg& r);
void serialize_data_abort(Writer& w, const DataAbortMsg& r);

// Deserializers follow the same names with `deserialize_` and take a Reader.
HelloReq deserialize_hello(Reader& r);
HelloResp deserialize_hello_resp(Reader& r);
NodeRegisterReq deserialize_node_register(Reader& r);
NodeRegisterResp deserialize_node_register_resp(Reader& r);
NodeHeartbeatReq deserialize_node_heartbeat(Reader& r);
NodeHeartbeatResp deserialize_node_heartbeat_resp(Reader& r);
NodeShutdownReq deserialize_node_shutdown(Reader& r);
NodeShutdownResp deserialize_node_shutdown_resp(Reader& r);
ObjectCreateReq deserialize_object_create(Reader& r);
ObjectCreateResp deserialize_object_create_resp(Reader& r);
ObjectInspectReq deserialize_object_inspect(Reader& r);
ObjectInspectResp deserialize_object_inspect_resp(Reader& r);
ObjectListReq deserialize_object_list(Reader& r);
ObjectListResp deserialize_object_list_resp(Reader& r);
LineageQueryReq deserialize_lineage_query(Reader& r);
LineageQueryResp deserialize_lineage_query_resp(Reader& r);
ObjectDeleteReq deserialize_object_delete(Reader& r);
ObjectDeleteResp deserialize_object_delete_resp(Reader& r);
LeaseAcquireReq deserialize_lease_acquire(Reader& r);
LeaseAcquireResp deserialize_lease_acquire_resp(Reader& r);
LeaseReleaseReq deserialize_lease_release(Reader& r);
LeaseReleaseResp deserialize_lease_release_resp(Reader& r);
ReplicateReq deserialize_replicate(Reader& r);
ReplicateResp deserialize_replicate_resp(Reader& r);
MigrateReq deserialize_migrate(Reader& r);
MigrateResp deserialize_migrate_resp(Reader& r);
PersistReq deserialize_persist(Reader& r);
PersistResp deserialize_persist_resp(Reader& r);
RestoreReq deserialize_restore(Reader& r);
RestoreResp deserialize_restore_resp(Reader& r);
VerifyReq deserialize_verify(Reader& r);
VerifyResp deserialize_verify_resp(Reader& r);
PlacementQueryReq deserialize_placement_query(Reader& r);
PlacementQueryResp deserialize_placement_query_resp(Reader& r);
DecisionQueryReq deserialize_decision_query(Reader& r);
DecisionQueryResp deserialize_decision_query_resp(Reader& r);
ObjectWriteReq deserialize_object_write(Reader& r);
ObjectWriteResp deserialize_object_write_resp(Reader& r);
InstructSendReq deserialize_instruct_send(Reader& r);
InstructRecvReq deserialize_instruct_recv(Reader& r);
InstructAbortReq deserialize_instruct_abort(Reader& r);
InstructVerifyReq deserialize_instruct_verify(Reader& r);
InstructPromoteReq deserialize_instruct_promote(Reader& r);
InstructDisposeReq deserialize_instruct_dispose(Reader& r);
InstructShutdownReq deserialize_instruct_shutdown(Reader& r);
CoordShutdownNodeReq deserialize_coord_shutdown_node(Reader& r);
CoordShutdownNodeResp deserialize_coord_shutdown_node_resp(Reader& r);
InstructResp deserialize_instruct_resp(Reader& r);
DataResultMsg deserialize_data_result(Reader& r);
DataResultResp deserialize_data_result_resp(Reader& r);
DataBlockMsg deserialize_data_block(Reader& r);
DataBlockAckMsg deserialize_data_block_ack(Reader& r);
DataAbortMsg deserialize_data_abort(Reader& r);

}  // namespace ctf