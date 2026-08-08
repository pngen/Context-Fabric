#include "context_fabric/authority/client.h"

#include <memory>

#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/core/version.h"

namespace ctf {

bool ClientSession::connect(const std::string& host, uint16_t port, uint64_t timeout_ms) {
    conn_ = std::make_unique<TransportConnection>("coordinator", Socket(), read_timeout_ms_, write_timeout_ms_);
    if (!conn_->connect(host, port, timeout_ms)) {
        conn_.reset();
        return false;
    }
    return true;
}

void ClientSession::close() {
    if (conn_) conn_->close();
    conn_.reset();
}

void ClientSession::set_timeouts(uint64_t read_ms, uint64_t write_ms) {
    read_timeout_ms_ = read_ms;
    write_timeout_ms_ = write_ms;
}

RpcResult ClientSession::call(MessageType req_type, MessageType resp_type,
                              const std::vector<uint8_t>& payload, std::vector<uint8_t>& resp_payload) {
    return send_request_with_id(req_type, resp_type, payload, resp_payload, next_request_id());
}

RpcResult ClientSession::send_request_with_id(MessageType req_type, MessageType resp_type,
                                              const std::vector<uint8_t>& payload,
                                              std::vector<uint8_t>& resp_payload, uint64_t request_id) {
    if (!conn_ || !conn_->is_open()) throw_error(ErrKind::Transport, "not connected to coordinator");
    conn_->send(req_type, request_id, 0, payload);
    FrameHeader hdr = conn_->recv(resp_payload);
    if (hdr.type != resp_type) {
        throw_error(ErrKind::Protocol, "unexpected response type");
    }
    if (hdr.request_id != request_id) {
        throw_error(ErrKind::Protocol, "response request id mismatch");
    }
    return RpcResult::success();
}

uint32_t ClientSession::hello() {
    HelloReq h;
    h.protocol_version = kProtocolVersion;
    h.client_name = "context-fabric-client";
    Writer w;
    serialize_hello(w, h);
    std::vector<uint8_t> payload;
    RpcResult r = call(kMsgHello, kMsgHelloResp, w.data(), payload);
    if (r.failed()) throw_error(r.err, r.message);
    Reader rd(payload.data(), payload.size());
    HelloResp resp = deserialize_hello_resp(rd);
    if (resp.protocol_version != kProtocolVersion) throw_error(ErrKind::Protocol, "protocol version mismatch");
    epoch_ = resp.coordinator_epoch;
    return epoch_;
}

ObjectCreateResp ClientSession::create_object(const ObjectId& id, const ContextMetadata& metadata,
                                              uint64_t owner_node, const ObjectId& parent,
                                              const std::string& derivation_op) {
    ObjectCreateReq req;
    req.metadata = metadata;
    req.object_id = id;
    req.parent = parent;
    req.owner_node = owner_node;
    req.derivation_op = derivation_op;
    Writer w;
    serialize_object_create(w, req);
    std::vector<uint8_t> payload;
    call(kMsgObjectCreate, kMsgObjectCreateResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     ObjectCreateResp resp = deserialize_object_create_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

void ClientSession::send_blocks(Socket& peer, uint64_t op_token, const ObjectId& id, const Generation& gen,
                                const std::vector<uint8_t>& data) {
    IntegrityId idg = compute_integrity(data.data(), data.size());
    const uint64_t chunk = 1024 * 1024;
    uint64_t total = (data.size() + chunk - 1) / chunk;
    for (uint64_t seq = 0; seq < total; ++seq) {
        size_t off = static_cast<size_t>(seq * chunk);
        size_t len = static_cast<size_t>(std::min<uint64_t>(chunk, data.size() - off));
        DataBlockMsg block;
        block.op_token = op_token;
        block.object_id = id;
        block.generation = gen;
        block.seq = seq;
        block.total_chunks = total;
        block.byte_size = data.size();
        block.crc32c = idg.crc32c;
        block.sha = idg.sha;
        block.data.assign(data.begin() + static_cast<std::ptrdiff_t>(off),
                          data.begin() + static_cast<std::ptrdiff_t>(off + len));
        Writer w;
        serialize_data_block(w, block);
        send_frame(peer, kMsgDataBlock, next_request_id(), 0, w.data(), write_timeout_ms_);
        std::vector<uint8_t> ack_payload;
        FrameHeader ack_hdr = recv_frame(peer, ack_payload, read_timeout_ms_);
        if (ack_hdr.type != kMsgDataBlockAck) throw_error(ErrKind::Protocol, "unexpected ack");
        Reader ar(ack_payload.data(), ack_payload.size());
        DataBlockAckMsg ack = deserialize_data_block_ack(ar);
        if (ack.result.failed()) throw CfError(ack.result.err, ack.result.message);
    }
    peer.shutdown_send();
    peer.close();
}

void ClientSession::upload_data(const NodeAddress& addr, uint64_t op_token, const ObjectId& id,
                                const Generation& gen, const std::vector<uint8_t>& data) {
    Socket peer;
    if (!peer.connect(addr.host, addr.port, 10000)) {
        throw_error(ErrKind::Transport, "cannot connect to node data plane for upload");
    }
    send_blocks(peer, op_token, id, gen, data);
}

ObjectInspectResp ClientSession::inspect(const ObjectId& id) {
    ObjectInspectReq req;
    req.object_id = id;
    Writer w;
    serialize_object_inspect(w, req);
    std::vector<uint8_t> payload;
    call(kMsgObjectInspect, kMsgObjectInspectResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     ObjectInspectResp resp = deserialize_object_inspect_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

ObjectListResp ClientSession::list(const std::string& namespace_filter) {
    ObjectListReq req;
    req.namespace_filter = namespace_filter;
    Writer w;
    serialize_object_list(w, req);
    std::vector<uint8_t> payload;
    call(kMsgObjectList, kMsgObjectListResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     ObjectListResp resp = deserialize_object_list_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

LineageQueryResp ClientSession::lineage(const ObjectId& id) {
    LineageQueryReq req;
    req.object_id = id;
    Writer w;
    serialize_lineage_query(w, req);
    std::vector<uint8_t> payload;
    call(kMsgLineageQuery, kMsgLineageQueryResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     LineageQueryResp resp = deserialize_lineage_query_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

ObjectDeleteResp ClientSession::remove(const ObjectId& id, bool force) {
    ObjectDeleteReq req;
    req.object_id = id;
    req.force = force;
    Writer w;
    serialize_object_delete(w, req);
    std::vector<uint8_t> payload;
    call(kMsgObjectDelete, kMsgObjectDeleteResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     ObjectDeleteResp resp = deserialize_object_delete_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

LeaseAcquireResp ClientSession::lease_acquire(const ObjectId& id, uint64_t holder_node, LeaseMode mode,
                                              uint64_t duration_ms) {
    LeaseAcquireReq req;
    req.object_id = id;
    req.holder_node = holder_node;
    req.mode = static_cast<uint32_t>(mode);
    req.duration_ms = duration_ms;
    Writer w;
    serialize_lease_acquire(w, req);
    std::vector<uint8_t> payload;
    call(kMsgLeaseAcquire, kMsgLeaseAcquireResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     LeaseAcquireResp resp = deserialize_lease_acquire_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

LeaseReleaseResp ClientSession::lease_release(const ObjectId& id, uint64_t holder_node, uint64_t lease_id) {
    LeaseReleaseReq req;
    req.object_id = id;
    req.holder_node = holder_node;
    req.lease_id = lease_id;
    Writer w;
    serialize_lease_release(w, req);
    std::vector<uint8_t> payload;
    call(kMsgLeaseRelease, kMsgLeaseReleaseResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     LeaseReleaseResp resp = deserialize_lease_release_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

ObjectWriteResp ClientSession::write_generation(const ObjectId& id, const Generation& expected_next,
                                                uint64_t lease_id, const std::vector<uint8_t>& data) {
    IntegrityId idg = compute_integrity(data.data(), data.size());
    ObjectWriteReq req;
    req.object_id = id;
    req.expected_next = expected_next;
    req.lease_id = lease_id;
    req.byte_size = data.size();
    req.crc32c = idg.crc32c;
    req.sha = idg.sha;
    Writer w;
    serialize_object_write(w, req);
    std::vector<uint8_t> payload;
    call(kMsgObjectWrite, kMsgObjectWriteResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     ObjectWriteResp resp = deserialize_object_write_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    upload_data(resp.data_addr, resp.op_token, id, resp.generation, data);
    return resp;
}

ReplicateResp ClientSession::replicate(const ObjectId& id, uint64_t target_node) {
    ReplicateReq req;
    req.object_id = id;
    req.target_node = target_node;
    Writer w;
    serialize_replicate(w, req);
    std::vector<uint8_t> payload;
    call(kMsgReplicate, kMsgReplicateResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     ReplicateResp resp = deserialize_replicate_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

MigrateResp ClientSession::migrate(const ObjectId& id, uint64_t target_node, StorageDomain domain,
                                   bool keep_source) {
    MigrateReq req;
    req.object_id = id;
    req.target_node = target_node;
    req.target_domain = static_cast<uint32_t>(domain);
    req.keep_source = keep_source ? 1 : 0;
    Writer w;
    serialize_migrate(w, req);
    std::vector<uint8_t> payload;
    call(kMsgMigrate, kMsgMigrateResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     MigrateResp resp = deserialize_migrate_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

PersistResp ClientSession::persist(const ObjectId& id, uint64_t on_node) {
    PersistReq req;
    req.object_id = id;
    req.on_node = on_node;
    Writer w;
    serialize_persist(w, req);
    std::vector<uint8_t> payload;
    call(kMsgPersist, kMsgPersistResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     PersistResp resp = deserialize_persist_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

RestoreResp ClientSession::restore(const ObjectId& id, uint64_t to_node) {
    RestoreReq req;
    req.object_id = id;
    req.to_node = to_node;
    Writer w;
    serialize_restore(w, req);
    std::vector<uint8_t> payload;
    call(kMsgRestore, kMsgRestoreResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     RestoreResp resp = deserialize_restore_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

VerifyResp ClientSession::verify(const ObjectId& id, uint64_t on_node, bool deep) {
    VerifyReq req;
    req.object_id = id;
    req.on_node = on_node;
    req.deep = deep ? 1 : 0;
    Writer w;
    serialize_verify(w, req);
    std::vector<uint8_t> payload;
    call(kMsgVerify, kMsgVerifyResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     VerifyResp resp = deserialize_verify_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

PlacementQueryResp ClientSession::placement(const ContextMetadata& metadata, uint64_t requester_node,
                                            uint64_t current_node) {
    PlacementQueryReq req;
    req.metadata = metadata;
    req.requester_node = requester_node;
    req.current_node = current_node;
    Writer w;
    serialize_placement_query(w, req);
    std::vector<uint8_t> payload;
    call(kMsgPlacementQuery, kMsgPlacementQueryResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
     PlacementQueryResp resp = deserialize_placement_query_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

DecisionQueryResp ClientSession::decision(const ContextMetadata& metadata, uint64_t current_node,
                                          uint64_t target_node, double bandwidth_bps) {
    DecisionQueryReq req;
    req.metadata = metadata;
    req.current_node = current_node;
    req.target_node = target_node;
    req.measured_bandwidth_bps = bandwidth_bps;
    Writer w;
    serialize_decision_query(w, req);
    std::vector<uint8_t> payload;
    RpcResult r = call(kMsgDecisionQuery, kMsgDecisionQueryResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
    DecisionQueryResp resp = deserialize_decision_query_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp;
}

void ClientSession::shutdown_node(uint64_t node_id) {
    CoordShutdownNodeReq req;
    req.node_id = node_id;
    Writer w;
    serialize_coord_shutdown_node(w, req);
    std::vector<uint8_t> payload;
    call(kMsgCoordShutdownNode, kMsgCoordShutdownNodeResp, w.data(), payload);
    Reader rd(payload.data(), payload.size());
    CoordShutdownNodeResp resp = deserialize_coord_shutdown_node_resp(rd);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
}

}  // namespace ctf