#include "context_fabric/protocol/messages.h"

#include "context_fabric/core/serializer.h"
#include "context_fabric/transport/frame.h"

namespace ctf {

// ---------------------------------------------------------------------------
// RpcResult
// ---------------------------------------------------------------------------
void serialize_result(Writer& w, const RpcResult& r) {
    w.u8(r.ok ? 1 : 0);
    w.u32(static_cast<uint32_t>(r.err));
    w.u64(r.err_code);
    w.str(r.message);
}

RpcResult deserialize_result(Reader& r) {
    RpcResult res;
    res.ok = r.u8() != 0;
    res.err = static_cast<ErrKind>(r.u32());
    res.err_code = r.u64();
    res.message = r.str();
    return res;
}

std::vector<uint8_t> result_payload(const RpcResult& r) {
    std::vector<uint8_t> out;
    Writer w;
    serialize_result(w, r);
    out = w.data();
    return out;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
namespace {

ObjectId read_object_id(Reader& r) {
    uint64_t hi = r.u64();
    uint64_t lo = r.u64();
    return ObjectId::from_parts(hi, lo);
}

void serialize_location(Writer& w, const Location& loc) {
    w.u64(loc.node_id);
    w.u32(static_cast<uint32_t>(loc.domain));
}
Location deserialize_location(Reader& r) {
    Location loc;
    loc.node_id = r.u64();
    loc.domain = static_cast<StorageDomain>(r.u32());
    return loc;
}
void serialize_lease(Writer& w, const Lease& l) {
    w.u64(l.object.hi());
    w.u64(l.object.lo());
    w.u64(l.generation.value());
    w.u64(l.holder.value());
    w.u32(static_cast<uint32_t>(l.mode));
    w.u64(l.coordinator_epoch);
    w.u64(l.lease_generation);
    w.u64(l.lease_id);
    w.u64(l.acquired_unix_ms);
    w.u64(l.expires_unix_ms);
}
Lease deserialize_lease(Reader& r) {
    Lease l;
    l.object = read_object_id(r);
    l.generation = Generation::from_value(r.u64());
    l.holder = NodeId(r.u64());
    l.mode = static_cast<LeaseMode>(r.u32());
    l.coordinator_epoch = r.u64();
    l.lease_generation = r.u64();
    l.lease_id = r.u64();
    l.acquired_unix_ms = r.u64();
    l.expires_unix_ms = r.u64();
    return l;
}
void serialize_lineage_vec(Writer& w, const std::vector<LineageEntry>& lineage) {
    std::vector<uint8_t> buf;
    serialize_lineage(lineage, buf);
    w.bytes(buf.data(), buf.size());
}
std::vector<LineageEntry> deserialize_lineage_vec(Reader& r) {
    std::vector<uint8_t> buf = r.bytes(r.u32());
    return deserialize_lineage(buf.data(), buf.size());
}
void serialize_placement(Writer& w, const PlacementDecision& d) {
    serialize_location(w, d.location);
    w.f64(d.score);
    w.str(d.policy);
    w.str(d.reason);
    w.f64(d.transfer.expected_seconds);
    w.f64(d.transfer.expected_bytes);
    w.f64(d.transfer.resource_cost);
    w.f64(d.recompute.expected_seconds);
    w.f64(d.recompute.resource_cost);
    w.u8(d.chose_transfer ? 1 : 0);
}
PlacementDecision deserialize_placement(Reader& r) {
    PlacementDecision d;
    d.location = deserialize_location(r);
    d.score = r.f64();
    d.policy = r.str();
    d.reason = r.str();
    d.transfer.expected_seconds = r.f64();
    d.transfer.expected_bytes = r.f64();
    d.transfer.resource_cost = r.f64();
    d.recompute.expected_seconds = r.f64();
    d.recompute.resource_cost = r.f64();
    d.chose_transfer = r.u8() != 0;
    return d;
}
void serialize_u64_vec(Writer& w, const std::vector<uint64_t>& v) {
    w.u32(static_cast<uint32_t>(v.size()));
    for (uint64_t x : v) w.u64(x);
}
std::vector<uint64_t> deserialize_u64_vec(Reader& r) {
    uint32_t n = r.u32();
    if (n > 1000000) throw_error(ErrKind::Protocol, "u64 vector count unreasonable");
    std::vector<uint64_t> v;
    v.reserve(n);
    for (uint32_t i = 0; i < n; ++i) v.push_back(r.u64());
    return v;
}
}  // namespace

// ---------------------------------------------------------------------------
// Hello
// ---------------------------------------------------------------------------
void serialize_hello(Writer& w, const HelloReq& r) {
    w.u32(r.protocol_version);
    w.str(r.client_name);
}
HelloReq deserialize_hello(Reader& r) {
    HelloReq m;
    m.protocol_version = r.u32();
    m.client_name = r.str();
    return m;
}
void serialize_hello_resp(Writer& w, const HelloResp& r) {
    w.u32(r.protocol_version);
    w.u32(r.coordinator_epoch);
    w.str(r.coordinator_id);
}
HelloResp deserialize_hello_resp(Reader& r) {
    HelloResp m;
    m.protocol_version = r.u32();
    m.coordinator_epoch = r.u32();
    m.coordinator_id = r.str();
    return m;
}

// ---------------------------------------------------------------------------
// Node registration
// ---------------------------------------------------------------------------
void serialize_node_register(Writer& w, const NodeRegisterReq& r) {
    w.u64(r.node_id);
    w.str(r.label);
    w.str(r.addr.host);
    w.u16(r.addr.port);
    w.u32(r.capabilities);
    w.u64(r.max_host_bytes);
    w.u64(r.max_persistent_bytes);
    w.u64(r.max_accelerator_bytes);
}
NodeRegisterReq deserialize_node_register(Reader& r) {
    NodeRegisterReq m;
    m.node_id = r.u64();
    m.label = r.str();
    m.addr.host = r.str();
    m.addr.port = r.u16();
    m.capabilities = r.u32();
    m.max_host_bytes = r.u64();
    m.max_persistent_bytes = r.u64();
    m.max_accelerator_bytes = r.u64();
    return m;
}
void serialize_node_register_resp(Writer& w, const NodeRegisterResp& r) {
    serialize_result(w, r.result);
    w.u64(r.session_id);
    w.u64(r.session_epoch);
    w.u32(r.coordinator_epoch);
    w.u64(r.heartbeat_interval_ms);
}
NodeRegisterResp deserialize_node_register_resp(Reader& r) {
    NodeRegisterResp m;
    m.result = deserialize_result(r);
    m.session_id = r.u64();
    m.session_epoch = r.u64();
    m.coordinator_epoch = r.u32();
    m.heartbeat_interval_ms = r.u64();
    return m;
}

void serialize_node_heartbeat(Writer& w, const NodeHeartbeatReq& r) {
    w.u64(r.node_id);
    w.u64(r.session_id);
    w.u64(r.session_epoch);
}
NodeHeartbeatReq deserialize_node_heartbeat(Reader& r) {
    NodeHeartbeatReq m;
    m.node_id = r.u64();
    m.session_id = r.u64();
    m.session_epoch = r.u64();
    return m;
}
void serialize_node_heartbeat_resp(Writer& w, const NodeHeartbeatResp& r) {
    serialize_result(w, r.result);
    w.u64(r.next_interval_ms);
}
NodeHeartbeatResp deserialize_node_heartbeat_resp(Reader& r) {
    NodeHeartbeatResp m;
    m.result = deserialize_result(r);
    m.next_interval_ms = r.u64();
    return m;
}

void serialize_node_shutdown(Writer& w, const NodeShutdownReq& r) {
    w.u64(r.node_id);
    w.u64(r.session_id);
}
NodeShutdownReq deserialize_node_shutdown(Reader& r) {
    NodeShutdownReq m;
    m.node_id = r.u64();
    m.session_id = r.u64();
    return m;
}
void serialize_node_shutdown_resp(Writer& w, const NodeShutdownResp& r) {
    serialize_result(w, r.result);
}
NodeShutdownResp deserialize_node_shutdown_resp(Reader& r) {
    NodeShutdownResp m;
    m.result = deserialize_result(r);
    return m;
}

// ---------------------------------------------------------------------------
// Object create
// ---------------------------------------------------------------------------
void serialize_object_create(Writer& w, const ObjectCreateReq& r) {
    std::vector<uint8_t> md;
    r.metadata.serialize(md);
    w.bytes(md.data(), md.size());
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.parent.hi());
    w.u64(r.parent.lo());
    w.u64(r.owner_node);
    w.str(r.derivation_op);
}
ObjectCreateReq deserialize_object_create(Reader& r) {
    ObjectCreateReq m;
    std::vector<uint8_t> md = r.bytes(r.u32());
    m.metadata.deserialize(md.data(), md.size());
    m.object_id = read_object_id(r);
    m.parent = read_object_id(r);
    m.owner_node = r.u64();
    m.derivation_op = r.str();
    return m;
}
void serialize_object_create_resp(Writer& w, const ObjectCreateResp& r) {
    serialize_result(w, r.result);
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
    w.str(r.data_addr.host);
    w.u16(r.data_addr.port);
    w.u64(r.op_token);
    w.u32(r.coordinator_epoch);
}
ObjectCreateResp deserialize_object_create_resp(Reader& r) {
    ObjectCreateResp m;
    m.result = deserialize_result(r);
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    m.data_addr.host = r.str();
    m.data_addr.port = r.u16();
    m.op_token = r.u64();
    m.coordinator_epoch = r.u32();
    return m;
}

void serialize_object_inspect(Writer& w, const ObjectInspectReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
}
ObjectInspectReq deserialize_object_inspect(Reader& r) {
    ObjectInspectReq m;
    m.object_id = read_object_id(r);
    return m;
}
void serialize_object_inspect_resp(Writer& w, const ObjectInspectResp& r) {
    serialize_result(w, r.result);
    w.u8(r.has);
    std::vector<uint8_t> md;
    r.metadata.serialize(md);
    w.bytes(md.data(), md.size());
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
    w.u64(r.lease_generation);
    w.u32(r.lifecycle);
    w.u64(r.authoritative_node);
    w.u32(r.authoritative_domain);
    w.u64(r.authoritative_generation);
    serialize_u64_vec(w, r.replica_nodes);
    serialize_u64_vec(w, r.persistent_nodes);
    w.u64(r.owner_node);
    w.u64(r.created_unix_ms);
    serialize_lineage_vec(w, r.lineage);
}
ObjectInspectResp deserialize_object_inspect_resp(Reader& r) {
    ObjectInspectResp m;
    m.result = deserialize_result(r);
    m.has = r.u8();
    std::vector<uint8_t> md = r.bytes(r.u32());
    m.metadata.deserialize(md.data(), md.size());
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    m.lease_generation = r.u64();
    m.lifecycle = r.u32();
    m.authoritative_node = r.u64();
    m.authoritative_domain = r.u32();
    m.authoritative_generation = r.u64();
    m.replica_nodes = deserialize_u64_vec(r);
    m.persistent_nodes = deserialize_u64_vec(r);
    m.owner_node = r.u64();
    m.created_unix_ms = r.u64();
    m.lineage = deserialize_lineage_vec(r);
    return m;
}

void serialize_object_list(Writer& w, const ObjectListReq& r) {
    w.str(r.namespace_filter);
}
ObjectListReq deserialize_object_list(Reader& r) {
    ObjectListReq m;
    m.namespace_filter = r.str();
    return m;
}
void serialize_object_list_resp(Writer& w, const ObjectListResp& r) {
    serialize_result(w, r.result);
    w.u32(static_cast<uint32_t>(r.ids.size()));
    for (const auto& id : r.ids) {
        w.u64(id.hi());
        w.u64(id.lo());
    }
}
ObjectListResp deserialize_object_list_resp(Reader& r) {
    ObjectListResp m;
    m.result = deserialize_result(r);
    uint32_t n = r.u32();
    if (n > 1000000) throw_error(ErrKind::Protocol, "object list count unreasonable");
    for (uint32_t i = 0; i < n; ++i) m.ids.emplace_back(read_object_id(r));
    return m;
}

void serialize_lineage_query(Writer& w, const LineageQueryReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
}
LineageQueryReq deserialize_lineage_query(Reader& r) {
    LineageQueryReq m;
    m.object_id = read_object_id(r);
    return m;
}
void serialize_lineage_query_resp(Writer& w, const LineageQueryResp& r) {
    serialize_result(w, r.result);
    serialize_lineage_vec(w, r.lineage);
}
LineageQueryResp deserialize_lineage_query_resp(Reader& r) {
    LineageQueryResp m;
    m.result = deserialize_result(r);
    m.lineage = deserialize_lineage_vec(r);
    return m;
}

void serialize_object_delete(Writer& w, const ObjectDeleteReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u8(r.force ? 1 : 0);
}
ObjectDeleteReq deserialize_object_delete(Reader& r) {
    ObjectDeleteReq m;
    m.object_id = read_object_id(r);
    m.force = r.u8() != 0;
    return m;
}
void serialize_object_delete_resp(Writer& w, const ObjectDeleteResp& r) {
    serialize_result(w, r.result);
}
ObjectDeleteResp deserialize_object_delete_resp(Reader& r) {
    ObjectDeleteResp m;
    m.result = deserialize_result(r);
    return m;
}

// ---------------------------------------------------------------------------
// Leases
// ---------------------------------------------------------------------------
void serialize_lease_acquire(Writer& w, const LeaseAcquireReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.holder_node);
    w.u32(r.mode);
    w.u64(r.duration_ms);
}
LeaseAcquireReq deserialize_lease_acquire(Reader& r) {
    LeaseAcquireReq m;
    m.object_id = read_object_id(r);
    m.holder_node = r.u64();
    m.mode = r.u32();
    m.duration_ms = r.u64();
    return m;
}
void serialize_lease_acquire_resp(Writer& w, const LeaseAcquireResp& r) {
    serialize_result(w, r.result);
    w.u8(r.has_lease);
    serialize_lease(w, r.lease);
}
LeaseAcquireResp deserialize_lease_acquire_resp(Reader& r) {
    LeaseAcquireResp m;
    m.result = deserialize_result(r);
    m.has_lease = r.u8();
    m.lease = deserialize_lease(r);
    return m;
}
void serialize_lease_release(Writer& w, const LeaseReleaseReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.holder_node);
    w.u64(r.lease_id);
}
LeaseReleaseReq deserialize_lease_release(Reader& r) {
    LeaseReleaseReq m;
    m.object_id = read_object_id(r);
    m.holder_node = r.u64();
    m.lease_id = r.u64();
    return m;
}
void serialize_lease_release_resp(Writer& w, const LeaseReleaseResp& r) {
    serialize_result(w, r.result);
}
LeaseReleaseResp deserialize_lease_release_resp(Reader& r) {
    LeaseReleaseResp m;
    m.result = deserialize_result(r);
    return m;
}

void serialize_object_write(Writer& w, const ObjectWriteReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.expected_next.value());
    w.u64(r.lease_id);
    w.u64(r.byte_size);
    w.u64(r.crc32c);
    w.u64(r.sha);
}
ObjectWriteReq deserialize_object_write(Reader& r) {
    ObjectWriteReq m;
    m.object_id = read_object_id(r);
    m.expected_next = Generation::from_value(r.u64());
    m.lease_id = r.u64();
    m.byte_size = r.u64();
    m.crc32c = r.u64();
    m.sha = r.u64();
    return m;
}
void serialize_object_write_resp(Writer& w, const ObjectWriteResp& r) {
    serialize_result(w, r.result);
    w.u64(r.generation.value());
    w.str(r.data_addr.host);
    w.u16(r.data_addr.port);
    w.u64(r.op_token);
}
ObjectWriteResp deserialize_object_write_resp(Reader& r) {
    ObjectWriteResp m;
    m.result = deserialize_result(r);
    m.generation = Generation::from_value(r.u64());
    m.data_addr.host = r.str();
    m.data_addr.port = r.u16();
    m.op_token = r.u64();
    return m;
}

// ---------------------------------------------------------------------------
// Replicate / migrate / persist / restore / verify
// ---------------------------------------------------------------------------
void serialize_replicate(Writer& w, const ReplicateReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.target_node);
    w.u64(r.repl_count);
}
ReplicateReq deserialize_replicate(Reader& r) {
    ReplicateReq m;
    m.object_id = read_object_id(r);
    m.target_node = r.u64();
    m.repl_count = r.u64();
    return m;
}
void serialize_replicate_resp(Writer& w, const ReplicateResp& r) {
    serialize_result(w, r.result);
    serialize_location(w, r.created_at);
}
ReplicateResp deserialize_replicate_resp(Reader& r) {
    ReplicateResp m;
    m.result = deserialize_result(r);
    m.created_at = deserialize_location(r);
    return m;
}

void serialize_migrate(Writer& w, const MigrateReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.target_node);
    w.u32(r.target_domain);
    w.u8(r.keep_source ? 1 : 0);
}
MigrateReq deserialize_migrate(Reader& r) {
    MigrateReq m;
    m.object_id = read_object_id(r);
    m.target_node = r.u64();
    m.target_domain = r.u32();
    m.keep_source = r.u8() != 0;
    return m;
}
void serialize_migrate_resp(Writer& w, const MigrateResp& r) {
    serialize_result(w, r.result);
    serialize_location(w, r.moved_to);
}
MigrateResp deserialize_migrate_resp(Reader& r) {
    MigrateResp m;
    m.result = deserialize_result(r);
    m.moved_to = deserialize_location(r);
    return m;
}

void serialize_persist(Writer& w, const PersistReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.on_node);
}
PersistReq deserialize_persist(Reader& r) {
    PersistReq m;
    m.object_id = read_object_id(r);
    m.on_node = r.u64();
    return m;
}
void serialize_persist_resp(Writer& w, const PersistResp& r) {
    serialize_result(w, r.result);
    w.u64(r.node);
}
PersistResp deserialize_persist_resp(Reader& r) {
    PersistResp m;
    m.result = deserialize_result(r);
    m.node = r.u64();
    return m;
}

void serialize_restore(Writer& w, const RestoreReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.to_node);
}
RestoreReq deserialize_restore(Reader& r) {
    RestoreReq m;
    m.object_id = read_object_id(r);
    m.to_node = r.u64();
    return m;
}
void serialize_restore_resp(Writer& w, const RestoreResp& r) {
    serialize_result(w, r.result);
    serialize_location(w, r.restored_to);
}
RestoreResp deserialize_restore_resp(Reader& r) {
    RestoreResp m;
    m.result = deserialize_result(r);
    m.restored_to = deserialize_location(r);
    return m;
}

void serialize_verify(Writer& w, const VerifyReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.on_node);
    w.u8(r.deep);
}
VerifyReq deserialize_verify(Reader& r) {
    VerifyReq m;
    m.object_id = read_object_id(r);
    m.on_node = r.u64();
    m.deep = r.u8();
    return m;
}
void serialize_verify_resp(Writer& w, const VerifyResp& r) {
    serialize_result(w, r.result);
    w.u8(r.ok ? 1 : 0);
    w.u64(r.bytes);
    w.u64(r.crc32c);
    w.u64(r.sha);
    w.u64(r.duration_ms);
}
VerifyResp deserialize_verify_resp(Reader& r) {
    VerifyResp m;
    m.result = deserialize_result(r);
    m.ok = r.u8() != 0;
    m.bytes = r.u64();
    m.crc32c = r.u64();
    m.sha = r.u64();
    m.duration_ms = r.u64();
    return m;
}

// ---------------------------------------------------------------------------
// Placement / decision
// ---------------------------------------------------------------------------
void serialize_placement_query(Writer& w, const PlacementQueryReq& r) {
    std::vector<uint8_t> md;
    r.metadata.serialize(md);
    w.bytes(md.data(), md.size());
    w.u64(r.requester_node);
    w.u64(r.current_node);
}
PlacementQueryReq deserialize_placement_query(Reader& r) {
    PlacementQueryReq m;
    std::vector<uint8_t> md = r.bytes(r.u32());
    m.metadata.deserialize(md.data(), md.size());
    m.requester_node = r.u64();
    m.current_node = r.u64();
    return m;
}
void serialize_placement_query_resp(Writer& w, const PlacementQueryResp& r) {
    serialize_result(w, r.result);
    serialize_placement(w, r.decision);
}
PlacementQueryResp deserialize_placement_query_resp(Reader& r) {
    PlacementQueryResp m;
    m.result = deserialize_result(r);
    m.decision = deserialize_placement(r);
    return m;
}

void serialize_decision_query(Writer& w, const DecisionQueryReq& r) {
    std::vector<uint8_t> md;
    r.metadata.serialize(md);
    w.bytes(md.data(), md.size());
    w.u64(r.current_node);
    w.u64(r.target_node);
    w.f64(r.measured_bandwidth_bps);
}
DecisionQueryReq deserialize_decision_query(Reader& r) {
    DecisionQueryReq m;
    std::vector<uint8_t> md = r.bytes(r.u32());
    m.metadata.deserialize(md.data(), md.size());
    m.current_node = r.u64();
    m.target_node = r.u64();
    m.measured_bandwidth_bps = r.f64();
    return m;
}
void serialize_decision_query_resp(Writer& w, const DecisionQueryResp& r) {
    serialize_result(w, r.result);
    serialize_placement(w, r.decision);
}
DecisionQueryResp deserialize_decision_query_resp(Reader& r) {
    DecisionQueryResp m;
    m.result = deserialize_result(r);
    m.decision = deserialize_placement(r);
    return m;
}

// ---------------------------------------------------------------------------
// Instructions + data plane
// ---------------------------------------------------------------------------
void serialize_instruct_send(Writer& w, const InstructSendReq& r) {
    w.u64(r.op_token);
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
    w.u64(r.dest_node);
    w.str(r.dest_addr.host);
    w.u16(r.dest_addr.port);
    w.u64(r.byte_size);
    w.u64(r.crc32c);
    w.u64(r.sha);
    w.u8(r.keep_as_replica);
    w.u8(r.delete_after_send);
}
InstructSendReq deserialize_instruct_send(Reader& r) {
    InstructSendReq m;
    m.op_token = r.u64();
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    m.dest_node = r.u64();
    m.dest_addr.host = r.str();
    m.dest_addr.port = r.u16();
    m.byte_size = r.u64();
    m.crc32c = r.u64();
    m.sha = r.u64();
    m.keep_as_replica = r.u8();
    m.delete_after_send = r.u8();
    return m;
}
void serialize_instruct_recv(Writer& w, const InstructRecvReq& r) {
    w.u64(r.op_token);
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
    w.u64(r.source_node);
    w.str(r.source_addr.host);
    w.u16(r.source_addr.port);
    w.u64(r.byte_size);
    w.u64(r.crc32c);
    w.u64(r.sha);
    w.u32(r.role);
    w.u32(r.domain);
    std::vector<uint8_t> md;
    r.metadata.serialize(md);
    w.bytes(md.data(), md.size());
}
InstructRecvReq deserialize_instruct_recv(Reader& r) {
    InstructRecvReq m;
    m.op_token = r.u64();
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    m.source_node = r.u64();
    m.source_addr.host = r.str();
    m.source_addr.port = r.u16();
    m.byte_size = r.u64();
    m.crc32c = r.u64();
    m.sha = r.u64();
    m.role = r.u32();
    m.domain = r.u32();
    std::vector<uint8_t> md = r.bytes(r.u32());
    m.metadata.deserialize(md.data(), md.size());
    return m;
}
void serialize_instruct_abort(Writer& w, const InstructAbortReq& r) {
    w.u64(r.op_token);
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
}
InstructAbortReq deserialize_instruct_abort(Reader& r) {
    InstructAbortReq m;
    m.op_token = r.u64();
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    return m;
}
void serialize_instruct_verify(Writer& w, const InstructVerifyReq& r) {
    w.u64(r.op_token);
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
    w.u64(r.byte_size);
    w.u64(r.crc32c);
    w.u64(r.sha);
    w.u8(r.deep);
}
InstructVerifyReq deserialize_instruct_verify(Reader& r) {
    InstructVerifyReq m;
    m.op_token = r.u64();
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    m.byte_size = r.u64();
    m.crc32c = r.u64();
    m.sha = r.u64();
    m.deep = r.u8();
    return m;
}
void serialize_instruct_promote(Writer& w, const InstructPromoteReq& r) {
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
}
InstructPromoteReq deserialize_instruct_promote(Reader& r) {
    InstructPromoteReq m;
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    return m;
}
void serialize_instruct_dispose(Writer& w, const InstructDisposeReq& r) {
    w.u64(r.op_token);
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
    w.u8(r.keep_as_replica);
}
InstructDisposeReq deserialize_instruct_dispose(Reader& r) {
    InstructDisposeReq m;
    m.op_token = r.u64();
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    m.keep_as_replica = r.u8();
    return m;
}
void serialize_instruct_shutdown(Writer& w, const InstructShutdownReq& r) {
    w.u64(r.node_id);
}
InstructShutdownReq deserialize_instruct_shutdown(Reader& r) {
    InstructShutdownReq m;
    m.node_id = r.u64();
    return m;
}
void serialize_coord_shutdown_node(Writer& w, const CoordShutdownNodeReq& r) {
    w.u64(r.node_id);
}
CoordShutdownNodeReq deserialize_coord_shutdown_node(Reader& r) {
    CoordShutdownNodeReq m;
    m.node_id = r.u64();
    return m;
}
void serialize_coord_shutdown_node_resp(Writer& w, const CoordShutdownNodeResp& r) {
    serialize_result(w, r.result);
}
CoordShutdownNodeResp deserialize_coord_shutdown_node_resp(Reader& r) {
    CoordShutdownNodeResp m;
    m.result = deserialize_result(r);
    return m;
}
void serialize_instruct_resp(Writer& w, const InstructResp& r) {
    serialize_result(w, r.result);
}
InstructResp deserialize_instruct_resp(Reader& r) {
    InstructResp m;
    m.result = deserialize_result(r);
    return m;
}
void serialize_data_result(Writer& w, const DataResultMsg& r) {
    w.u64(r.op_token);
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
    serialize_result(w, r.result);
    w.u64(r.bytes_moved);
    w.u64(r.duration_ms);
    w.u64(r.crc32c);
    w.u64(r.sha);
}
DataResultMsg deserialize_data_result(Reader& r) {
    DataResultMsg m;
    m.op_token = r.u64();
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    m.result = deserialize_result(r);
    m.bytes_moved = r.u64();
    m.duration_ms = r.u64();
    m.crc32c = r.u64();
    m.sha = r.u64();
    return m;
}
void serialize_data_result_resp(Writer& w, const DataResultResp& r) {
    serialize_result(w, r.result);
}
DataResultResp deserialize_data_result_resp(Reader& r) {
    DataResultResp m;
    m.result = deserialize_result(r);
    return m;
}

void serialize_data_block(Writer& w, const DataBlockMsg& r) {
    w.u64(r.op_token);
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
    w.u64(r.seq);
    w.u64(r.total_chunks);
    w.u64(r.byte_size);
    w.u64(r.crc32c);
    w.u64(r.sha);
    w.bytes(r.data.data(), r.data.size());
}
DataBlockMsg deserialize_data_block(Reader& r) {
    DataBlockMsg m;
    m.op_token = r.u64();
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    m.seq = r.u64();
    m.total_chunks = r.u64();
    m.byte_size = r.u64();
    m.crc32c = r.u64();
    m.sha = r.u64();
    uint32_t n = r.u32();
    if (n > kFrameMaxPayload) throw_error(ErrKind::Protocol, "data block exceeds frame cap");
    m.data = r.bytes(n);
    return m;
}
void serialize_data_block_ack(Writer& w, const DataBlockAckMsg& r) {
    w.u64(r.op_token);
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
    w.u64(r.seq);
    serialize_result(w, r.result);
}
DataBlockAckMsg deserialize_data_block_ack(Reader& r) {
    DataBlockAckMsg m;
    m.op_token = r.u64();
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    m.seq = r.u64();
    m.result = deserialize_result(r);
    return m;
}
void serialize_data_abort(Writer& w, const DataAbortMsg& r) {
    w.u64(r.op_token);
    w.u64(r.object_id.hi());
    w.u64(r.object_id.lo());
    w.u64(r.generation.value());
    serialize_result(w, r.result);
}
DataAbortMsg deserialize_data_abort(Reader& r) {
    DataAbortMsg m;
    m.op_token = r.u64();
    m.object_id = read_object_id(r);
    m.generation = Generation::from_value(r.u64());
    m.result = deserialize_result(r);
    return m;
}

}  // namespace ctf