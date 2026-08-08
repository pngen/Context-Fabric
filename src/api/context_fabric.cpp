#include "context_fabric/api/context_fabric.h"

#include <thread>
#include <chrono>

#include "context_fabric/core/error.h"

namespace ctf {

bool ContextFabric::connect(const std::string& host, uint16_t port, uint64_t timeout_ms) {
    if (!session_.connect(host, port, timeout_ms)) return false;
    session_.hello();
    return true;
}

void ContextFabric::disconnect() {
    session_.close();
}

ObjectId ContextFabric::create_object(const ContextMetadata& metadata, uint64_t owner_node,
                                      const std::vector<uint8_t>& data, const ObjectId& parent,
                                      const std::string& derivation_op) {
    ObjectCreateResp resp =
        session_.create_object(ObjectId(), metadata, owner_node, parent, derivation_op);
    session_.upload_data(resp.data_addr, resp.op_token, resp.object_id, resp.generation, data);
    // Wait for the create to commit (the coordinator commits asynchronously
    // once the node reports the verified upload).
    for (int i = 0; i < 200; ++i) {
        ObjectInspectResp ir = session_.inspect(resp.object_id);
        if (ir.has && ir.lifecycle == static_cast<uint32_t>(LifecycleState::Available)) {
            return resp.object_id;
        }
        if (ir.has && ir.lifecycle == static_cast<uint32_t>(LifecycleState::Released)) {
            throw_error(ErrKind::State, "object creation aborted");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    throw_error(ErrKind::Timeout, "object creation did not commit");
}

Generation ContextFabric::write_generation(const ObjectId& id, const Lease& write_lease,
                                           const std::vector<uint8_t>& data) {
    ObjectWriteResp resp = session_.write_generation(id, write_lease.generation.next(), write_lease.lease_id, data);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp.generation;
}

ObjectInspectResp ContextFabric::inspect(const ObjectId& id) {
    return session_.inspect(id);
}

ObjectListResp ContextFabric::list(const std::string& namespace_filter) {
    return session_.list(namespace_filter);
}

LineageQueryResp ContextFabric::lineage(const ObjectId& id) {
    return session_.lineage(id);
}

void ContextFabric::remove(const ObjectId& id, bool force) {
    ObjectDeleteResp resp = session_.remove(id, force);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
}

Lease ContextFabric::acquire_read_lease(const ObjectId& id, uint64_t holder_node, uint64_t duration_ms) {
    LeaseAcquireResp resp = session_.lease_acquire(id, holder_node, LeaseMode::Read, duration_ms);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    if (!resp.has_lease) throw_error(ErrKind::Conflict, "lease not granted");
    return resp.lease;
}

Lease ContextFabric::acquire_write_lease(const ObjectId& id, uint64_t holder_node, uint64_t duration_ms) {
    LeaseAcquireResp resp = session_.lease_acquire(id, holder_node, LeaseMode::Write, duration_ms);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    if (!resp.has_lease) throw_error(ErrKind::Conflict, "lease not granted");
    return resp.lease;
}

void ContextFabric::release_lease(const ObjectId& id, uint64_t holder_node, uint64_t lease_id) {
    LeaseReleaseResp resp = session_.lease_release(id, holder_node, lease_id);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
}

Location ContextFabric::replicate(const ObjectId& id, uint64_t target_node) {
    ReplicateResp resp = session_.replicate(id, target_node);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp.created_at;
}

Location ContextFabric::migrate(const ObjectId& id, uint64_t target_node, StorageDomain domain,
                                bool keep_source) {
    MigrateResp resp = session_.migrate(id, target_node, domain, keep_source);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp.moved_to;
}

uint64_t ContextFabric::persist(const ObjectId& id, uint64_t on_node) {
    PersistResp resp = session_.persist(id, on_node);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp.node;
}

Location ContextFabric::restore(const ObjectId& id, uint64_t to_node) {
    RestoreResp resp = session_.restore(id, to_node);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp.restored_to;
}

VerifyResp ContextFabric::verify(const ObjectId& id, uint64_t on_node, bool deep) {
    return session_.verify(id, on_node, deep);
}

PlacementDecision ContextFabric::query_placement(const ContextMetadata& metadata, uint64_t requester_node,
                                                 uint64_t current_node) {
    PlacementQueryResp resp = session_.placement(metadata, requester_node, current_node);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp.decision;
}

PlacementDecision ContextFabric::decide_transfer_vs_recompute(const ContextMetadata& metadata,
                                                              uint64_t current_node, uint64_t target_node,
                                                              double measured_bandwidth_bps) {
    DecisionQueryResp resp = session_.decision(metadata, current_node, target_node, measured_bandwidth_bps);
    if (resp.result.failed()) throw CfError(resp.result.err, resp.result.message);
    return resp.decision;
}

}  // namespace ctf