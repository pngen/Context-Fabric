#include "context_fabric/authority/coordinator.h"

#include <algorithm>
#include <cstdio>
#include <memory>

#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/core/state_machine.h"
#include "context_fabric/core/version.h"
#include "context_fabric/placement/placement_engine.h"
#include "context_fabric/placement/cost_model.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Internal structures
// ---------------------------------------------------------------------------
struct Coordinator::Conn {
    uint64_t id = 0;
    Socket sock;
    std::mutex send_mutex;
    std::atomic<bool> closed{false};
    std::atomic<uint64_t> node_id{0};
    uint64_t session_id = 0;
    std::string peer;
};

struct Coordinator::PendingOp {
    uint64_t op_token = 0;
    uint64_t conn_id = 0;  // origin client connection (async create)
    ObjectId object_id;
    Generation generation;
    std::string kind;  // create/replicate/migrate/persist/restore/write/verify
    uint64_t target_node = 0;
    uint64_t source_node = 0;  // data-plane source (0 = client upload / self)
    StorageDomain target_domain = StorageDomain::HostMemory;
    bool keep_source = false;
    uint64_t on_node = 0;
    uint64_t created_ms = 0;
    uint64_t timeout_ms = 60000;

    uint8_t receiver_done = 0;
    uint8_t sender_done = 0;
    RpcResult receiver_result;
    RpcResult sender_result;
    uint64_t receiver_crc = 0;
    uint64_t receiver_sha = 0;
    uint64_t bytes_moved = 0;
    uint64_t duration_ms = 0;

    // verify op results
    RpcResult verify_result;
    uint64_t verify_bytes = 0;
    uint64_t verify_crc = 0;
    uint64_t verify_sha = 0;
    uint64_t verify_duration_ms = 0;

    bool done = false;
    bool committed = false;
    std::string reason;
    std::mutex cv_mutex;
    std::condition_variable cv;
};

namespace {

uint64_t make_idem_key(uint64_t conn_id, uint64_t req_id) {
    return (conn_id << 32) ^ (req_id & 0xFFFFFFFFull);
}

bool is_mutating(MessageType t) {
    switch (t) {
        case kMsgNodeRegister:
        case kMsgNodeHeartbeat:
        case kMsgNodeShutdown:
        case kMsgDataResult:
        case kMsgObjectCreate:
        case kMsgObjectWrite:
        case kMsgObjectDelete:
        case kMsgLeaseAcquire:
        case kMsgLeaseRelease:
        case kMsgReplicate:
        case kMsgMigrate:
        case kMsgPersist:
        case kMsgRestore:
        case kMsgVerify:
            return true;
        default:
            return false;
    }
}

MessageType response_type_for(MessageType req) {
    switch (req) {
        case kMsgHello: return kMsgHelloResp;
        case kMsgNodeRegister: return kMsgNodeRegisterResp;
        case kMsgNodeHeartbeat: return kMsgNodeHeartbeatResp;
        case kMsgNodeShutdown: return kMsgNodeShutdownResp;
        case kMsgDataResult: return kMsgDataResultResp;
        case kMsgObjectCreate: return kMsgObjectCreateResp;
        case kMsgObjectWrite: return kMsgObjectWriteResp;
        case kMsgObjectInspect: return kMsgObjectInspectResp;
        case kMsgObjectList: return kMsgObjectListResp;
        case kMsgLineageQuery: return kMsgLineageQueryResp;
        case kMsgObjectDelete: return kMsgObjectDeleteResp;
        case kMsgLeaseAcquire: return kMsgLeaseAcquireResp;
        case kMsgLeaseRelease: return kMsgLeaseReleaseResp;
        case kMsgReplicate: return kMsgReplicateResp;
        case kMsgMigrate: return kMsgMigrateResp;
        case kMsgPersist: return kMsgPersistResp;
        case kMsgRestore: return kMsgRestoreResp;
        case kMsgVerify: return kMsgVerifyResp;
        case kMsgPlacementQuery: return kMsgPlacementQueryResp;
        case kMsgDecisionQuery: return kMsgDecisionQueryResp;
        default: return kMsgNone;
    }
}

// Build a failure payload for a given request type.
std::vector<uint8_t> failure_payload(MessageType req, const RpcResult& r) {
    Writer w;
    switch (req) {
        case kMsgNodeRegister: {
            NodeRegisterResp m;
            m.result = r;
            serialize_node_register_resp(w, m);
            break;
        }
        case kMsgNodeHeartbeat: {
            NodeHeartbeatResp m;
            m.result = r;
            serialize_node_heartbeat_resp(w, m);
            break;
        }
        case kMsgNodeShutdown: {
            NodeShutdownResp m;
            m.result = r;
            serialize_node_shutdown_resp(w, m);
            break;
        }
        case kMsgDataResult: {
            DataResultResp m;
            m.result = r;
            serialize_data_result_resp(w, m);
            break;
        }
        case kMsgObjectCreate: {
            ObjectCreateResp m;
            m.result = r;
            serialize_object_create_resp(w, m);
            break;
        }
        case kMsgObjectWrite: {
            ObjectWriteResp m;
            m.result = r;
            serialize_object_write_resp(w, m);
            break;
        }
        case kMsgObjectInspect: {
            ObjectInspectResp m;
            m.result = r;
            m.has = 0;
            serialize_object_inspect_resp(w, m);
            break;
        }
        case kMsgObjectList: {
            ObjectListResp m;
            m.result = r;
            serialize_object_list_resp(w, m);
            break;
        }
        case kMsgLineageQuery: {
            LineageQueryResp m;
            m.result = r;
            serialize_lineage_query_resp(w, m);
            break;
        }
        case kMsgObjectDelete: {
            ObjectDeleteResp m;
            m.result = r;
            serialize_object_delete_resp(w, m);
            break;
        }
        case kMsgLeaseAcquire: {
            LeaseAcquireResp m;
            m.result = r;
            m.has_lease = 0;
            serialize_lease_acquire_resp(w, m);
            break;
        }
        case kMsgLeaseRelease: {
            LeaseReleaseResp m;
            m.result = r;
            serialize_lease_release_resp(w, m);
            break;
        }
        case kMsgReplicate: {
            ReplicateResp m;
            m.result = r;
            serialize_replicate_resp(w, m);
            break;
        }
        case kMsgMigrate: {
            MigrateResp m;
            m.result = r;
            serialize_migrate_resp(w, m);
            break;
        }
        case kMsgPersist: {
            PersistResp m;
            m.result = r;
            serialize_persist_resp(w, m);
            break;
        }
        case kMsgRestore: {
            RestoreResp m;
            m.result = r;
            serialize_restore_resp(w, m);
            break;
        }
        case kMsgVerify: {
            VerifyResp m;
            m.result = r;
            m.ok = false;
            serialize_verify_resp(w, m);
            break;
        }
        case kMsgPlacementQuery: {
            PlacementQueryResp m;
            m.result = r;
            serialize_placement_query_resp(w, m);
            break;
        }
        case kMsgDecisionQuery: {
            DecisionQueryResp m;
            m.result = r;
            serialize_decision_query_resp(w, m);
            break;
        }
        default: {
            RpcResult fail = RpcResult::failure(ErrKind::Internal, "no response type for request");
            serialize_result(w, fail);
            break;
        }
    }
    return w.data();
}

}  // namespace

Coordinator::Coordinator(Config cfg, Telemetry& telemetry)
    : cfg_(std::move(cfg)),
      telemetry_(telemetry),
      store_(CoordinatorStore(cfg_.store_path.empty() ? std::string("coordinator.store") : cfg_.store_path)) {}

Coordinator::~Coordinator() {
    shutdown();
}

uint32_t Coordinator::epoch() const noexcept {
    return epoch_;
}

size_t Coordinator::online_node_count() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    size_t n = 0;
    for (const auto& [id, node] : nodes_) {
        if (node.online) ++n;
    }
    return n;
}

void Coordinator::emit(const std::string& event, const JsonObject& doc) {
    telemetry_.emit(event, doc);
}

void Coordinator::mark_dirty() {
    dirty_ = true;
}

void Coordinator::persist_if_dirty() {
    if (!dirty_) return;
    dirty_ = false;
    CoordinatorSnapshot snap;
    snap.epoch = epoch_;
    snap.seq = seq_;
    for (const auto& [id, node] : nodes_) snap.nodes.push_back(node);
    for (const auto& [id, obj] : objects_) snap.objects.push_back(obj);
    try {
        store_.save(snap);
        last_persist_ms_ = now_unix_ms();
    } catch (const CfError& e) {
        JsonObject doc;
        doc.field("error", e.what());
        emit("coordinator.persist_failed", doc);
    }
}

void Coordinator::send_to_conn(const std::shared_ptr<Conn>& conn, MessageType type, uint64_t req_id,
                               const std::vector<uint8_t>& payload) {
    if (!conn || conn->closed.load()) return;
    {
        std::lock_guard<std::mutex> lock(conn->send_mutex);
        try {
            send_frame(conn->sock, type, req_id, 0, payload, 10000);
        } catch (const CfError&) {
            conn->closed.store(true);
            return;
        }
    }
    // Record the response so retries of the same (conn, request id) are
    // served from cache instead of being re-executed.
    if (req_id != 0) {
        std::lock_guard<std::mutex> lock(idem_mutex_);
        uint64_t key = make_idem_key(conn->id, req_id);
        if (idem_index_.find(key) == idem_index_.end()) {
            idem_cache_.push_back({key, type, payload});
            idem_index_[key] = idem_cache_.size() - 1;
            if (idem_cache_.size() > 4096) {
                uint64_t evicted_key = idem_cache_.front().key;
                idem_index_.erase(evicted_key);
                idem_cache_.pop_front();
                for (size_t i = 0; i < idem_cache_.size(); ++i) {
                    idem_index_[idem_cache_[i].key] = i;
                }
            }
        }
    }
}

void Coordinator::send_instruction(uint64_t node_id, MessageType type, const std::vector<uint8_t>& payload) {
    // Leaf lock: never acquires state_mutex_, so it is safe to call while
    // holding state_mutex_.
    std::lock_guard<std::mutex> lock(conn_mutex_);
    auto cit = node_conns_.find(node_id);
    if (cit == node_conns_.end()) {
        throw_error(ErrKind::NotFound, "instruction target node has no connection");
    }
    auto it = conns_.find(cit->second);
    if (it == conns_.end() || it->second->closed.load()) {
        throw_error(ErrKind::NotFound, "instruction target node connection closed");
    }
    auto conn = it->second;
    std::lock_guard<std::mutex> slock(conn->send_mutex);
    try {
        send_frame(conn->sock, type, 0, 0, payload, 10000);
    } catch (const CfError&) {
        conn->closed.store(true);
    }
}

bool Coordinator::node_online(uint64_t node_id) const {
    auto it = nodes_.find(node_id);
    return it != nodes_.end() && it->second.online;
}

const CoordNode* Coordinator::find_node(uint64_t node_id) const {
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) return nullptr;
    return &it->second;
}

// ---------------------------------------------------------------------------
// Threading
// ---------------------------------------------------------------------------
void Coordinator::enqueue(WorkItem&& item) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    while (queue_size_ >= cfg_.queue_capacity && running_.load()) {
        queue_cv_.wait(lock);
    }
    queue_.push_back(std::move(item));
    ++queue_size_;
    queue_cv_.notify_one();
}

Coordinator::WorkItem Coordinator::dequeue() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_cv_.wait(lock, [&] { return !queue_.empty() || !running_.load(); });
    if (queue_.empty()) {
        WorkItem item;
        item.kind = WorkItem::kShutdown;
        return item;
    }
    WorkItem item = std::move(queue_.front());
    queue_.pop_front();
    --queue_size_;
    queue_cv_.notify_one();
    return item;
}

bool Coordinator::start() {
    uint16_t port = 0;
    if (!listener_.listen(cfg_.bind_host, cfg_.control_port, port)) {
        return false;
    }
    bound_port_.store(port);
    running_.store(true);

    if (!cfg_.telemetry_path.empty()) telemetry_.open(cfg_.telemetry_path);
    telemetry_.set_coordinator_epoch(epoch_);

    // Recover from disk.
    try {
        if (store_.exists()) {
            CoordinatorSnapshot snap = store_.load();
            epoch_ = snap.epoch + 1;
            seq_ = snap.seq;
            for (auto& n : snap.nodes) {
                n.online = false;
                n.session_id = 0;
                n.session_epoch = 0;
                nodes_[n.node_id] = std::move(n);
            }
            for (auto& o : snap.objects) {
                objects_[o.id] = std::move(o);
            }
        } else {
            epoch_ = 1;
            seq_ = 1;
            CoordinatorSnapshot snap;
            snap.epoch = epoch_;
            snap.seq = seq_;
            store_.save(snap);
        }
    } catch (const CfError& e) {
        fprintf(stderr, "coordinator: failure loading durable state: %s\n", e.what());
        running_.store(false);
        listener_.close();
        return false;
    }
    telemetry_.set_coordinator_epoch(epoch_);

    reconcile_at_startup();

    JsonObject doc;
    doc.field("port", static_cast<uint64_t>(bound_port_.load()));
    doc.field("epoch", static_cast<uint64_t>(epoch_));
    emit("coordinator.start", doc);

    accept_thread_ = std::thread([this] { accept_loop(); });
    sweep_thread_ = std::thread([this] { sweep_loop(); });
    for (size_t i = 0; i < cfg_.driver_threads; ++i) {
        driver_threads_.emplace_back([this] { driver_loop(); });
    }
    return true;
}

void Coordinator::run() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_cv_.wait(lock, [&] { return !running_.load(); });
}

void Coordinator::request_shutdown() {
    running_.store(false);
    queue_cv_.notify_all();
    listener_.shutdown_both();
}

void Coordinator::shutdown() {
    request_shutdown();
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    for (auto& t : driver_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        for (auto& [id, conn] : conns_) {
            conn->sock.shutdown_both();
            conn->sock.close();
            conn->closed.store(true);
        }
    }
    for (auto& t : reader_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    if (sweep_thread_.joinable()) {
        sweep_thread_.join();
    }
    persist_if_dirty();
    telemetry_.close();
}

void Coordinator::accept_loop() {
    while (running_.load()) {
        Socket s = listener_.accept(200);
        if (!s.valid()) continue;
                {
            std::lock_guard<std::mutex> lock(conn_mutex_);
            if (conns_.size() >= cfg_.max_connections) {
                s.close();
                continue;
            }
            auto conn = std::make_shared<Conn>();
            conn->id = conn_next_id_++;
            conn->sock = std::move(s);
            conn->peer = conn->sock.peer_address();
            conns_[conn->id] = conn;
            reader_threads_.emplace_back([this, conn] { reader_loop(conn); });
        }
    }
}

void Coordinator::reader_loop(std::shared_ptr<Conn> conn) {
    while (running_.load() && !conn->closed.load()) {
        std::vector<uint8_t> payload;
        FrameHeader hdr;
        try {
            hdr = recv_frame(conn->sock, payload, 30000);
        } catch (const CfError&) {
            break;
        }
        WorkItem item;
        item.kind = WorkItem::kRequest;
        item.conn_id = conn->id;
        item.hdr = hdr;
        item.payload = std::move(payload);
        enqueue(std::move(item));
    }
    conn->closed.store(true);
    WorkItem item;
    item.kind = WorkItem::kClose;
    item.conn_id = conn->id;
    enqueue(std::move(item));
}

void Coordinator::driver_loop() {
    while (running_.load()) {
        WorkItem item = dequeue();
        if (item.kind == WorkItem::kShutdown) break;
        if (item.kind == WorkItem::kClose) {
            try {
                handle_close(item.conn_id);
            } catch (const std::exception& e) {
                JsonObject doc;
                doc.field("error", e.what());
                emit("coordinator.close_error", doc);
            }
            continue;
        }
        if (item.hdr.type == kMsgNone) continue;

        std::shared_ptr<Conn> conn;
        {
            std::lock_guard<std::mutex> lock(conn_mutex_);
            auto it = conns_.find(item.conn_id);
            if (it == conns_.end()) continue;
            conn = it->second;
        }
        if (conn->closed.load()) continue;

        // Idempotency for mutating requests with a request_id.
        if (is_mutating(item.hdr.type) && item.hdr.request_id != 0) {
            uint64_t key = make_idem_key(item.conn_id, item.hdr.request_id);
            MessageType cached_type = kMsgNone;
            std::vector<uint8_t> cached_payload;
            {
                std::lock_guard<std::mutex> lock(idem_mutex_);
                auto idx = idem_index_.find(key);
                if (idx != idem_index_.end()) {
                    cached_type = idem_cache_[idx->second].type;
                    cached_payload = idem_cache_[idx->second].payload;
                }
            }
            if (cached_type != kMsgNone) {
                send_to_conn(conn, cached_type, item.hdr.request_id, cached_payload);
                continue;
            }
        }

        try {
            handle_request(conn, item.hdr, item.payload);
        } catch (const CfError& e) {
                        MessageType rt = response_type_for(item.hdr.type);
            RpcResult fail = RpcResult::failure(e.kind(), e.what(), e.code());
            if (rt != kMsgNone) {
                send_to_conn(conn, rt, item.hdr.request_id, failure_payload(item.hdr.type, fail));
            }
        } catch (const std::exception& e) {
            MessageType rt = response_type_for(item.hdr.type);
            RpcResult fail = RpcResult::failure(ErrKind::Internal, e.what());
            if (rt != kMsgNone) {
                send_to_conn(conn, rt, item.hdr.request_id, failure_payload(item.hdr.type, fail));
            }
        }
    }
}

void Coordinator::handle_close(uint64_t conn_id) {
    uint64_t node_id = 0;
    {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        auto it = conns_.find(conn_id);
        if (it == conns_.end()) return;
        node_id = it->second->node_id;
        conns_.erase(it);
    }
    if (node_id != 0) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        node_disconnected(node_id);
    }
}

void Coordinator::handle_request(const std::shared_ptr<Conn>& conn, const FrameHeader& hdr,
                                 const std::vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    switch (hdr.type) {
        case kMsgHello: return handle_hello(conn, hdr.request_id, r);
        case kMsgNodeRegister: return handle_register(conn, hdr.request_id, r);
        case kMsgNodeHeartbeat: return handle_heartbeat(conn, hdr.request_id, r);
        case kMsgNodeShutdown: return handle_node_shutdown(conn, hdr.request_id, r);
        case kMsgDataResult: return handle_data_result(conn, hdr.request_id, r);
        case kMsgObjectCreate: return handle_object_create(conn, hdr.request_id, r);
        case kMsgObjectWrite: return handle_object_write(conn, hdr.request_id, r);
        case kMsgObjectInspect: return handle_object_inspect(conn, hdr.request_id, r);
        case kMsgObjectList: return handle_object_list(conn, hdr.request_id, r);
        case kMsgLineageQuery: return handle_lineage(conn, hdr.request_id, r);
        case kMsgObjectDelete: return handle_object_delete(conn, hdr.request_id, r);
        case kMsgLeaseAcquire: return handle_lease_acquire(conn, hdr.request_id, r);
        case kMsgLeaseRelease: return handle_lease_release(conn, hdr.request_id, r);
        case kMsgReplicate: return handle_replicate(conn, hdr.request_id, r);
        case kMsgMigrate: return handle_migrate(conn, hdr.request_id, r);
        case kMsgPersist: return handle_persist(conn, hdr.request_id, r);
        case kMsgRestore: return handle_restore(conn, hdr.request_id, r);
        case kMsgVerify: return handle_verify(conn, hdr.request_id, r);
        case kMsgPlacementQuery: return handle_placement_query(conn, hdr.request_id, r);
        case kMsgDecisionQuery: return handle_decision_query(conn, hdr.request_id, r);
        case kMsgCoordShutdownNode: return handle_coord_shutdown_node(conn, hdr.request_id, r);
        default:
            throw_error(ErrKind::Protocol, "unknown message type");
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------
void Coordinator::handle_hello(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    HelloReq req = deserialize_hello(r);
        if (req.protocol_version != kProtocolVersion) {
        throw_error(ErrKind::Protocol, "protocol version mismatch");
    }
    HelloResp resp;
    resp.protocol_version = kProtocolVersion;
    resp.coordinator_epoch = epoch_;
    resp.coordinator_id = "context-fabric-coordinator";
    Writer w;
    serialize_hello_resp(w, resp);
        send_to_conn(conn, kMsgHelloResp, req_id, w.data());
    }

void Coordinator::handle_register(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    NodeRegisterReq req = deserialize_node_register(r);
    if (req.node_id == 0) throw_error(ErrKind::InvalidArgument, "node id must be nonzero");

    uint64_t session_id = 0;
    uint64_t session_epoch = 0;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto it = nodes_.find(req.node_id);
        if (it != nodes_.end()) {
            it->second.label = req.label;
            it->second.addr = req.addr;
            it->second.caps = capabilities_from_bits(req.capabilities);
            it->second.caps.max_host_bytes = req.max_host_bytes;
            it->second.caps.max_persistent_bytes = req.max_persistent_bytes;
            it->second.caps.max_accelerator_bytes = req.max_accelerator_bytes;
            it->second.session_id = ++seq_;
            it->second.session_epoch += 1;
            it->second.last_heartbeat_ms = now_unix_ms();
            it->second.online = true;
        } else {
            CoordNode n;
            n.node_id = req.node_id;
            n.label = req.label;
            n.addr = req.addr;
            n.caps = capabilities_from_bits(req.capabilities);
            n.caps.max_host_bytes = req.max_host_bytes;
            n.caps.max_persistent_bytes = req.max_persistent_bytes;
            n.caps.max_accelerator_bytes = req.max_accelerator_bytes;
            n.session_id = ++seq_;
            n.session_epoch = 1;
            n.last_heartbeat_ms = now_unix_ms();
            n.online = true;
            nodes_[req.node_id] = std::move(n);
        }
        session_id = nodes_[req.node_id].session_id;
        session_epoch = nodes_[req.node_id].session_epoch;
        mark_dirty();
        persist_if_dirty();

        // A node returning to the fabric restores authority for objects it
        // still holds (they were marked Recovering during the coordinator
        // restart because the node was not yet registered).
        for (auto& [oid, o] : objects_) {
            if (o.lifecycle == LifecycleState::Recovering && o.authoritative_node == req.node_id) {
                o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Available);
                JsonObject d;
                d.field("id", oid.to_hex());
                d.field("node", req.node_id);
                emit("recovery.authority_restored", d);
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        conn->node_id = req.node_id;
        conn->session_id = session_id;
        node_conns_[req.node_id] = conn->id;
    }

    JsonObject doc;
    doc.field("label", req.label.empty() ? std::to_string(req.node_id) : req.label);
    doc.field("addr", req.addr.to_string());
    emit("node.register", doc);

    NodeRegisterResp resp;
    resp.result = RpcResult::success();
    resp.session_id = session_id;
    resp.session_epoch = session_epoch;
    resp.coordinator_epoch = epoch_;
    resp.heartbeat_interval_ms = cfg_.heartbeat_interval_ms;
    Writer w;
    serialize_node_register_resp(w, resp);
    send_to_conn(conn, kMsgNodeRegisterResp, req_id, w.data());
}

void Coordinator::handle_heartbeat(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    NodeHeartbeatReq req = deserialize_node_heartbeat(r);
    std::lock_guard<std::mutex> lock(state_mutex_);
    NodeHeartbeatResp resp;
    auto nit = nodes_.find(req.node_id);
    if (nit == nodes_.end() || !nit->second.online || nit->second.session_id != req.session_id ||
        nit->second.session_epoch != req.session_epoch) {
        resp.result = RpcResult::failure(ErrKind::StaleLease, "session not valid");
    } else {
        nit->second.last_heartbeat_ms = now_unix_ms();
        resp.result = RpcResult::success();
        resp.next_interval_ms = cfg_.heartbeat_interval_ms;
    }
    Writer w;
    serialize_node_heartbeat_resp(w, resp);
    send_to_conn(conn, kMsgNodeHeartbeatResp, req_id, w.data());
}

void Coordinator::handle_node_shutdown(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    NodeShutdownReq req = deserialize_node_shutdown(r);
    std::lock_guard<std::mutex> lock(state_mutex_);
    NodeShutdownResp resp;
    auto nit = nodes_.find(req.node_id);
    if (nit == nodes_.end()) {
        resp.result = RpcResult::failure(ErrKind::NotFound, "node not registered");
    } else {
        resp.result = RpcResult::success();
        node_disconnected(req.node_id);
    }
    Writer w;
    serialize_node_shutdown_resp(w, resp);
    send_to_conn(conn, kMsgNodeShutdownResp, req_id, w.data());
}

void Coordinator::handle_object_inspect(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    ObjectInspectReq req = deserialize_object_inspect(r);
    std::lock_guard<std::mutex> lock(state_mutex_);
    ObjectInspectResp resp;
    resp.result = RpcResult::success();
    auto it = objects_.find(req.object_id);
    if (it == objects_.end()) {
        resp.has = 0;
    } else {
        const CoordObject& o = it->second;
        resp.has = 1;
        resp.metadata = o.metadata;
        resp.object_id = o.id;
        resp.generation = o.generation;
        resp.lease_generation = o.lease_generation;
        resp.lifecycle = static_cast<uint32_t>(o.lifecycle);
        resp.authoritative_node = o.authoritative_node;
        resp.authoritative_domain = static_cast<uint32_t>(o.authoritative_domain);
        resp.authoritative_generation = o.generation.value();
        resp.replica_nodes = o.replicas;
        resp.persistent_nodes = o.persistent_nodes;
        resp.owner_node = o.owner_node;
        resp.created_unix_ms = o.created_unix_ms;
        resp.lineage = o.lineage;
    }
    Writer w;
    serialize_object_inspect_resp(w, resp);
    send_to_conn(conn, kMsgObjectInspectResp, req_id, w.data());
}

void Coordinator::handle_object_list(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    ObjectListReq req = deserialize_object_list(r);
    std::lock_guard<std::mutex> lock(state_mutex_);
    ObjectListResp resp;
    resp.result = RpcResult::success();
    for (const auto& [id, obj] : objects_) {
        if (!req.namespace_filter.empty() && obj.metadata.namespace_id != req.namespace_filter) continue;
        resp.ids.push_back(id);
    }
    Writer w;
    serialize_object_list_resp(w, resp);
    send_to_conn(conn, kMsgObjectListResp, req_id, w.data());
}

void Coordinator::handle_lineage(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    LineageQueryReq req = deserialize_lineage_query(r);
    std::lock_guard<std::mutex> lock(state_mutex_);
    LineageQueryResp resp;
    resp.result = RpcResult::success();
    auto it = objects_.find(req.object_id);
    if (it == objects_.end()) {
        throw_error(ErrKind::NotFound, "object not found");
    }
    resp.lineage = it->second.lineage;
    Writer w;
    serialize_lineage_query_resp(w, resp);
    send_to_conn(conn, kMsgLineageQueryResp, req_id, w.data());
}

void Coordinator::handle_object_delete(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    ObjectDeleteReq req = deserialize_object_delete(r);
    std::lock_guard<std::mutex> lock(state_mutex_);
    ObjectDeleteResp resp;
    auto it = objects_.find(req.object_id);
    if (it == objects_.end()) {
        throw_error(ErrKind::NotFound, "object not found");
    }
    CoordObject& o = it->second;
    if (o.in_flight()) {
        throw_error(ErrKind::Conflict, "object has an in-flight operation");
    }
    bool has_write = write_leases_.count(req.object_id) != 0;
    bool has_read = read_leases_.count(req.object_id) != 0;
    if ((has_write || has_read) && !req.force) {
        throw_error(ErrKind::Conflict, "object has active leases; use --force");
    }
    write_leases_.erase(req.object_id);
    read_leases_.erase(req.object_id);
    o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Released);
    objects_.erase(it);
    mark_dirty();
    persist_if_dirty();

    JsonObject doc;
    doc.field("id", req.object_id.to_hex());
    emit("object.released", doc);

    resp.result = RpcResult::success();
    Writer w;
    serialize_object_delete_resp(w, resp);
    send_to_conn(conn, kMsgObjectDeleteResp, req_id, w.data());
}

// ---------------------------------------------------------------------------
// Leases
// ---------------------------------------------------------------------------
void Coordinator::handle_lease_acquire(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    LeaseAcquireReq req = deserialize_lease_acquire(r);
    std::lock_guard<std::mutex> lock(state_mutex_);
    Lease lease;

    auto oit = objects_.find(req.object_id);
    if (oit == objects_.end()) throw_error(ErrKind::NotFound, "object not found");
    CoordObject& o = oit->second;
    if (o.lifecycle == LifecycleState::Released || o.lifecycle == LifecycleState::Corrupt) {
        throw_error(ErrKind::State, "object cannot be leased in its current state");
    }
    if (o.in_flight()) {
        throw_error(ErrKind::Conflict, "object has an in-flight operation");
    }
    if (!node_online(req.holder_node)) {
        throw_error(ErrKind::NotFound, "lease holder node is not online");
    }

    lease.object = req.object_id;
    lease.generation = o.generation;
    lease.holder = NodeId(req.holder_node);
    lease.mode = static_cast<LeaseMode>(req.mode);
    lease.coordinator_epoch = epoch_;
    lease.lease_generation = o.lease_generation;
    lease.lease_id = ++seq_;
    lease.acquired_unix_ms = now_unix_ms();
    uint64_t duration = req.duration_ms != 0 ? req.duration_ms : cfg_.lease_default_ms;
    lease.expires_unix_ms = lease.acquired_unix_ms + duration;

    if (lease.mode == LeaseMode::Write) {
        if (write_leases_.count(req.object_id)) {
            throw_error(ErrKind::Conflict, "write lease already held");
        }
        auto rit = read_leases_.find(req.object_id);
        if (rit != read_leases_.end() && !rit->second.empty()) {
            throw_error(ErrKind::Conflict, "cannot acquire write lease while read leases are active");
        }
        write_leases_[req.object_id] = lease;
        o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::LeasedWrite);
    } else if (lease.mode == LeaseMode::Read) {
        if (write_leases_.count(req.object_id)) {
            throw_error(ErrKind::Conflict, "cannot acquire read lease while write lease active");
        }
        read_leases_[req.object_id].push_back(lease);
        o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::LeasedRead);
    } else {
        throw_error(ErrKind::InvalidArgument, "invalid lease mode");
    }

    JsonObject doc;
    doc.field("id", req.object_id.to_hex());
    doc.field("mode", lease_mode_name(lease.mode));
    doc.field("holder", req.holder_node);
    doc.field("lease_id", lease.lease_id);
    emit("lease.acquire", doc);

    LeaseAcquireResp resp;
    resp.result = RpcResult::success();
    resp.has_lease = 1;
    resp.lease = lease;
    Writer w;
    serialize_lease_acquire_resp(w, resp);
    send_to_conn(conn, kMsgLeaseAcquireResp, req_id, w.data());
}

void Coordinator::handle_lease_release(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    LeaseReleaseReq req = deserialize_lease_release(r);
    std::lock_guard<std::mutex> lock(state_mutex_);
    LeaseReleaseResp resp;
    resp.result = RpcResult::success();

    auto oit = objects_.find(req.object_id);
    bool exists = oit != objects_.end();

    bool released = false;
    auto wit = write_leases_.find(req.object_id);
    if (wit != write_leases_.end() && wit->second.lease_id == req.lease_id &&
        wit->second.holder.value() == req.holder_node) {
        write_leases_.erase(wit);
        released = true;
    }
    if (!released) {
        auto& rls = read_leases_[req.object_id];
        for (auto it = rls.begin(); it != rls.end(); ++it) {
            if (it->lease_id == req.lease_id && it->holder.value() == req.holder_node) {
                rls.erase(it);
                released = true;
                break;
            }
        }
        if (rls.empty()) read_leases_.erase(req.object_id);
    }

    if (!released) {
        throw_error(ErrKind::NotFound, "lease not found");
    }
    if (exists) {
        CoordObject& o = oit->second;
        bool write_held = write_leases_.count(req.object_id) != 0;
        bool read_held = read_leases_.count(req.object_id) != 0;
        LifecycleState target;
        if (write_held) target = LifecycleState::LeasedWrite;
        else if (read_held) target = LifecycleState::LeasedRead;
        else target = LifecycleState::Available;
        if (o.lifecycle == LifecycleState::LeasedWrite || o.lifecycle == LifecycleState::LeasedRead) {
            if (target != o.lifecycle) {
                o.lifecycle = LifecycleAuthority::transition(o.lifecycle, target);
            }
        }
    }

    Writer w;
    serialize_lease_release_resp(w, resp);
    send_to_conn(conn, kMsgLeaseReleaseResp, req_id, w.data());
}

// ---------------------------------------------------------------------------
// Object create / write
// ---------------------------------------------------------------------------
void Coordinator::handle_object_create(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    ObjectCreateReq req = deserialize_object_create(r);
    if (req.metadata.byte_size == 0) throw_error(ErrKind::InvalidArgument, "object byte size must be nonzero");
    if (req.owner_node == 0) throw_error(ErrKind::InvalidArgument, "owner node required");

    ObjectId id;
    ContextMetadata meta;
    NodeAddress owner_addr;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (!node_online(req.owner_node)) throw_error(ErrKind::NotFound, "owner node not online");
        const CoordNode* node = find_node(req.owner_node);
        if (!node->caps.host_memory) throw_error(ErrKind::NotSupported, "owner node lacks host memory");
        if (node->caps.max_host_bytes != 0 && req.metadata.byte_size > node->caps.max_host_bytes) {
            throw_error(ErrKind::Capacity, "object exceeds owner host capacity");
        }
        if (req.object_id.valid()) {
            if (objects_.count(req.object_id)) {
                throw_error(ErrKind::AlreadyExists, "object id already exists");
            }
            id = req.object_id;
        } else {
            id = ObjectId::deterministic(req.metadata.namespace_id, "obj" + std::to_string(seq_));
        }
        meta = req.metadata;
        if (meta.compatibility_fingerprint == 0) {
            meta.compatibility_fingerprint = meta.compute_compatibility_fingerprint();
        }
        owner_addr = node->addr;

        CoordObject obj;
        obj.metadata = meta;
        obj.id = id;
        obj.generation = Generation::first();
        obj.lease_generation = 1;
        obj.lifecycle = LifecycleState::Creating;
        obj.authoritative_node = req.owner_node;
        obj.authoritative_domain = StorageDomain::HostMemory;
        obj.owner_node = req.owner_node;
        obj.created_unix_ms = now_unix_ms();
        if (meta.lifetime_seconds.has_value()) {
            obj.expires_unix_ms = obj.created_unix_ms + *meta.lifetime_seconds * 1000;
        }
        LineageEntry entry;
        entry.object = id;
        entry.generation = obj.generation;
        entry.parent = req.parent;
        entry.operation = req.derivation_op.empty() ? "create" : req.derivation_op;
        entry.producer = node->label.empty() ? std::to_string(req.owner_node) : node->label;
        entry.creation_sequence = ++seq_;
        entry.created_unix_ms = obj.created_unix_ms;
        obj.lineage.push_back(entry);
        objects_[id] = std::move(obj);
        mark_dirty();
        persist_if_dirty();
    }

    uint64_t op_token = ++seq_;  // seqlock: seq_ is monotonic; creating a token
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto op = std::make_shared<PendingOp>();
        op->op_token = op_token;
        op->conn_id = conn->id;
        op->object_id = id;
        op->generation = Generation::first();
        op->kind = "create";
        op->target_node = req.owner_node;
        op->timeout_ms = cfg_.create_timeout_ms;
        op->created_ms = now_unix_ms();
        pending_ops_[op_token] = op;
        objects_[id].active_op_token = op_token;
        objects_[id].active_op_kind = "create";
        objects_[id].active_op_target = req.owner_node;
        objects_[id].active_op_domain = StorageDomain::HostMemory;
    }

    InstructRecvReq ir;
    ir.op_token = op_token;
    ir.object_id = id;
    ir.generation = Generation::first();
    ir.source_node = 0;  // client upload
    ir.byte_size = meta.byte_size;
    ir.role = static_cast<uint32_t>(CopyRole::Authoritative);
    ir.domain = static_cast<uint32_t>(StorageDomain::HostMemory);
    ir.metadata = meta;
    Writer w;
    serialize_instruct_recv(w, ir);
    send_instruction(req.owner_node, kMsgInstructRecv, w.data());

    JsonObject doc;
    doc.field("id", id.to_hex());
    doc.field("owner", req.owner_node);
    doc.field("bytes", meta.byte_size);
    emit("object.create_requested", doc);

    ObjectCreateResp resp;
    resp.result = RpcResult::success();
    resp.object_id = id;
    resp.generation = Generation::first();
    resp.data_addr = owner_addr;
    resp.op_token = op_token;
    resp.coordinator_epoch = epoch_;
    Writer w2;
    serialize_object_create_resp(w2, resp);
    send_to_conn(conn, kMsgObjectCreateResp, req_id, w2.data());
}

void Coordinator::handle_object_write(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    ObjectWriteReq req = deserialize_object_write(r);
    if (req.byte_size == 0) throw_error(ErrKind::InvalidArgument, "byte size must be nonzero");

    uint64_t op_token = 0;
    Generation new_gen;
    NodeAddress data_addr;
    uint64_t auth_node = 0;
    ContextMetadata meta;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto oit = objects_.find(req.object_id);
        if (oit == objects_.end()) throw_error(ErrKind::NotFound, "object not found");
        CoordObject& o = oit->second;
        if (o.in_flight()) throw_error(ErrKind::Conflict, "object has an in-flight operation");

        auto lit = write_leases_.find(req.object_id);
        if (lit == write_leases_.end()) throw_error(ErrKind::StaleLease, "no write lease held");
        Lease& lease = lit->second;
        if (lease.lease_id != req.lease_id) throw_error(ErrKind::StaleLease, "write lease id mismatch");
        if (lease.coordinator_epoch != epoch_) throw_error(ErrKind::StaleLease, "write lease epoch stale");
        if (lease.lease_generation != o.lease_generation) throw_error(ErrKind::StaleLease, "write lease generation stale");
        if (lease.expired(now_unix_ms())) throw_error(ErrKind::StaleLease, "write lease expired");
        if (!node_online(lease.holder.value())) throw_error(ErrKind::StaleLease, "lease holder offline");

        if (req.expected_next.value() != o.generation.next().value()) {
            throw_error(ErrKind::Conflict, "generation does not match expected next");
        }
        if (o.authoritative_node == 0 || !node_online(o.authoritative_node)) {
            throw_error(ErrKind::State, "authoritative node not available");
        }

        new_gen = o.generation.next();
        o.generation = new_gen;
        o.metadata.byte_size = req.byte_size;
        o.metadata.logical_size = req.byte_size;
        if (o.lifecycle != LifecycleState::LeasedWrite) {
            o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::LeasedWrite);
        }
        o.active_op_token = ++seq_;
        o.active_op_kind = "write";
        o.active_op_target = o.authoritative_node;
        o.active_op_domain = StorageDomain::HostMemory;
        o.integrity.crc32c = req.crc32c;
        o.integrity.sha = req.sha;
        o.integrity.bytes = static_cast<uint32_t>(req.byte_size);
        op_token = o.active_op_token;
        auth_node = o.authoritative_node;
        meta = o.metadata;
        mark_dirty();
        persist_if_dirty();

        auto op = std::make_shared<PendingOp>();
        op->op_token = op_token;
        op->object_id = req.object_id;
        op->generation = new_gen;
        op->kind = "write";
        op->target_node = auth_node;
        op->timeout_ms = cfg_.create_timeout_ms;
        op->created_ms = now_unix_ms();
        pending_ops_[op_token] = op;
    }

    const CoordNode* node = find_node(auth_node);
    data_addr = node->addr;
    InstructRecvReq ir;
    ir.op_token = op_token;
    ir.object_id = req.object_id;
    ir.generation = new_gen;
    ir.source_node = 0;
    ir.byte_size = req.byte_size;
    ir.crc32c = req.crc32c;
    ir.sha = req.sha;
    ir.role = static_cast<uint32_t>(CopyRole::Authoritative);
    ir.domain = static_cast<uint32_t>(StorageDomain::HostMemory);
    ir.metadata = meta;
    Writer w;
    serialize_instruct_recv(w, ir);
    send_instruction(auth_node, kMsgInstructRecv, w.data());

    JsonObject doc;
    doc.field("id", req.object_id.to_hex());
    doc.field("gen", new_gen.value());
    emit("object.write_requested", doc);

    ObjectWriteResp resp;
    resp.result = RpcResult::success();
    resp.generation = new_gen;
    resp.data_addr = data_addr;
    resp.op_token = op_token;
    Writer w2;
    serialize_object_write_resp(w2, resp);
    send_to_conn(conn, kMsgObjectWriteResp, req_id, w2.data());
}

// ---------------------------------------------------------------------------
// Data result handling and op commit
// ---------------------------------------------------------------------------
void Coordinator::handle_data_result(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    DataResultMsg msg = deserialize_data_result(r);
    std::shared_ptr<PendingOp> op;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto pit = pending_ops_.find(msg.op_token);
        if (pit == pending_ops_.end()) {
            DataResultResp resp;
            resp.result = RpcResult::success();
            Writer w;
            serialize_data_result_resp(w, resp);
            send_to_conn(conn, kMsgDataResultResp, req_id, w.data());
            return;
        }
        op = pit->second;
        if (op->done) {
            DataResultResp resp;
            resp.result = RpcResult::success();
            Writer w;
            serialize_data_result_resp(w, resp);
            send_to_conn(conn, kMsgDataResultResp, req_id, w.data());
            return;
        }
    }

    std::lock_guard<std::mutex> lock(state_mutex_);
    if (op->kind == "verify") {
        op->verify_result = msg.result;
        op->verify_bytes = msg.bytes_moved;
        op->verify_crc = msg.crc32c;
        op->verify_sha = msg.sha;
        op->verify_duration_ms = msg.duration_ms;
        op->done = true;
        op->committed = msg.result.ok;
        op->cv.notify_all();
    } else {
        bool is_receiver;
        if (op->kind == "persist" || op->kind == "create" || op->kind == "write") {
            is_receiver = true;  // single participant
        } else {
            is_receiver = (conn->node_id == op->target_node);
        }

        if (is_receiver) {
            op->receiver_done = 1;
            op->receiver_result = msg.result;
            op->receiver_crc = msg.crc32c;
            op->receiver_sha = msg.sha;
            op->bytes_moved = msg.bytes_moved;
            op->duration_ms = msg.duration_ms;
        } else {
            op->sender_done = 1;
            op->sender_result = msg.result;
        }

        bool commit_ready;
        if (op->kind == "persist" || op->kind == "create" || op->kind == "write") {
            commit_ready = op->receiver_done != 0;
        } else if (op->source_node == op->target_node) {
            // Same-node transfer (domain migration / self-load restore): the
            // single participant reports once.
            commit_ready = op->receiver_done != 0;
        } else {
            commit_ready = op->receiver_done != 0 && (op->sender_done != 0 || op->kind == "replicate");
        }

        if (commit_ready) {
            bool ok = op->receiver_result.ok;
            if (op->sender_done != 0 && op->kind != "persist" && op->kind != "create" &&
                op->kind != "write" && !op->sender_result.ok) {
                ok = false;
            }
            if (ok) {
                commit_pending_op(op);
            } else {
                std::string reason = op->receiver_result.ok ? op->sender_result.message : op->receiver_result.message;
                abort_pending_op(op, reason.empty() ? "participant reported failure" : reason);
            }
        }
    }

    DataResultResp ack;
    ack.result = RpcResult::success();
    Writer w;
    serialize_data_result_resp(w, ack);
    send_to_conn(conn, kMsgDataResultResp, req_id, w.data());
}

void Coordinator::commit_pending_op(std::shared_ptr<PendingOp> op) {
    // Caller holds state_mutex_.
    pending_ops_.erase(op->op_token);
    auto oit = objects_.find(op->object_id);
    if (oit == objects_.end()) {
        op->done = true;
        op->committed = false;
        op->reason = "object disappeared during operation";
        op->cv.notify_all();
        return;
    }
    CoordObject& o = oit->second;
    if (o.active_op_token != op->op_token) {
        op->done = true;
        op->committed = false;
        op->reason = "operation superseded";
        op->cv.notify_all();
        return;
    }

    std::string opname = op->kind;
    if (op->kind == "replicate") {
        if (std::find(o.replicas.begin(), o.replicas.end(), op->target_node) == o.replicas.end()) {
            o.replicas.push_back(op->target_node);
        }
        o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Available);
    } else if (op->kind == "migrate") {
        o.authoritative_node = op->target_node;
        o.authoritative_domain = op->target_domain;
        o.lease_generation += 1;  // ownership changed; old lease tokens invalid
        if (op->keep_source) {
            if (std::find(o.replicas.begin(), o.replicas.end(), o.owner_node) == o.replicas.end()) {
                o.replicas.push_back(o.owner_node);
            }
        } else {
            o.replicas.erase(std::remove(o.replicas.begin(), o.replicas.end(), o.owner_node), o.replicas.end());
        }
        o.owner_node = op->target_node;
        o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Available);
        auto wit = write_leases_.find(op->object_id);
        if (wit != write_leases_.end() && wit->second.holder.value() != op->target_node) {
            write_leases_.erase(wit);
        }
    } else if (op->kind == "persist") {
        if (std::find(o.persistent_nodes.begin(), o.persistent_nodes.end(), op->target_node) ==
            o.persistent_nodes.end()) {
            o.persistent_nodes.push_back(op->target_node);
        }
        o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Available);
    } else if (op->kind == "restore") {
        o.authoritative_node = op->target_node;
        o.authoritative_domain = StorageDomain::HostMemory;
        o.lease_generation += 1;
        o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Available);
    } else if (op->kind == "write") {
        o.integrity.crc32c = op->receiver_crc;
        o.integrity.sha = op->receiver_sha;
        o.integrity.bytes = static_cast<uint32_t>(o.metadata.byte_size);
        if (o.lifecycle != LifecycleState::LeasedWrite) {
            o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::LeasedWrite);
        }
        auto wit = write_leases_.find(op->object_id);
        if (wit == write_leases_.end()) {
            o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Available);
        }
    } else if (op->kind == "create") {
        o.integrity.crc32c = op->receiver_crc;
        o.integrity.sha = op->receiver_sha;
        o.integrity.bytes = static_cast<uint32_t>(o.metadata.byte_size);
        o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Available);
    }

    o.active_op_token = 0;
    o.active_op_kind.clear();
    o.active_op_target = 0;
    o.active_op_domain = StorageDomain::Unknown;
    o.active_op_reports = 0;

    mark_dirty();
    persist_if_dirty();

    op->done = true;
    op->committed = true;
    op->reason = "committed:" + op->kind;
    op->cv.notify_all();

    JsonObject doc;
    doc.field("id", op->object_id.to_hex());
    doc.field("gen", op->generation.value());
    doc.field("op", opname);
    doc.field("bytes", op->bytes_moved);
    emit("coordinator.op_committed", doc);
}

void Coordinator::abort_pending_op(std::shared_ptr<PendingOp> op, const std::string& reason) {
    // Caller holds state_mutex_.
    std::vector<uint64_t> notify_nodes;
    pending_ops_.erase(op->op_token);
    auto oit = objects_.find(op->object_id);
    if (oit != objects_.end()) {
        CoordObject& o = oit->second;
        if (o.active_op_token == op->op_token) {
            if (o.active_op_target != 0) notify_nodes.push_back(o.active_op_target);
            if (o.authoritative_node != 0 && o.authoritative_node != o.active_op_target) {
                notify_nodes.push_back(o.authoritative_node);
            }
            o.active_op_token = 0;
            o.active_op_kind.clear();
            o.active_op_target = 0;
            o.active_op_domain = StorageDomain::Unknown;
            o.active_op_reports = 0;
            if (o.lifecycle == LifecycleState::Creating) {
                o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Released);
            } else if (o.lifecycle == LifecycleState::Replicating ||
                       o.lifecycle == LifecycleState::Migrating ||
                       o.lifecycle == LifecycleState::Persisting ||
                       o.lifecycle == LifecycleState::Restoring) {
                o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Available);
            }
            mark_dirty();
            persist_if_dirty();
        }
    }

    op->done = true;
    op->committed = false;
    op->reason = reason;
    op->cv.notify_all();

    JsonObject doc;
    doc.field("id", op->object_id.to_hex());
    doc.field("op", op->kind);
    doc.field("reason", reason);
    emit("coordinator.op_aborted", doc);

    for (uint64_t node : notify_nodes) {
        InstructAbortReq ar;
        ar.op_token = op->op_token;
        ar.object_id = op->object_id;
        ar.generation = op->generation;
        Writer w;
        serialize_instruct_abort(w, ar);
        try {
            send_instruction(node, kMsgInstructAbort, w.data());
        } catch (const CfError&) {
        }
    }
}

// ---------------------------------------------------------------------------
// Orchestration (sync ops)
// ---------------------------------------------------------------------------
std::vector<uint8_t> Coordinator::orchestrate_transfer(const std::string& kind, ObjectId object_id,
                                                       uint64_t target_node, StorageDomain target_domain,
                                                       bool keep_source, uint64_t on_node) {
    uint64_t op_token = 0;
    Generation gen;
    uint64_t source_node = 0;
    uint64_t byte_size = 0;
    uint64_t crc = 0, sha = 0;
    ContextMetadata meta;
    NodeAddress source_addr;
    NodeAddress target_addr;

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto oit = objects_.find(object_id);
        if (oit == objects_.end()) throw_error(ErrKind::NotFound, "object not found");
        CoordObject& o = oit->second;
        if (o.in_flight()) throw_error(ErrKind::Conflict, "object has an in-flight operation");
        if (o.lifecycle == LifecycleState::Released || o.lifecycle == LifecycleState::Corrupt) {
            throw_error(ErrKind::State, "object not operable in current state");
        }
        if (o.authoritative_node == 0) throw_error(ErrKind::State, "object has no authoritative copy");

        if (kind == "replicate" || kind == "persist" || kind == "restore") {
            auto wit = write_leases_.find(object_id);
            if (wit != write_leases_.end() && !wit->second.expired(now_unix_ms()) &&
                wit->second.holder.value() != o.owner_node) {
                throw_error(ErrKind::Conflict, "write lease held by another node");
            }
        }
        if (kind == "migrate") {
            auto wit = write_leases_.find(object_id);
            if (wit != write_leases_.end() && wit->second.holder.value() != o.owner_node) {
                throw_error(ErrKind::Conflict, "write lease held by another node");
            }
        }

        if (kind == "replicate") {
            if (target_node == o.authoritative_node) {
                throw_error(ErrKind::InvalidArgument, "cannot replicate to the authoritative node");
            }
            if (std::find(o.replicas.begin(), o.replicas.end(), target_node) != o.replicas.end()) {
                throw_error(ErrKind::AlreadyExists, "replica already present on target node");
            }
            if (!node_online(target_node)) throw_error(ErrKind::NotFound, "target node not online");
            const CoordNode* t = find_node(target_node);
            if (!t->caps.host_memory) throw_error(ErrKind::NotSupported, "target lacks host memory");
            if (t->caps.max_host_bytes != 0 && o.metadata.byte_size > t->caps.max_host_bytes) {
                throw_error(ErrKind::Capacity, "target host capacity cannot hold object");
            }
            o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Replicating);
            op_token = ++seq_;
            gen = o.generation;
            source_node = o.authoritative_node;
            byte_size = o.metadata.byte_size;
            crc = o.integrity.crc32c;
            sha = o.integrity.sha;
            meta = o.metadata;
            o.active_op_token = op_token;
            o.active_op_kind = "replicate";
            o.active_op_target = target_node;
            o.active_op_domain = StorageDomain::HostMemory;
        } else if (kind == "migrate") {
            if (target_node == o.authoritative_node && target_domain == o.authoritative_domain) {
                throw_error(ErrKind::InvalidArgument, "cannot migrate to the current authoritative location");
            }
            if (!node_online(target_node)) throw_error(ErrKind::NotFound, "target node not online");
            const CoordNode* t = find_node(target_node);
            if (target_domain == StorageDomain::HostMemory && !t->caps.host_memory) {
                throw_error(ErrKind::NotSupported, "target lacks host memory");
            }
            if (target_domain == StorageDomain::AcceleratorLocal && !t->caps.accelerator) {
                throw_error(ErrKind::NotSupported, "target lacks accelerator domain");
            }
            if (t->caps.max_host_bytes != 0 && o.metadata.byte_size > t->caps.max_host_bytes) {
                throw_error(ErrKind::Capacity, "target capacity cannot hold object");
            }
            o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Migrating);
            op_token = ++seq_;
            gen = o.generation;
            source_node = o.authoritative_node;
            byte_size = o.metadata.byte_size;
            crc = o.integrity.crc32c;
            sha = o.integrity.sha;
            meta = o.metadata;
            o.active_op_token = op_token;
            o.active_op_kind = "migrate";
            o.active_op_target = target_node;
            o.active_op_domain = target_domain;
            o.active_op_keep_source = keep_source;
        } else if (kind == "persist") {
            uint64_t node = on_node != 0 ? on_node : o.authoritative_node;
            if (!node_online(node)) throw_error(ErrKind::NotFound, "persist node not online");
            const CoordNode* p = find_node(node);
            if (!p->caps.local_persistent) throw_error(ErrKind::NotSupported, "node lacks persistent storage");
            if (std::find(o.persistent_nodes.begin(), o.persistent_nodes.end(), node) != o.persistent_nodes.end()) {
                throw_error(ErrKind::AlreadyExists, "object already persisted on node");
            }
            o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Persisting);
            op_token = ++seq_;
            gen = o.generation;
            source_node = o.authoritative_node;
            byte_size = o.metadata.byte_size;
            crc = o.integrity.crc32c;
            sha = o.integrity.sha;
            meta = o.metadata;
            o.active_op_token = op_token;
            o.active_op_kind = "persist";
            o.active_op_target = node;
            o.active_op_domain = StorageDomain::LocalPersistent;
            target_node = node;
        } else if (kind == "restore") {
            uint64_t holder = 0;
            for (uint64_t n : o.persistent_nodes) {
                if (node_online(n)) {
                    holder = n;
                    break;
                }
            }
            if (holder == 0) throw_error(ErrKind::NotFound, "no live node holds a persistent copy");
            uint64_t dest = on_node != 0 ? on_node : o.authoritative_node;
            if (dest == 0 || !node_online(dest)) dest = o.owner_node;
            if (!node_online(dest)) dest = holder;
            if (!node_online(dest)) throw_error(ErrKind::NotFound, "no online destination for restore");
            const CoordNode* d = find_node(dest);
            if (!d->caps.host_memory) throw_error(ErrKind::NotSupported, "restore destination lacks host memory");

            if (o.lifecycle == LifecycleState::Recovering) {
                o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Restoring);
            } else {
                o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Restoring);
            }
            op_token = ++seq_;
            gen = o.generation;
            source_node = holder;
            byte_size = o.metadata.byte_size;
            crc = o.integrity.crc32c;
            sha = o.integrity.sha;
            meta = o.metadata;
            o.active_op_token = op_token;
            o.active_op_kind = "restore";
            o.active_op_target = dest;
            o.active_op_domain = StorageDomain::HostMemory;
            target_node = dest;
        }
        mark_dirty();
        persist_if_dirty();
        const CoordNode* s = find_node(source_node);
        const CoordNode* t = find_node(target_node);
        source_addr = s->addr;
        if (t) target_addr = t->addr;
    }


    auto op = std::make_shared<PendingOp>();
    op->op_token = op_token;
    op->object_id = object_id;
    op->generation = gen;
    op->kind = kind;
    op->target_node = target_node;
    op->source_node = source_node;
    op->target_domain = target_domain;
    op->keep_source = keep_source;
    op->on_node = on_node;
    op->created_ms = now_unix_ms();
    op->timeout_ms = cfg_.op_timeout_ms;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        pending_ops_[op_token] = op;
    }

    if (kind == "persist") {
        InstructRecvReq ir;
        ir.op_token = op_token;
        ir.object_id = object_id;
        ir.generation = gen;
        ir.source_node = 0;  // self-load from own authoritative copy
        ir.byte_size = byte_size;
        ir.crc32c = crc;
        ir.sha = sha;
        ir.role = static_cast<uint32_t>(CopyRole::Persistent);
        ir.domain = static_cast<uint32_t>(StorageDomain::LocalPersistent);
        ir.metadata = meta;
        Writer w;
        serialize_instruct_recv(w, ir);
        send_instruction(op->target_node, kMsgInstructRecv, w.data());
    } else if (kind == "restore") {
        if (source_node == op->target_node) {
            InstructRecvReq ir;
            ir.op_token = op_token;
            ir.object_id = object_id;
            ir.generation = gen;
            ir.source_node = source_node;  // self-load from persistent
            ir.byte_size = byte_size;
            ir.crc32c = crc;
            ir.sha = sha;
            ir.role = static_cast<uint32_t>(CopyRole::Authoritative);
            ir.domain = static_cast<uint32_t>(StorageDomain::HostMemory);
            ir.self_load = 1;
            ir.metadata = meta;
            Writer w;
            serialize_instruct_recv(w, ir);
            send_instruction(op->target_node, kMsgInstructRecv, w.data());
        } else {
            InstructRecvReq ir;
            ir.op_token = op_token;
            ir.object_id = object_id;
            ir.generation = gen;
            ir.source_node = source_node;
            ir.source_addr = source_addr;
            ir.byte_size = byte_size;
            ir.crc32c = crc;
            ir.sha = sha;
            ir.role = static_cast<uint32_t>(CopyRole::Authoritative);
            ir.domain = static_cast<uint32_t>(StorageDomain::HostMemory);
            ir.metadata = meta;
            Writer w;
            serialize_instruct_recv(w, ir);
            send_instruction(op->target_node, kMsgInstructRecv, w.data());

            InstructSendReq is;
            is.op_token = op_token;
            is.object_id = object_id;
            is.generation = gen;
            is.dest_node = op->target_node;
            is.dest_addr = target_addr;
            is.byte_size = byte_size;
            is.crc32c = crc;
            is.sha = sha;
            Writer w2;
            serialize_instruct_send(w2, is);
            send_instruction(source_node, kMsgInstructSend, w2.data());
        }
    } else {
        // replicate / migrate: source sends to target.
        InstructRecvReq ir;
        ir.op_token = op_token;
        ir.object_id = object_id;
        ir.generation = gen;
        ir.source_node = source_node;
        ir.source_addr = source_addr;
        ir.byte_size = byte_size;
        ir.crc32c = crc;
        ir.sha = sha;
        ir.role = static_cast<uint32_t>(kind == "migrate" ? CopyRole::Authoritative : CopyRole::Replica);
        ir.domain = static_cast<uint32_t>(op->target_domain);
        ir.metadata = meta;
        Writer w;
        serialize_instruct_recv(w, ir);
        send_instruction(op->target_node, kMsgInstructRecv, w.data());

        InstructSendReq is;
        is.op_token = op_token;
        is.object_id = object_id;
        is.generation = gen;
        is.dest_node = op->target_node;
        is.dest_addr = target_addr;
        is.byte_size = byte_size;
        is.crc32c = crc;
        is.sha = sha;
        is.keep_as_replica = kind == "migrate" && keep_source ? 1 : 0;
        is.delete_after_send = kind == "migrate" && !keep_source ? 1 : 0;
        Writer w2;
        serialize_instruct_send(w2, is);
        send_instruction(source_node, kMsgInstructSend, w2.data());
    }

    // Wait for completion.
    {
        std::unique_lock<std::mutex> lock(op->cv_mutex);
        op->cv.wait_for(lock, std::chrono::milliseconds(op->timeout_ms), [&] { return op->done; });
    }
    if (!op->done) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        abort_pending_op(op, "operation timed out");
    }
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        pending_ops_.erase(op->op_token);
    }

    if (!op->committed) {
        throw_error(ErrKind::Conflict, op->reason.empty() ? "operation failed" : op->reason);
    }

    // Post-commit disposal: for migration, the old authoritative node must not
    // delete its copy until the coordinator has committed, so it is told now.
    // Same-node domain migration (e.g. host -> accelerator) has no separate
    // source process to dispose; the destination already staged the copy.
    if (kind == "migrate" && source_node != target_node) {
        InstructDisposeReq dr;
        dr.op_token = op_token;
        dr.object_id = object_id;
        dr.generation = gen;
        dr.keep_as_replica = keep_source ? 1 : 0;
        Writer w;
        serialize_instruct_dispose(w, dr);
        try {
            send_instruction(source_node, kMsgInstructDispose, w.data());
        } catch (const CfError&) {
        }
    }

    Writer w;
    if (kind == "replicate") {
        ReplicateResp resp;
        resp.result = RpcResult::success();
        resp.created_at.node_id = target_node;
        resp.created_at.domain = StorageDomain::HostMemory;
        serialize_replicate_resp(w, resp);
    } else if (kind == "migrate") {
        MigrateResp resp;
        resp.result = RpcResult::success();
        resp.moved_to.node_id = target_node;
        resp.moved_to.domain = target_domain;
        serialize_migrate_resp(w, resp);
    } else if (kind == "persist") {
        PersistResp resp;
        resp.result = RpcResult::success();
        resp.node = op->target_node;
        serialize_persist_resp(w, resp);
    } else if (kind == "restore") {
        RestoreResp resp;
        resp.result = RpcResult::success();
        resp.restored_to.node_id = op->target_node;
        resp.restored_to.domain = StorageDomain::HostMemory;
        serialize_restore_resp(w, resp);
    } else {
        throw_error(ErrKind::Internal, "unknown op kind");
    }
    return w.data();
}

void Coordinator::handle_replicate(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    ReplicateReq req = deserialize_replicate(r);
    std::vector<uint8_t> payload =
        orchestrate_transfer("replicate", req.object_id, req.target_node, StorageDomain::HostMemory, false, 0);

    JsonObject doc;
    doc.field("id", req.object_id.to_hex());
    doc.field("target", req.target_node);
    emit("replicate.committed", doc);

    send_to_conn(conn, kMsgReplicateResp, req_id, payload);
}

void Coordinator::handle_migrate(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    MigrateReq req = deserialize_migrate(r);
    std::vector<uint8_t> payload =
        orchestrate_transfer("migrate", req.object_id, req.target_node,
                             static_cast<StorageDomain>(req.target_domain), req.keep_source != 0, 0);

    JsonObject doc;
    doc.field("id", req.object_id.to_hex());
    doc.field("target", req.target_node);
    emit("migrate.committed", doc);

    send_to_conn(conn, kMsgMigrateResp, req_id, payload);
}

void Coordinator::handle_persist(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    PersistReq req = deserialize_persist(r);
    std::vector<uint8_t> payload =
        orchestrate_transfer("persist", req.object_id, 0, StorageDomain::LocalPersistent, false, req.on_node);

    JsonObject doc;
    doc.field("id", req.object_id.to_hex());
    emit("persist.committed", doc);
    send_to_conn(conn, kMsgPersistResp, req_id, payload);
}

void Coordinator::handle_restore(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    RestoreReq req = deserialize_restore(r);
    std::vector<uint8_t> payload =
        orchestrate_transfer("restore", req.object_id, 0, StorageDomain::HostMemory, false, req.to_node);

    JsonObject doc;
    doc.field("id", req.object_id.to_hex());
    emit("restore.committed", doc);
    send_to_conn(conn, kMsgRestoreResp, req_id, payload);
}

void Coordinator::handle_verify(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    VerifyReq req = deserialize_verify(r);
    uint64_t node = 0;
    Generation gen;
    uint64_t crc = 0, sha = 0;
    uint64_t op_token = 0;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto oit = objects_.find(req.object_id);
        if (oit == objects_.end()) throw_error(ErrKind::NotFound, "object not found");
        CoordObject& o = oit->second;
        if (o.lifecycle == LifecycleState::Released) throw_error(ErrKind::State, "object released");

        node = req.on_node != 0 ? req.on_node : o.authoritative_node;
        if (node == 0 || !node_online(node)) throw_error(ErrKind::NotFound, "verify node not online");
        gen = o.generation;
        crc = o.integrity.crc32c;
        sha = o.integrity.sha;
        op_token = ++seq_;

        auto op = std::make_shared<PendingOp>();
        op->op_token = op_token;
        op->object_id = req.object_id;
        op->generation = gen;
        op->kind = "verify";
        op->target_node = node;
        op->created_ms = now_unix_ms();
        op->timeout_ms = cfg_.op_timeout_ms;
        pending_ops_[op_token] = op;
    }

    InstructVerifyReq iv;
    iv.op_token = op_token;
    iv.object_id = req.object_id;
    iv.generation = gen;
    iv.byte_size = objects_[req.object_id].metadata.byte_size;
    iv.crc32c = crc;
    iv.sha = sha;
    iv.deep = req.deep;
    Writer w;
    serialize_instruct_verify(w, iv);
    send_instruction(node, kMsgInstructVerify, w.data());

    std::shared_ptr<PendingOp> op;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto pit = pending_ops_.find(op_token);
        if (pit == pending_ops_.end()) throw_error(ErrKind::Internal, "verify op missing");
        op = pit->second;
    }
    {
        std::unique_lock<std::mutex> lock(op->cv_mutex);
        op->cv.wait_for(lock, std::chrono::milliseconds(op->timeout_ms), [&] { return op->done; });
    }
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        pending_ops_.erase(op_token);
    }

    VerifyResp resp;
    if (!op->done) {
        resp.result = RpcResult::failure(ErrKind::Timeout, "verify timed out");
        resp.ok = false;
    } else if (!op->verify_result.ok) {
        resp.result = op->verify_result;
        resp.ok = false;
    } else {
        resp.result = RpcResult::success();
        resp.ok = true;
        resp.bytes = op->verify_bytes;
        resp.crc32c = op->verify_crc;
        resp.sha = op->verify_sha;
        resp.duration_ms = op->verify_duration_ms;
    }
    Writer w2;
    serialize_verify_resp(w2, resp);
    send_to_conn(conn, kMsgVerifyResp, req_id, w2.data());
}

void Coordinator::handle_placement_query(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    PlacementQueryReq req = deserialize_placement_query(r);
    std::lock_guard<std::mutex> lock(state_mutex_);
    PlacementQueryResp resp;
    resp.result = RpcResult::success();

    PlacementEngine engine;
    std::vector<PlacementCandidate> candidates;
    Location current;
    for (const auto& [nid, node] : nodes_) {
        if (!node.online) continue;
        if (node.caps.host_memory) {
            PlacementCandidate c;
            c.location.node_id = nid;
            c.location.domain = StorageDomain::HostMemory;
            c.caps = node.caps;
            c.total_bytes = node.caps.max_host_bytes;
            c.bandwidth_bps = 1e9;
            candidates.push_back(c);
        }
        if (node.caps.accelerator) {
            PlacementCandidate c;
            c.location.node_id = nid;
            c.location.domain = StorageDomain::AcceleratorLocal;
            c.caps = node.caps;
            c.total_bytes = node.caps.max_accelerator_bytes;
            candidates.push_back(c);
        }
    }
    if (req.current_node != 0) {
        current.node_id = req.current_node;
        current.domain = StorageDomain::HostMemory;
    }
    if (candidates.empty()) {
        resp.result = RpcResult::failure(ErrKind::NotFound, "no online placement candidates");
    } else {
        resp.decision = engine.decide(req.metadata, current, candidates, kPolicyCostAware);
    }
    Writer w;
    serialize_placement_query_resp(w, resp);
    send_to_conn(conn, kMsgPlacementQueryResp, req_id, w.data());
}

void Coordinator::handle_decision_query(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    DecisionQueryReq req = deserialize_decision_query(r);
    std::lock_guard<std::mutex> lock(state_mutex_);
    DecisionQueryResp resp;
    resp.result = RpcResult::success();
    TransferVsRecompute planner;
    resp.decision = planner.decide(req.metadata, req.current_node, req.target_node, req.measured_bandwidth_bps);
    Writer w;
    serialize_decision_query_resp(w, resp);
    send_to_conn(conn, kMsgDecisionQueryResp, req_id, w.data());
}

void Coordinator::handle_coord_shutdown_node(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r) {
    CoordShutdownNodeReq req = deserialize_coord_shutdown_node(r);
    CoordShutdownNodeResp resp;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto nit = nodes_.find(req.node_id);
        if (nit == nodes_.end() || !nit->second.online) {
            resp.result = RpcResult::failure(ErrKind::NotFound, "node not online");
        } else {
            resp.result = RpcResult::success();
        }
    }
    if (resp.result.ok) {
        InstructShutdownReq is;
        is.node_id = req.node_id;
        Writer w;
        serialize_instruct_shutdown(w, is);
        try {
            send_instruction(req.node_id, kMsgInstructShutdown, w.data());
        } catch (const CfError&) {
            resp.result = RpcResult::failure(ErrKind::NotFound, "node connection lost");
        }
    }
    Writer w;
    serialize_coord_shutdown_node_resp(w, resp);
    send_to_conn(conn, kMsgCoordShutdownNodeResp, req_id, w.data());
}

// ---------------------------------------------------------------------------
// Node disconnect cleanup and recovery
// ---------------------------------------------------------------------------
void Coordinator::node_disconnected(uint64_t node_id) {
    // Caller holds state_mutex_.
    auto nit = nodes_.find(node_id);
    if (nit == nodes_.end()) return;
    nit->second.online = false;
    {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        node_conns_.erase(node_id);
    }

    JsonObject doc;
    doc.field("node", node_id);
    emit("node.offline", doc);

    // Abort any in-flight op involving this node.
    std::vector<uint64_t> ops_to_abort;
    for (auto& [token, op] : pending_ops_) {
        if (op->target_node == node_id || op->on_node == node_id) {
            ops_to_abort.push_back(token);
        }
    }
    for (uint64_t token : ops_to_abort) {
        auto it = pending_ops_.find(token);
        if (it == pending_ops_.end()) continue;
        abort_pending_op(it->second, "node disconnected during operation");
    }

    // Revoke read leases held by the node.
    std::vector<ObjectId> to_clean;
    for (auto& [oid, leases] : read_leases_) {
        leases.erase(std::remove_if(leases.begin(), leases.end(),
                                    [&](const Lease& l) { return l.holder.value() == node_id; }),
                     leases.end());
        if (leases.empty()) to_clean.push_back(oid);
    }
    for (auto& oid : to_clean) read_leases_.erase(oid);

    // Revoke write leases held by the node.
    std::vector<ObjectId> revoked_writes;
    for (auto& [oid, lease] : write_leases_) {
        if (lease.holder.value() == node_id) {
            revoked_writes.push_back(oid);
        }
    }
    for (auto& oid : revoked_writes) {
        write_leases_.erase(oid);
        auto oit = objects_.find(oid);
        if (oit != objects_.end()) {
            oit->second.lease_generation += 1;
        }
    }

    // Handle objects authoritative on the disconnected node.
    for (auto& [oid, o] : objects_) {
        if (o.authoritative_node != node_id) continue;
        if (o.in_flight()) continue;  // handled above
        if (o.lifecycle == LifecycleState::Released || o.lifecycle == LifecycleState::Corrupt ||
            o.lifecycle == LifecycleState::Expired) {
            continue;  // terminal states are not recovered
        }
        uint64_t elect = 0;
        for (uint64_t rn : o.replicas) {
            if (node_online(rn)) {
                if (elect == 0 || rn < elect) elect = rn;
            }
        }
        if (elect != 0) {
            o.authoritative_node = elect;
            o.authoritative_domain = StorageDomain::HostMemory;
            o.lease_generation += 1;
            o.replicas.erase(std::remove(o.replicas.begin(), o.replicas.end(), elect), o.replicas.end());
            o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Recovering);
            o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Available);

            JsonObject d2;
            d2.field("id", oid.to_hex());
            d2.field("promoted_to", elect);
            emit("recovery.promoted_replica", d2);

            InstructPromoteReq pr;
            pr.object_id = oid;
            pr.generation = o.generation;
            Writer w;
            serialize_instruct_promote(w, pr);
            try {
                send_instruction(elect, kMsgInstructPromote, w.data());
            } catch (const CfError&) {
            }
        } else {
            o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Recovering);
            JsonObject d3;
            d3.field("id", oid.to_hex());
            emit("recovery.authority_lost", d3);
        }
    }
    mark_dirty();
    persist_if_dirty();
}

void Coordinator::sweep_loop() {
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        if (!running_.load()) break;
        try {
            sweep_once();
        } catch (const std::exception& e) {
            JsonObject doc;
            doc.field("error", e.what());
            emit("coordinator.sweep_error", doc);
        }
    }
}

void Coordinator::sweep_once() {
    std::lock_guard<std::mutex> lock(state_mutex_);
        uint64_t now = now_unix_ms();

        // Node heartbeat timeout.
        std::vector<uint64_t> dead;
        for (auto& [nid, node] : nodes_) {
            if (node.online && now - node.last_heartbeat_ms > cfg_.session_timeout_ms) dead.push_back(nid);
        }
        for (uint64_t nid : dead) node_disconnected(nid);

        // Lease expiry.
        std::vector<ObjectId> expired;
        for (auto& [oid, lease] : write_leases_) {
            if (lease.expired(now)) expired.push_back(oid);
        }
        for (auto& oid : expired) {
            write_leases_.erase(oid);
            auto oit = objects_.find(oid);
            if (oit != objects_.end()) {
                oit->second.lease_generation += 1;
                if (oit->second.lifecycle == LifecycleState::LeasedWrite) {
                    oit->second.lifecycle = LifecycleAuthority::transition(oit->second.lifecycle, LifecycleState::Available);
                }
            }
            JsonObject d;
            d.field("id", oid.to_hex());
            emit("lease.expired_write", d);
        }
        std::vector<ObjectId> expired_read;
        for (auto& [oid, leases] : read_leases_) {
            leases.erase(std::remove_if(leases.begin(), leases.end(), [&](const Lease& l) { return l.expired(now); }),
                         leases.end());
            if (leases.empty()) expired_read.push_back(oid);
        }
        for (auto& oid : expired_read) {
            read_leases_.erase(oid);
            auto oit = objects_.find(oid);
            if (oit != objects_.end() && oit->second.lifecycle == LifecycleState::LeasedRead) {
                oit->second.lifecycle = LifecycleAuthority::transition(oit->second.lifecycle, LifecycleState::Available);
            }
        }

        // Pending op timeout (async ops: create/write).
        std::vector<uint64_t> timed_out;
        for (auto& [token, op] : pending_ops_) {
            if (op->done) continue;
            if (now - op->created_ms > op->timeout_ms) timed_out.push_back(token);
        }
        for (uint64_t token : timed_out) {
            auto pit = pending_ops_.find(token);
            if (pit == pending_ops_.end()) continue;
            abort_pending_op(pit->second, "operation timed out");
        }

        // Object lifetime expiry.
        std::vector<ObjectId> life_expired;
        for (auto& [oid, o] : objects_) {
            if (o.expires_unix_ms != 0 && now >= o.expires_unix_ms && o.lifecycle != LifecycleState::Expired &&
                o.lifecycle != LifecycleState::Released) {
                life_expired.push_back(oid);
            }
        }
        for (auto& oid : life_expired) {
            auto oit = objects_.find(oid);
            if (oit == objects_.end()) continue;
            oit->second.lifecycle = LifecycleAuthority::transition(oit->second.lifecycle, LifecycleState::Expired);
            JsonObject d;
            d.field("id", oid.to_hex());
            emit("object.expired", d);
        }
}

void Coordinator::reconcile_at_startup() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    // In-flight operations are never durable; on restart they are aborted to
    // the deterministic pre-operation committed state.
    for (auto& [oid, o] : objects_) {
        o.active_op_token = 0;
        o.active_op_kind.clear();
        o.active_op_target = 0;
        o.active_op_domain = StorageDomain::Unknown;
        o.active_op_reports = 0;
        switch (o.lifecycle) {
            case LifecycleState::Creating:
                o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Released);
                break;
            case LifecycleState::Replicating:
            case LifecycleState::Migrating:
            case LifecycleState::Persisting:
            case LifecycleState::Restoring:
            case LifecycleState::LeasedRead:
            case LifecycleState::LeasedWrite:
            case LifecycleState::Invalidating:
                o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Available);
                break;
            default:
                break;
        }
        if (o.lifecycle != LifecycleState::Released && o.lifecycle != LifecycleState::Corrupt &&
            o.lifecycle != LifecycleState::Expired) {
            auto nit = nodes_.find(o.authoritative_node);
            if (nit == nodes_.end()) {
                o.lifecycle = LifecycleAuthority::transition(o.lifecycle, LifecycleState::Recovering);
            }
        }
    }
    write_leases_.clear();
    read_leases_.clear();
    pending_ops_.clear();
    mark_dirty();
    persist_if_dirty();
}

}  // namespace ctf