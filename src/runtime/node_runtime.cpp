#include "context_fabric/runtime/node_runtime.h"

#include <algorithm>
#include <thread>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <new>

#include "context_fabric/core/clock.h"
#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/core/version.h"

namespace ctf {

namespace fs = std::filesystem;

namespace {

class ActiveSocketGuard {
public:
    ActiveSocketGuard(std::mutex& mutex, std::unordered_set<Socket*>& sockets, Socket& socket)
        : mutex_(mutex), sockets_(sockets), socket_(socket) {
        std::lock_guard<std::mutex> lock(mutex_);
        sockets_.insert(&socket_);
    }

    ~ActiveSocketGuard() {
        std::lock_guard<std::mutex> lock(mutex_);
        sockets_.erase(&socket_);
        socket_.close();
    }

    ActiveSocketGuard(const ActiveSocketGuard&) = delete;
    ActiveSocketGuard& operator=(const ActiveSocketGuard&) = delete;

private:
    std::mutex& mutex_;
    std::unordered_set<Socket*>& sockets_;
    Socket& socket_;
};

uint64_t remaining_timeout_ms(uint64_t deadline_ms) {
    const uint64_t now = steady_now_ms();
    if (now >= deadline_ms) throw_error(ErrKind::Timeout, "operation deadline exceeded");
    return deadline_ms - now;
}

uint64_t operation_deadline_ms(uint64_t timeout_ms) noexcept {
    const uint64_t now = steady_now_ms();
    return timeout_ms > (std::numeric_limits<uint64_t>::max)() - now
               ? (std::numeric_limits<uint64_t>::max)()
               : now + timeout_ms;
}

}  // namespace

NodeRuntime::NodeRuntime(NodeConfig cfg, Telemetry& telemetry)
    : cfg_(std::move(cfg)), telemetry_(telemetry) {}

NodeRuntime::~NodeRuntime() {
    shutdown();
}

NodeAddress NodeRuntime::data_addr() const {
    NodeAddress a;
    a.host = cfg_.data_bind_host;
    a.port = data_port_.load();
    return a;
}

void NodeRuntime::enqueue_job(Job&& job) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    while (queue_.size() >= cfg_.job_queue_capacity && running_.load()) {
        queue_cv_.wait(lock);
    }
    if (!running_.load()) return;
    queue_.push_back(std::move(job));
    queue_cv_.notify_one();
}

NodeRuntime::Job NodeRuntime::dequeue_job() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_cv_.wait(lock, [&] { return !queue_.empty() || !running_.load(); });
    if (queue_.empty()) {
        Job job;
        job.kind = Job::kRecv;  // sentinel; will be dropped
        return job;
    }
    Job job = std::move(queue_.front());
    queue_.pop_front();
    queue_cv_.notify_one();
    return job;
}

bool NodeRuntime::start() {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    if (running_.load() || control_thread_.joinable() || heartbeat_thread_.joinable() ||
        accept_thread_.joinable()) {
        return false;
    }
    for (const auto& worker : worker_threads_) {
        if (worker.joinable()) return false;
    }
    if (!cfg_.node_id.valid()) throw_error(ErrKind::InvalidArgument, "node id must be valid");
    if (cfg_.coord_host.empty() || cfg_.coord_port == 0) {
        throw_error(ErrKind::InvalidArgument, "coordinator address must be valid");
    }
    if (cfg_.host_capacity == 0) {
        throw_error(ErrKind::InvalidArgument, "node host capacity must be nonzero");
    }
    if (!cfg_.store_root.empty() && cfg_.persistent_capacity == 0) {
        throw_error(ErrKind::InvalidArgument, "persistent capacity must be nonzero when a store root is configured");
    }
    if (cfg_.heartbeat_interval_ms == 0 || cfg_.op_timeout_ms == 0 ||
        cfg_.heartbeat_interval_ms > static_cast<uint64_t>((std::numeric_limits<int64_t>::max)()) ||
        cfg_.op_timeout_ms > static_cast<uint64_t>((std::numeric_limits<int64_t>::max)()) ||
        cfg_.transfer_workers == 0 ||
        cfg_.max_pending_recv == 0 || cfg_.job_queue_capacity == 0) {
        throw_error(ErrKind::InvalidArgument, "node worker, queue, heartbeat, and timeout values must be nonzero");
    }

    {
        std::lock_guard<std::mutex> queue_lock(queue_mutex_);
        queue_.clear();
    }
    {
        std::lock_guard<std::mutex> state_lock(mutex_);
        pending_recv_.clear();
    }
    {
        std::lock_guard<std::mutex> socket_lock(active_socket_mutex_);
        active_sockets_.clear();
        active_sockets_.reserve(cfg_.transfer_workers);
    }
    worker_threads_.clear();
    caps_ = NodeCapabilities{};
    data_port_.store(0);
    session_id_.store(0);
    session_epoch_.store(0);

    // Capabilities: runtime discovery, never inferred from marketing names.
    caps_.host_memory = true;
    caps_.max_host_bytes = cfg_.host_capacity;
    if (!cfg_.store_root.empty()) {
        persistent_ = std::make_unique<LocalPersistentBackend>(cfg_.store_root, cfg_.persistent_capacity);
        if (persistent_->available()) {
            caps_.local_persistent = true;
            caps_.max_persistent_bytes = cfg_.persistent_capacity;
            auto recovered = persistent_->recover();
            for (const auto& key : recovered) {
                JsonObject d;
                d.field("id", key.id.to_hex());
                d.field("gen", key.gen.value());
                telemetry_.emit("node.persistent_recovered", d);
            }
        } else {
            persistent_.reset();
            throw_error(ErrKind::Persistence, "configured persistent backend is unavailable");
        }
    }
    if (cfg_.enable_cuda) {
        accelerator_ = std::unique_ptr<AcceleratorBackend>(create_accelerator_backend(cfg_.accelerator_capacity));
        if (accelerator_ && accelerator_->available()) {
            caps_.accelerator = true;
            caps_.cuda = true;
            caps_.max_accelerator_bytes = accelerator_->capacity_bytes();
            JsonObject d;
            d.field("devices", accelerator_->device_count());
            d.field("name", accelerator_->device_name(0));
            telemetry_.emit("node.accelerator_detected", d);
            accelerator_->set_device(0);
        } else {
            accelerator_.reset();
            throw_error(ErrKind::NotSupported, "CUDA was requested but no accelerator backend is available");
        }
    }

    host_ = std::make_unique<HostMemoryBackend>(cfg_.host_capacity);
    objects_.set_host(host_.get());
    objects_.set_persistent(persistent_.get());
    objects_.set_accelerator(accelerator_.get());

    if (!cfg_.telemetry_path.empty()) telemetry_.open(cfg_.telemetry_path);
    telemetry_.set_node_id(cfg_.node_id.value());

    uint16_t bound_port = 0;
    if (!listener_.listen(cfg_.data_bind_host, cfg_.data_port, bound_port)) return false;
    data_port_.store(bound_port);

    running_.store(true);
    try {
        worker_threads_.reserve(cfg_.transfer_workers);
        for (size_t i = 0; i < cfg_.transfer_workers; ++i) {
            worker_threads_.emplace_back([this] { worker_loop(); });
        }
        accept_thread_ = std::thread([this] { accept_loop(); });
        heartbeat_thread_ = std::thread([this] { heartbeat_loop(); });
        control_thread_ = std::thread([this] { control_loop(); });
    } catch (...) {
        request_shutdown();
        if (control_thread_.joinable()) control_thread_.join();
        if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
        if (accept_thread_.joinable()) accept_thread_.join();
        for (auto& worker : worker_threads_) {
            if (worker.joinable()) worker.join();
        }
        worker_threads_.clear();
        listener_.close();
        data_port_.store(0);
        throw;
    }

    JsonObject doc;
    doc.field("id", cfg_.node_id.to_string());
    doc.field("label", cfg_.label);
    doc.field("data_addr", data_addr().to_string());
    doc.field("coordinator", cfg_.coord_host + ":" + std::to_string(cfg_.coord_port));
    telemetry_.emit("node.start", doc);
    return true;
}

void NodeRuntime::run() {
    std::unique_lock<std::mutex> lock(shutdown_mutex_);
    shutdown_cv_.wait(lock, [&] { return !running_.load(); });
}

void NodeRuntime::request_shutdown() {
    running_.store(false);
    // Every waiter tests running_ while holding the mutex paired with its
    // condition variable. Passing through that mutex after the store closes
    // the window in which a waiter has evaluated its predicate but has not
    // blocked yet; without it the notification is lost, the waiter sleeps
    // forever (the worker queue wait is unbounded), and shutdown() never
    // returns from joining it.
    { std::lock_guard<std::mutex> lock(queue_mutex_); }
    queue_cv_.notify_all();
    { std::lock_guard<std::mutex> lock(mutex_); }
    pending_cv_.notify_all();
    { std::lock_guard<std::mutex> lock(shutdown_mutex_); }
    shutdown_cv_.notify_all();
    // Wakes threads blocked in recv/send on loopback sockets (closing the
    // handle from another thread does not reliably unblock them on Windows).
    {
        std::lock_guard<std::mutex> ctrl_lock(ctrl_socket_lifecycle_mutex_);
        ctrl_sock_.shutdown_both();
    }
    listener_.shutdown_both();
    std::lock_guard<std::mutex> socket_lock(active_socket_mutex_);
    for (Socket* socket : active_sockets_) socket->shutdown_both();
}

void NodeRuntime::shutdown() {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    request_shutdown();
    if (control_thread_.joinable()) {
        control_thread_.join();
    }
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    for (auto& t : worker_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    worker_threads_.clear();
    {
        std::lock_guard<std::mutex> ctrl_lock(ctrl_socket_lifecycle_mutex_);
        ctrl_sock_.close();
    }
    listener_.close();
    data_port_.store(0);
    session_id_.store(0);
    session_epoch_.store(0);
    {
        std::lock_guard<std::mutex> queue_lock(queue_mutex_);
        queue_.clear();
    }
    {
        std::lock_guard<std::mutex> state_lock(mutex_);
        pending_recv_.clear();
    }
    try {
        objects_.reset();
    } catch (const CfError& e) {
        JsonObject doc;
        doc.field("error", e.what());
        telemetry_.emit("node.object_store_shutdown_error", doc);
    }
    if (host_) host_->shutdown();
    if (persistent_) persistent_->shutdown();
    if (accelerator_) accelerator_->shutdown();
    telemetry_.close();
}

void NodeRuntime::send_data_result(uint64_t op_token, const ObjectId& id, const Generation& gen,
                                   const RpcResult& result, uint64_t bytes, uint64_t duration_ms, uint64_t crc,
                                   uint64_t sha) {
    DataResultMsg msg;
    msg.op_token = op_token;
    msg.object_id = id;
    msg.generation = gen;
    msg.result = result;
    msg.bytes_moved = bytes;
    msg.duration_ms = duration_ms;
    msg.crc32c = crc;
    msg.sha = sha;
    Writer w;
    serialize_data_result(w, msg);
    std::lock_guard<std::mutex> lock(ctrl_send_mutex_);
    try {
        send_frame(ctrl_sock_, kMsgDataResult, req_seq_.fetch_add(1), cfg_.node_id.value(), w.data(), 30000);
    } catch (const CfError&) {
    }
}

bool NodeRuntime::register_with_coordinator() {
    std::lock_guard<std::mutex> send_lock(ctrl_send_mutex_);
    {
        std::lock_guard<std::mutex> ctrl_lock(ctrl_socket_lifecycle_mutex_);
        if (!running_.load()) return false;
        ctrl_sock_.close();
        if (!ctrl_sock_.connect(cfg_.coord_host, cfg_.coord_port, 5000)) {
            return false;
        }
    }
    if (!running_.load()) return false;
    // Hello / version negotiation.
    HelloReq hello;
    hello.protocol_version = kProtocolVersion;
    hello.client_name = "context-fabric-node/" + cfg_.label;
    Writer w;
    serialize_hello(w, hello);
    try {
        const uint64_t hello_request = req_seq_.fetch_add(1);
        send_frame(ctrl_sock_, kMsgHello, hello_request, cfg_.node_id.value(), w.data(), 10000);
        std::vector<uint8_t> payload;
        FrameHeader hdr = recv_frame(ctrl_sock_, payload, 10000);
        if (hdr.type != kMsgHelloResp || hdr.request_id != hello_request) return false;
        Reader r(payload.data(), payload.size());
        HelloResp resp = deserialize_hello_resp(r);
        if (!r.at_end() || resp.protocol_version != kProtocolVersion || resp.coordinator_epoch == 0 ||
            resp.coordinator_id.empty()) {
            return false;
        }
        epoch_.store(resp.coordinator_epoch);

        NodeRegisterReq reg;
        reg.node_id = cfg_.node_id.value();
        reg.label = cfg_.label;
        reg.addr = data_addr();
        reg.capabilities = capabilities_to_bits(caps_);
        reg.max_host_bytes = caps_.max_host_bytes;
        reg.max_persistent_bytes = caps_.max_persistent_bytes;
        reg.max_accelerator_bytes = caps_.max_accelerator_bytes;
        Writer w2;
        serialize_node_register(w2, reg);
        const uint64_t register_request = req_seq_.fetch_add(1);
        send_frame(ctrl_sock_, kMsgNodeRegister, register_request, cfg_.node_id.value(), w2.data(), 10000);
        payload.clear();
        hdr = recv_frame(ctrl_sock_, payload, 10000);
        if (hdr.type != kMsgNodeRegisterResp || hdr.request_id != register_request) return false;
        Reader r2(payload.data(), payload.size());
        NodeRegisterResp resp2 = deserialize_node_register_resp(r2);
        if (!r2.at_end() || resp2.result.failed() || resp2.session_id == 0 || resp2.session_epoch == 0 ||
            resp2.coordinator_epoch == 0) {
            return false;
        }
        session_id_.store(resp2.session_id);
        session_epoch_.store(resp2.session_epoch);
        epoch_.store(resp2.coordinator_epoch);
        telemetry_.set_coordinator_epoch(epoch_.load());
        return true;
    } catch (const CfError&) {
        std::lock_guard<std::mutex> ctrl_lock(ctrl_socket_lifecycle_mutex_);
        ctrl_sock_.close();
        return false;
    }
}

void NodeRuntime::control_loop() {
    while (running_.load()) {
        if (!register_with_coordinator()) {
            JsonObject d;
            d.field("coordinator", cfg_.coord_host + ":" + std::to_string(cfg_.coord_port));
            telemetry_.emit("node.register_retry", d);
            std::unique_lock<std::mutex> shutdown_lock(shutdown_mutex_);
            shutdown_cv_.wait_for(shutdown_lock, std::chrono::milliseconds(1000),
                                  [&] { return !running_.load(); });
            continue;
        }
        JsonObject d;
        d.field("epoch", epoch_.load());
        telemetry_.emit("node.registered", d);

        // Main read loop.
        bool reconnect = false;
        while (running_.load() && !reconnect) {
            std::vector<uint8_t> payload;
            FrameHeader hdr;
            try {
                hdr = recv_frame(ctrl_sock_, payload, 30000);
            } catch (const CfError& e) {
                if (e.kind() == ErrKind::Timeout) continue;
                reconnect = true;
                break;
            }
            try {
                handle_instruction(hdr.type, payload);
            } catch (const CfError& e) {
                JsonObject de;
                de.field("error", e.what());
                telemetry_.emit("node.instruction_error", de);
            } catch (const std::exception& e) {
                JsonObject de;
                de.field("error", e.what());
                telemetry_.emit("node.instruction_error_unexpected", de);
            }
        }
        if (reconnect && running_.load()) {
            JsonObject de;
            de.field("message", "coordinator connection lost; reconnecting");
            telemetry_.emit("node.reconnecting", de);
        }
    }
}

void NodeRuntime::heartbeat_loop() {
    while (running_.load()) {
        {
            std::unique_lock<std::mutex> shutdown_lock(shutdown_mutex_);
            if (shutdown_cv_.wait_for(shutdown_lock, std::chrono::milliseconds(cfg_.heartbeat_interval_ms),
                                      [&] { return !running_.load(); })) {
                break;
            }
        }
        NodeHeartbeatReq hb;
        hb.node_id = cfg_.node_id.value();
        hb.session_id = session_id_.load();
        hb.session_epoch = session_epoch_.load();
        Writer w;
        serialize_node_heartbeat(w, hb);
        std::lock_guard<std::mutex> lock(ctrl_send_mutex_);
        if (!ctrl_sock_.valid()) continue;
        try {
            send_frame(ctrl_sock_, kMsgNodeHeartbeat, req_seq_.fetch_add(1), cfg_.node_id.value(), w.data(), 3000);
        } catch (const CfError&) {
        }
    }
}

void NodeRuntime::accept_loop() {
    while (running_.load()) {
        Socket s = listener_.accept(200);
        if (!s.valid()) continue;
        Job job;
        job.kind = Job::kRecv;
        job.sock = std::move(s);
        enqueue_job(std::move(job));
    }
}

void NodeRuntime::worker_loop() {
    while (running_.load()) {
        Job job = dequeue_job();
        if (!running_.load()) break;
        try {
            switch (job.kind) {
                case Job::kSend:
                    do_send(job.send);
                    break;
                case Job::kRecv:
                    do_recv(std::move(job.sock));
                    break;
                case Job::kVerify:
                    do_verify(job.verify);
                    break;
            }
        } catch (const CfError& e) {
            JsonObject d;
            d.field("error", e.what());
            telemetry_.emit("node.worker_error", d);
        } catch (const std::exception& e) {
            JsonObject d;
            d.field("error", e.what());
            telemetry_.emit("node.worker_error_unexpected", d);
        }
    }
}

void NodeRuntime::handle_instruction(MessageType type, const std::vector<uint8_t>& payload) {
    Reader r(payload.data(), payload.size());
    switch (type) {
        case kMsgInstructSend: {
            InstructSendReq instr = deserialize_instruct_send(r);
            Job job;
            job.kind = Job::kSend;
            job.send = instr;
            enqueue_job(std::move(job));
            break;
        }
        case kMsgInstructRecv: {
            InstructRecvReq instr = deserialize_instruct_recv(r);
            handle_instruct_recv(instr);
            break;
        }
        case kMsgInstructVerify: {
            InstructVerifyReq instr = deserialize_instruct_verify(r);
            Job job;
            job.kind = Job::kVerify;
            job.verify = instr;
            enqueue_job(std::move(job));
            break;
        }
        case kMsgInstructAbort: {
            InstructAbortReq instr = deserialize_instruct_abort(r);
            handle_instruct_abort(instr);
            break;
        }
        case kMsgInstructPromote: {
            InstructPromoteReq instr = deserialize_instruct_promote(r);
            objects_.set_role(instr.object_id, instr.generation, CopyRole::Authoritative);
            JsonObject d;
            d.field("id", instr.object_id.to_hex());
            telemetry_.emit("node.role_promoted", d);
            break;
        }
        case kMsgInstructDispose: {
            InstructDisposeReq instr = deserialize_instruct_dispose(r);
            handle_instruct_dispose(instr);
            break;
        }
        case kMsgInstructShutdown: {
            JsonObject d;
            d.field("message", "coordinator requested shutdown");
            telemetry_.emit("node.shutdown_requested", d);
            request_shutdown();
            break;
        }
        case kMsgInstructResp:
            (void)deserialize_instruct_resp(r);
            break;
        case kMsgDataResultResp:
            (void)deserialize_data_result_resp(r);
            break;
        case kMsgNodeHeartbeatResp:
            (void)deserialize_node_heartbeat_resp(r);
            break;
        default:
            throw_error(ErrKind::Protocol, "unexpected message on control connection");
    }
    if (!r.at_end()) throw_error(ErrKind::Protocol, "trailing bytes after control instruction");
}

void NodeRuntime::handle_instruct_recv(const InstructRecvReq& instr) {
    if (instr.op_token == 0 || !instr.object_id.valid() || !instr.generation.valid() ||
        instr.byte_size == 0 || instr.metadata.byte_size != instr.byte_size || instr.self_load > 1) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Protocol, "invalid receive instruction identity or size"),
                         0, 0, 0, 0);
        return;
    }
    const CopyRole role = static_cast<CopyRole>(instr.role);
    const StorageDomain domain = static_cast<StorageDomain>(instr.domain);
    if ((role != CopyRole::Authoritative && role != CopyRole::Replica && role != CopyRole::Persistent) ||
        (domain != StorageDomain::HostMemory && domain != StorageDomain::LocalPersistent &&
         domain != StorageDomain::AcceleratorLocal)) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Protocol, "unsupported receive role or domain"),
                         0, 0, 0, 0);
        return;
    }
    if (role == CopyRole::Persistent) {
        if (domain != StorageDomain::LocalPersistent) {
            send_data_result(instr.op_token, instr.object_id, instr.generation,
                             RpcResult::failure(ErrKind::Protocol, "persistent role requires persistent domain"),
                             0, 0, 0, 0);
            return;
        }
        if (instr.self_load != 0) {
            send_data_result(instr.op_token, instr.object_id, instr.generation,
                             RpcResult::failure(ErrKind::Protocol,
                                                "persistent destination cannot use restore self-load"),
                             0, 0, 0, 0);
            return;
        }
        if (instr.source_node == cfg_.node_id.value()) {
            do_self_persist(instr);
            return;
        }
    }
    if (instr.self_load != 0) {
        if (domain != StorageDomain::HostMemory || role != CopyRole::Authoritative) {
            send_data_result(instr.op_token, instr.object_id, instr.generation,
                             RpcResult::failure(ErrKind::Protocol, "invalid restore instruction"),
                             0, 0, 0, 0);
            return;
        }
        do_self_restore(instr);
        return;
    }

    bool rejected = false;
    std::string rejection;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const uint64_t now = steady_now_ms();
        for (auto it = pending_recv_.begin(); it != pending_recv_.end();) {
            if (now >= it->second.created_ms && now - it->second.created_ms >= cfg_.op_timeout_ms) {
                it = pending_recv_.erase(it);
            } else {
                ++it;
            }
        }
        if (pending_recv_.find(instr.op_token) != pending_recv_.end()) {
            rejected = true;
            rejection = "duplicate pending receive token";
        } else if (pending_recv_.size() >= cfg_.max_pending_recv) {
            rejected = true;
            rejection = "too many pending receives";
        } else {
            PendingRecv pr;
            pr.instr = instr;
            pr.created_ms = now;
            pending_recv_.emplace(instr.op_token, std::move(pr));
        }
    }
    if (rejected) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Capacity, rejection), 0, 0, 0, 0);
        return;
    }
    pending_cv_.notify_all();
}

void NodeRuntime::handle_instruct_abort(const InstructAbortReq& instr) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_recv_.erase(instr.op_token);
    }
    pending_cv_.notify_all();
}

void NodeRuntime::handle_instruct_dispose(const InstructDisposeReq& instr) {
    if (instr.op_token == 0 || !instr.object_id.valid() || !instr.generation.valid() ||
        instr.keep_as_replica > 1) {
        throw_error(ErrKind::Protocol, "invalid dispose instruction");
    }
    if (instr.keep_as_replica) {
        objects_.set_role(instr.object_id, instr.generation, CopyRole::Replica);
        JsonObject d;
        d.field("id", instr.object_id.to_hex());
        d.field("gen", instr.generation.value());
        telemetry_.emit("node.copy_demoted", d);
    } else {
        objects_.remove(instr.object_id, instr.generation);
        // Persistent records recovered after a node restart predate the
        // in-memory object-store index. Remove any such catalogued record as
        // well; the call is a no-op when objects_.remove already released it.
        if (persistent_ && persistent_->contains(instr.object_id, instr.generation)) {
            persistent_->remove(instr.object_id, instr.generation);
        }
        JsonObject d;
        d.field("id", instr.object_id.to_hex());
        d.field("gen", instr.generation.value());
        telemetry_.emit("node.copy_disposed", d);
    }
}

void NodeRuntime::do_send(const InstructSendReq& instr) {
    uint64_t t0 = steady_now_ms();
    const uint64_t deadline = operation_deadline_ms(cfg_.op_timeout_ms);
    if (instr.op_token == 0 || !instr.object_id.valid() || !instr.generation.valid() ||
        !instr.dest_addr.valid() || instr.byte_size == 0) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Protocol, "invalid send instruction"), 0, 0, 0, 0);
        return;
    }

    std::vector<uint8_t> bytes;
    try {
        bytes = objects_.read(instr.object_id, instr.generation);
    } catch (const CfError& e) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(e.kind(), e.what()), 0, steady_now_ms() - t0, 0, 0);
        return;
    } catch (const std::bad_alloc&) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Capacity, "cannot stage source bytes"), 0, 0, 0, 0);
        return;
    }
    if (bytes.size() != instr.byte_size) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Integrity, "source byte size mismatch"), 0, 0, 0, 0);
        return;
    }
    const IntegrityId source_integrity = compute_integrity(bytes.data(), bytes.size());
    if (source_integrity.crc32c != instr.crc32c || source_integrity.sha != instr.sha) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Integrity, "source checksum mismatch"), 0, 0, 0, 0);
        return;
    }

    Socket peer;
    if (!peer.connect(instr.dest_addr.host, instr.dest_addr.port,
                      (std::min)(uint64_t{10000}, remaining_timeout_ms(deadline)))) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Transport, "cannot connect to destination"), 0, 0, 0, 0);
        return;
    }
    ActiveSocketGuard active(active_socket_mutex_, active_sockets_, peer);

    const size_t total = bytes.empty() ? 1 : 1 + (bytes.size() - 1) / static_cast<size_t>(chunk_size_);
    bool failed = false;
    ErrKind fail_kind = ErrKind::Transport;
    std::string fail_reason;
    for (size_t seq = 0; seq < total && !failed; ++seq) {
        if (!running_.load()) {
            failed = true;
            fail_kind = ErrKind::Shutdown;
            fail_reason = "node is shutting down";
            break;
        }
        size_t off = seq * chunk_size_;
        size_t len = (std::min)(chunk_size_, bytes.size() - off);
        DataBlockMsg block;
        block.op_token = instr.op_token;
        block.object_id = instr.object_id;
        block.generation = instr.generation;
        block.seq = static_cast<uint64_t>(seq);
        block.total_chunks = static_cast<uint64_t>(total);
        block.byte_size = bytes.size();
        block.crc32c = instr.crc32c;
        block.sha = instr.sha;
        block.data.assign(bytes.begin() + static_cast<std::ptrdiff_t>(off),
                          bytes.begin() + static_cast<std::ptrdiff_t>(off + len));

        Writer w;
        serialize_data_block(w, block);
        try {
            send_frame(peer, kMsgDataBlock, req_seq_.fetch_add(1), cfg_.node_id.value(), w.data(),
                       remaining_timeout_ms(deadline));
            std::vector<uint8_t> ack_payload;
            FrameHeader ack_hdr = recv_frame(peer, ack_payload, remaining_timeout_ms(deadline));
            if (ack_hdr.type != kMsgDataBlockAck) {
                failed = true;
                fail_kind = ErrKind::Protocol;
                fail_reason = "unexpected ack type";
                break;
            }
            Reader ar(ack_payload.data(), ack_payload.size());
            DataBlockAckMsg ack = deserialize_data_block_ack(ar);
            if (!ar.at_end() || ack.op_token != instr.op_token || ack.object_id != instr.object_id ||
                ack.generation != instr.generation || ack.seq != seq) {
                failed = true;
                fail_kind = ErrKind::Protocol;
                fail_reason = "data block acknowledgement identity mismatch";
                break;
            }
            if (ack.result.failed()) {
                failed = true;
                fail_kind = ack.result.err;
                fail_reason = ack.result.message;
                break;
            }
        } catch (const CfError& e) {
            failed = true;
            fail_kind = e.kind();
            fail_reason = e.what();
            break;
        }
    }
    peer.shutdown_send();

    uint64_t duration = steady_now_ms() - t0;
    if (failed) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(fail_kind, fail_reason), 0, duration, 0, 0);
    } else {
        send_data_result(instr.op_token, instr.object_id, instr.generation, RpcResult::success(), bytes.size(),
                         duration, instr.crc32c, instr.sha);
    }
}

void NodeRuntime::do_recv(Socket sock) {
    ActiveSocketGuard active(active_socket_mutex_, active_sockets_, sock);
    const uint64_t started = steady_now_ms();
    const uint64_t deadline = operation_deadline_ms(cfg_.op_timeout_ms);
    const uint64_t authorization_deadline = (std::min)(deadline, operation_deadline_ms(5000));
    constexpr uint32_t kSerializedBlockOverhead = 76;
    const uint32_t block_payload_cap = static_cast<uint32_t>(chunk_size_) + kSerializedBlockOverhead;

    std::vector<uint8_t> payload;
    FrameHeader hdr;
    DataBlockMsg first;
    try {
        hdr = recv_frame(sock, payload, remaining_timeout_ms(deadline), block_payload_cap);
        if (hdr.type != kMsgDataBlock) throw_error(ErrKind::Protocol, "expected initial data block");
        Reader r(payload.data(), payload.size());
        first = deserialize_data_block(r);
        if (!r.at_end()) throw_error(ErrKind::Protocol, "trailing bytes after data block");
    } catch (const CfError&) {
        return;  // No authenticated operation identity is available yet.
    }

    // The sender can race ahead of the coordinator instruction. Wait on a
    // condition variable so abort and shutdown wake this worker immediately.
    InstructRecvReq instr;
    bool found = false;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        while (running_.load()) {
            auto it = pending_recv_.find(first.op_token);
            if (it != pending_recv_.end()) {
                instr = it->second.instr;
                found = true;
                break;
            }
            const uint64_t now = steady_now_ms();
            if (now >= authorization_deadline) break;
            pending_cv_.wait_for(lock, std::chrono::milliseconds(authorization_deadline - now));
        }
    }
    if (!found) return;

    const CopyRole role = static_cast<CopyRole>(instr.role);
    const StorageDomain domain = static_cast<StorageDomain>(instr.domain);
    const uint64_t needed = instr.byte_size;
    const uint64_t total_chunks = 1 + (needed - 1) / chunk_size_;
    uint64_t authorized_crc = instr.crc32c;
    uint64_t authorized_sha = instr.sha;
    const bool client_upload = instr.source_node == 0 && authorized_crc == 0 && authorized_sha == 0;
    bool missing_upload_integrity = false;
    if (client_upload) {
        // Creation/write uploads originate outside the node set, so the
        // coordinator does not know their content identity in advance. Bind
        // the operation to block zero, then require every later block and the
        // final streamed digest to match it exactly.
        authorized_crc = first.crc32c;
        authorized_sha = first.sha;
        missing_upload_integrity = authorized_sha == 0;
    }

    auto report = [&](const RpcResult& result, uint64_t bytes, uint64_t crc, uint64_t sha) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_recv_.erase(instr.op_token);
        }
        pending_cv_.notify_all();
        send_data_result(instr.op_token, instr.object_id, instr.generation, result, bytes,
                         steady_now_ms() - started, crc, sha);
    };
    auto reject = [&](ErrKind kind, const std::string& message) {
        report(RpcResult::failure(kind, message), 0, 0, 0);
    };
    if (missing_upload_integrity) {
        reject(ErrKind::Protocol, "client upload did not provide an integrity fingerprint");
        return;
    }
    auto pending = [&] {
        std::lock_guard<std::mutex> lock(mutex_);
        return pending_recv_.find(instr.op_token) != pending_recv_.end();
    };
    auto validate_block = [&](const DataBlockMsg& block, uint64_t seq) {
        if (block.op_token != instr.op_token || block.object_id != instr.object_id ||
            block.generation != instr.generation || block.seq != seq ||
            block.total_chunks != total_chunks || block.byte_size != needed ||
            block.crc32c != authorized_crc || block.sha != authorized_sha) {
            throw_error(ErrKind::Protocol, "data block does not match authorized transfer");
        }
        const uint64_t offset = seq * chunk_size_;
        const uint64_t expected_size = (std::min)(chunk_size_, needed - offset);
        if (block.data.size() != expected_size) {
            throw_error(ErrKind::Protocol, "data block has invalid chunk size");
        }
    };
    auto send_ack = [&](uint64_t seq, const RpcResult& result) {
        DataBlockAckMsg ack;
        ack.op_token = instr.op_token;
        ack.object_id = instr.object_id;
        ack.generation = instr.generation;
        ack.seq = seq;
        ack.result = result;
        Writer w;
        serialize_data_block_ack(w, ack);
        send_frame(sock, kMsgDataBlockAck, req_seq_.fetch_add(1), cfg_.node_id.value(), w.data(),
                   remaining_timeout_ms(deadline));
    };

    try {
        validate_block(first, 0);
    } catch (const CfError& e) {
        reject(e.kind(), e.what());
        return;
    }

    // Subtraction-based checks cannot wrap. Backends repeat the authoritative
    // capacity check when committing, closing the race with concurrent stores.
    StorageBackend* destination = nullptr;
    if (domain == StorageDomain::LocalPersistent) {
        destination = persistent_.get();
    } else if (domain == StorageDomain::HostMemory) {
        destination = host_.get();
    }
    if (domain == StorageDomain::AcceleratorLocal) {
        if (!accelerator_ || !accelerator_->available()) {
            reject(ErrKind::NotSupported, "destination accelerator unavailable");
            return;
        }
        const uint64_t used = accelerator_->used_bytes();
        const uint64_t capacity = accelerator_->capacity_bytes();
        if (used > capacity || needed > capacity - used) {
            reject(ErrKind::Capacity, "destination accelerator capacity exhausted");
            return;
        }
    } else {
        if (destination == nullptr || !destination->available()) {
            reject(ErrKind::NotSupported, "destination storage domain unavailable");
            return;
        }
        const uint64_t used = destination->used_bytes();
        const uint64_t capacity = destination->capacity_bytes();
        if (used > capacity || needed > capacity - used) {
            reject(ErrKind::Capacity, "destination storage capacity exhausted");
            return;
        }
    }

    std::vector<uint8_t> buffer;
    try {
        if (needed > static_cast<uint64_t>((std::numeric_limits<size_t>::max)())) {
            throw_error(ErrKind::Capacity, "transfer exceeds addressable memory");
        }
        buffer.reserve(static_cast<size_t>(needed));
    } catch (const std::bad_alloc&) {
        reject(ErrKind::Capacity, "cannot allocate transfer staging buffer");
        return;
    } catch (const CfError& e) {
        reject(e.kind(), e.what());
        return;
    }

    IntegrityBuilder integrity;
    ErrKind fail_kind = ErrKind::Internal;
    std::string fail_reason;
    try {
        for (uint64_t seq = 0; seq < total_chunks; ++seq) {
            if (!running_.load()) throw_error(ErrKind::Shutdown, "node is shutting down");
            if (!pending()) throw_error(ErrKind::Shutdown, "transfer was aborted");

            DataBlockMsg block;
            if (seq == 0) {
                block = std::move(first);
            } else {
                std::vector<uint8_t> next_payload;
                FrameHeader next_header = recv_frame(sock, next_payload, remaining_timeout_ms(deadline),
                                                     block_payload_cap);
                if (next_header.type != kMsgDataBlock) {
                    throw_error(ErrKind::Protocol, "expected data block");
                }
                Reader reader(next_payload.data(), next_payload.size());
                block = deserialize_data_block(reader);
                if (!reader.at_end()) throw_error(ErrKind::Protocol, "trailing bytes after data block");
                validate_block(block, seq);
            }

            integrity.update(block.data.data(), block.data.size());
            buffer.insert(buffer.end(), block.data.begin(), block.data.end());
            send_ack(seq, RpcResult::success());
        }
    } catch (const CfError& e) {
        fail_kind = e.kind();
        fail_reason = e.what();
    } catch (const std::bad_alloc&) {
        fail_kind = ErrKind::Capacity;
        fail_reason = "transfer staging buffer exhausted";
    }
    if (!fail_reason.empty()) {
        reject(fail_kind, fail_reason);
        return;
    }

    if (buffer.size() != needed) {
        reject(ErrKind::Protocol, "transfer byte count mismatch");
        return;
    }
    const IntegrityId received = integrity.finish();
    if (received.crc32c != authorized_crc || received.sha != authorized_sha || received.bytes != needed) {
        reject(ErrKind::Integrity, "checksum mismatch at destination");
        return;
    }
    if (!running_.load() || !pending()) {
        reject(ErrKind::Shutdown, "transfer was cancelled before commit");
        return;
    }
    try {
        (void)remaining_timeout_ms(deadline);
    } catch (const CfError& e) {
        reject(e.kind(), e.what());
        return;
    }

    try {
        if (domain == StorageDomain::AcceleratorLocal) {
            uint8_t* device = nullptr;
            try {
                accelerator_->allocate(device, buffer.size());
                accelerator_->copy_h2d(device, buffer.data(), buffer.size());
                accelerator_->synchronize();
                accelerator_->verify(device, buffer.size(), received.crc32c, received.sha);
                objects_.retain_accelerator(instr.object_id, instr.generation, role, device,
                                            buffer.size(), received);
                device = nullptr;  // ownership transferred to the object store
            } catch (...) {
                if (device != nullptr) accelerator_->free(device);
                throw;
            }
        } else {
            objects_.store(instr.object_id, instr.generation, role, domain, &instr.metadata,
                           buffer.data(), buffer.size());
        }
    } catch (const CfError& e) {
        reject(e.kind(), e.what());
        return;
    } catch (const std::bad_alloc&) {
        reject(ErrKind::Capacity, "destination storage allocation failed");
        return;
    }

    report(RpcResult::success(), buffer.size(), received.crc32c, received.sha);
}

void NodeRuntime::do_verify(const InstructVerifyReq& instr) {
    uint64_t t0 = steady_now_ms();
    if (instr.op_token == 0 || !instr.object_id.valid() || !instr.generation.valid() ||
        instr.byte_size == 0 || instr.deep > 1) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Protocol, "invalid verify instruction"),
                         0, 0, 0, 0);
        return;
    }
    try {
        objects_.verify(instr.object_id, instr.generation, instr.crc32c, instr.sha,
                        instr.deep != 0, instr.byte_size);
        send_data_result(instr.op_token, instr.object_id, instr.generation, RpcResult::success(),
                         instr.byte_size, steady_now_ms() - t0, instr.crc32c, instr.sha);
    } catch (const CfError& e) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(e.kind(), e.what()), 0, steady_now_ms() - t0, 0, 0);
    } catch (const std::bad_alloc&) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Capacity, "verification staging allocation failed"),
                         0, steady_now_ms() - t0, 0, 0);
    }
}

void NodeRuntime::do_self_persist(const InstructRecvReq& instr) {
    uint64_t t0 = steady_now_ms();
    try {
        std::vector<uint8_t> bytes = objects_.read(instr.object_id, instr.generation);
        if (bytes.size() != instr.byte_size) {
            throw_error(ErrKind::Integrity, "self-persist source byte size mismatch");
        }
        IntegrityId idg = compute_integrity(bytes.data(), bytes.size());
        if (idg.crc32c != instr.crc32c || idg.sha != instr.sha) {
            throw_error(ErrKind::Integrity, "self-persist source checksum mismatch");
        }
        objects_.store(instr.object_id, instr.generation, CopyRole::Persistent, StorageDomain::LocalPersistent,
                       &instr.metadata, bytes.data(), bytes.size());
        uint64_t duration = steady_now_ms() - t0;
        send_data_result(instr.op_token, instr.object_id, instr.generation, RpcResult::success(), bytes.size(),
                         duration, idg.crc32c, idg.sha);
    } catch (const CfError& e) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(e.kind(), e.what()), 0, steady_now_ms() - t0, 0, 0);
    } catch (const std::bad_alloc&) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Capacity, "self-persist staging allocation failed"),
                         0, steady_now_ms() - t0, 0, 0);
    }
}

void NodeRuntime::do_self_restore(const InstructRecvReq& instr) {
    uint64_t t0 = steady_now_ms();
    try {
        if (!persistent_) throw_error(ErrKind::NotSupported, "no persistent backend");
        std::vector<uint8_t> bytes = persistent_->get(instr.object_id, instr.generation);
        if (bytes.size() != instr.byte_size) {
            throw_error(ErrKind::Integrity, "restore source byte size mismatch");
        }
        IntegrityId idg = compute_integrity(bytes.data(), bytes.size());
        if (idg.crc32c != instr.crc32c || idg.sha != instr.sha) {
            throw_error(ErrKind::Integrity, "restore source checksum mismatch");
        }
        objects_.store(instr.object_id, instr.generation, CopyRole::Authoritative, StorageDomain::HostMemory,
                       &instr.metadata, bytes.data(), bytes.size());
        uint64_t duration = steady_now_ms() - t0;
        send_data_result(instr.op_token, instr.object_id, instr.generation, RpcResult::success(), bytes.size(),
                         duration, idg.crc32c, idg.sha);
    } catch (const CfError& e) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(e.kind(), e.what()), 0, steady_now_ms() - t0, 0, 0);
    } catch (const std::bad_alloc&) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Capacity, "restore staging allocation failed"),
                         0, steady_now_ms() - t0, 0, 0);
    }
}

}  // namespace ctf
