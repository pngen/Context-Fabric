#include "context_fabric/runtime/node_runtime.h"

#include <algorithm>
#include <thread>
#include <cstdio>
#include <filesystem>

#include "context_fabric/core/clock.h"
#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/core/version.h"

namespace ctf {

namespace fs = std::filesystem;

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
    while (queue_size_ >= cfg_.job_queue_capacity && running_.load()) {
        queue_cv_.wait(lock);
    }
    if (!running_.load()) return;
    queue_.push_back(std::move(job));
    ++queue_size_;
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
    --queue_size_;
    queue_cv_.notify_one();
    return job;
}

bool NodeRuntime::start() {
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
        }
    }
    if (cfg_.enable_cuda) {
        accelerator_ = std::unique_ptr<AcceleratorBackend>(create_accelerator_backend(cfg_.host_capacity));
        if (accelerator_ && accelerator_->available()) {
            caps_.accelerator = true;
            caps_.cuda = true;
            caps_.max_accelerator_bytes = cfg_.accelerator_capacity;
            JsonObject d;
            d.field("devices", accelerator_->device_count());
            d.field("name", accelerator_->device_name(0));
            telemetry_.emit("node.accelerator_detected", d);
            accelerator_->set_device(0);
        } else {
            accelerator_.reset();
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
    worker_threads_.reserve(cfg_.transfer_workers);
    for (size_t i = 0; i < cfg_.transfer_workers; ++i) {
        worker_threads_.emplace_back([this] { worker_loop(); });
    }
    accept_thread_ = std::thread([this] { accept_loop(); });
    heartbeat_thread_ = std::thread([this] { heartbeat_loop(); });
    control_thread_ = std::thread([this] { control_loop(); });

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
    queue_cv_.notify_all();
    shutdown_cv_.notify_all();
    // Wakes threads blocked in recv/send on loopback sockets (closing the
    // handle from another thread does not reliably unblock them on Windows).
    ctrl_sock_.shutdown_both();
    listener_.shutdown_both();
}

void NodeRuntime::shutdown() {
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
        if (t.joinable()) t.join();
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
    ctrl_sock_.close();
    if (!ctrl_sock_.connect(cfg_.coord_host, cfg_.coord_port, 5000)) {
        return false;
    }
    // Hello / version negotiation.
    HelloReq hello;
    hello.protocol_version = kProtocolVersion;
    hello.client_name = "context-fabric-node/" + cfg_.label;
    Writer w;
    serialize_hello(w, hello);
    try {
        send_frame(ctrl_sock_, kMsgHello, req_seq_.fetch_add(1), cfg_.node_id.value(), w.data(), 10000);
        std::vector<uint8_t> payload;
        FrameHeader hdr = recv_frame(ctrl_sock_, payload, 10000);
        if (hdr.type != kMsgHelloResp) return false;
        Reader r(payload.data(), payload.size());
        HelloResp resp = deserialize_hello_resp(r);
        if (resp.protocol_version != kProtocolVersion) return false;
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
        send_frame(ctrl_sock_, kMsgNodeRegister, req_seq_.fetch_add(1), cfg_.node_id.value(), w2.data(), 10000);
        payload.clear();
        hdr = recv_frame(ctrl_sock_, payload, 10000);
        if (hdr.type != kMsgNodeRegisterResp) return false;
        Reader r2(payload.data(), payload.size());
        NodeRegisterResp resp2 = deserialize_node_register_resp(r2);
        if (resp2.result.failed()) return false;
        session_id_ = resp2.session_id;
        session_epoch_ = resp2.session_epoch;
        epoch_.store(resp2.coordinator_epoch);
        telemetry_.set_coordinator_epoch(epoch_.load());
        return true;
    } catch (const CfError&) {
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
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
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
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg_.heartbeat_interval_ms));
        if (!running_.load()) break;
        if (!ctrl_sock_.valid()) continue;
        NodeHeartbeatReq hb;
        hb.node_id = cfg_.node_id.value();
        hb.session_id = session_id_;
        hb.session_epoch = session_epoch_;
        Writer w;
        serialize_node_heartbeat(w, hb);
        std::lock_guard<std::mutex> lock(ctrl_send_mutex_);
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
        case kMsgDataResultResp:
        case kMsgNodeHeartbeatResp:
            break;  // acknowledgements
        default:
            throw_error(ErrKind::Protocol, "unexpected message on control connection");
    }
}

void NodeRuntime::handle_instruct_recv(const InstructRecvReq& instr) {
    if (static_cast<CopyRole>(instr.role) == CopyRole::Persistent) {
        do_self_persist(instr);
        return;
    }
    if (instr.self_load != 0) {
        do_self_restore(instr);
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_recv_.size() >= cfg_.max_pending_recv) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Capacity, "too many pending receives"), 0, 0, 0, 0);
        return;
    }
    PendingRecv pr;
    pr.instr = instr;
    pr.created_ms = now_unix_ms();
    pending_recv_[instr.op_token] = std::move(pr);
}

void NodeRuntime::handle_instruct_abort(const InstructAbortReq& instr) {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_recv_.erase(instr.op_token);
}

void NodeRuntime::handle_instruct_dispose(const InstructDisposeReq& instr) {
    if (instr.keep_as_replica) {
        objects_.set_role(instr.object_id, instr.generation, CopyRole::Replica);
        JsonObject d;
        d.field("id", instr.object_id.to_hex());
        d.field("gen", instr.generation.value());
        telemetry_.emit("node.copy_demoted", d);
    } else {
        objects_.remove(instr.object_id, instr.generation);
        JsonObject d;
        d.field("id", instr.object_id.to_hex());
        d.field("gen", instr.generation.value());
        telemetry_.emit("node.copy_disposed", d);
    }
}

void NodeRuntime::do_send(const InstructSendReq& instr) {
    uint64_t t0 = steady_now_ms();
    std::vector<uint8_t> bytes = objects_.read(instr.object_id, instr.generation);
    if (bytes.size() != instr.byte_size) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Integrity, "source byte size mismatch"), 0, 0, 0, 0);
        return;
    }

    Socket peer;
    if (!peer.connect(instr.dest_addr.host, instr.dest_addr.port, 10000)) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Transport, "cannot connect to destination"), 0, 0, 0, 0);
        return;
    }

    size_t total = (bytes.size() + chunk_size_ - 1) / chunk_size_;
    bool failed = false;
    std::string fail_reason;
    for (size_t seq = 0; seq < total && !failed; ++seq) {
        size_t off = seq * chunk_size_;
        size_t len = std::min(chunk_size_, bytes.size() - off);
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
            send_frame(peer, kMsgDataBlock, req_seq_.fetch_add(1), cfg_.node_id.value(), w.data(), 30000);
            std::vector<uint8_t> ack_payload;
            FrameHeader ack_hdr = recv_frame(peer, ack_payload, 30000);
            if (ack_hdr.type != kMsgDataBlockAck) {
                failed = true;
                fail_reason = "unexpected ack type";
                break;
            }
            Reader ar(ack_payload.data(), ack_payload.size());
            DataBlockAckMsg ack = deserialize_data_block_ack(ar);
            if (ack.result.failed()) {
                failed = true;
                fail_reason = ack.result.message;
                break;
            }
        } catch (const CfError& e) {
            failed = true;
            fail_reason = e.what();
            break;
        }
    }
    peer.shutdown_send();
    peer.close();

    uint64_t duration = steady_now_ms() - t0;
    if (failed) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(ErrKind::Transport, fail_reason), 0, duration, 0, 0);
    } else {
        send_data_result(instr.op_token, instr.object_id, instr.generation, RpcResult::success(), bytes.size(),
                         duration, instr.crc32c, instr.sha);
    }
}

void NodeRuntime::do_recv(Socket sock) {
    auto fail = [&](const std::string& msg) {
        sock.close();
        send_data_result(0, ObjectId(), Generation(), RpcResult::failure(ErrKind::Protocol, msg), 0, 0, 0, 0);
    };

    std::vector<uint8_t> payload;
    FrameHeader hdr;
    try {
        hdr = recv_frame(sock, payload, 15000);
    } catch (const CfError&) {
        sock.close();
        return;
    }
    if (hdr.type != kMsgDataBlock) {
        sock.close();
        return;
    }
    Reader r(payload.data(), payload.size());
    DataBlockMsg first = deserialize_data_block(r);

    // Wait for the pending receive registration (the connection may arrive
    // before the InstructRecv instruction is processed).
    InstructRecvReq instr;
    bool found = false;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        for (int attempt = 0; attempt < 150 && !found; ++attempt) {
            auto it = pending_recv_.find(first.op_token);
            if (it != pending_recv_.end()) {
                instr = it->second.instr;
                found = true;
                break;
            }
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            lock.lock();
        }
    }
    if (!found) {
        fail("no pending receive for op token");
        return;
    }
    if (first.op_token != instr.op_token || first.object_id != instr.object_id ||
        first.generation != instr.generation) {
        fail("transfer identity mismatch");
        return;
    }

    uint64_t t0 = now_unix_ms();
    CopyRole role = static_cast<CopyRole>(instr.role);
    StorageDomain domain = static_cast<StorageDomain>(instr.domain);
    uint64_t needed = instr.byte_size;

    auto send_ack = [&](uint64_t seq, const RpcResult& result) {
        DataBlockAckMsg ack;
        ack.op_token = instr.op_token;
        ack.object_id = instr.object_id;
        ack.generation = instr.generation;
        ack.seq = seq;
        ack.result = result;
        Writer w;
        serialize_data_block_ack(w, ack);
        send_frame(sock, kMsgDataBlockAck, req_seq_.fetch_add(1), cfg_.node_id.value(), w.data(), 30000);
    };
    auto report = [&](const RpcResult& result, uint64_t bytes, uint64_t crc, uint64_t sha) {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_recv_.erase(instr.op_token);
        send_data_result(instr.op_token, instr.object_id, instr.generation, result, bytes,
                         now_unix_ms() - t0, crc, sha);
    };

    // Capacity check before accepting bytes.
    if (domain == StorageDomain::LocalPersistent) {
        if (!persistent_ || !persistent_->available() ||
            persistent_->used_bytes() + needed > persistent_->capacity_bytes()) {
            report(RpcResult::failure(ErrKind::Capacity, "destination persistent capacity exhausted"), 0, 0, 0);
            return;
        }
    } else {
        if (!host_ || host_->used_bytes() + needed > host_->capacity_bytes()) {
            report(RpcResult::failure(ErrKind::Capacity, "destination host capacity exhausted"), 0, 0, 0);
            return;
        }
    }

    std::vector<uint8_t> buffer;
    buffer.reserve(static_cast<size_t>(needed));
    IntegrityBuilder ib;
    uint64_t expected_crc = first.crc32c;
    uint64_t expected_sha = first.sha;
    uint64_t total_chunks = first.total_chunks;
    uint64_t seq = 0;
    bool ok = true;
    std::string fail_reason;

    try {
        // Process the first block.
        ib.update(first.data.data(), first.data.size());
        buffer.insert(buffer.end(), first.data.begin(), first.data.end());
        send_ack(0, RpcResult::success());
        ++seq;

        while (seq < total_chunks) {
            std::vector<uint8_t> p2;
            FrameHeader h2 = recv_frame(sock, p2, 30000);
            if (h2.type != kMsgDataBlock) {
                ok = false;
                fail_reason = "expected data block";
                break;
            }
            Reader r2(p2.data(), p2.size());
            DataBlockMsg b = deserialize_data_block(r2);
            if (b.seq != seq) {
                ok = false;
                fail_reason = "out-of-order or duplicate block";
                send_ack(b.seq, RpcResult::failure(ErrKind::Protocol, fail_reason));
                break;
            }
            ib.update(b.data.data(), b.data.size());
            buffer.insert(buffer.end(), b.data.begin(), b.data.end());
            send_ack(seq, RpcResult::success());
            ++seq;
        }
    } catch (const CfError& e) {
        ok = false;
        fail_reason = e.what();
    }

    if (ok && buffer.size() != needed) {
        ok = false;
        fail_reason = "byte count mismatch";
    }
    IntegrityId received = ib.finish();
    if (ok && (received.crc32c != expected_crc || received.sha != expected_sha)) {
        ok = false;
        fail_reason = "checksum mismatch at destination";
    }
    sock.close();

    if (!ok) {
        report(RpcResult::failure(ErrKind::Integrity, fail_reason), 0, 0, 0);
        return;
    }

    try {
        if (domain == StorageDomain::AcceleratorLocal) {
            ContextMetadata md = instr.metadata;
            objects_.store(instr.object_id, instr.generation, CopyRole::Replica, StorageDomain::HostMemory, &md,
                           buffer.data(), buffer.size());
            if (accelerator_) {
                uint8_t* device = nullptr;
                accelerator_->allocate(device, buffer.size());
                accelerator_->copy_h2d(device, buffer.data(), buffer.size());
                accelerator_->synchronize();
                accelerator_->verify(device, buffer.size(), received.crc32c, received.sha);
                objects_.retain_accelerator(instr.object_id, instr.generation,
                                            role == CopyRole::Authoritative ? CopyRole::Authoritative
                                                                             : CopyRole::Replica,
                                            device, buffer.size());
            }
        } else if (domain == StorageDomain::LocalPersistent && role == CopyRole::Persistent) {
            ContextMetadata md = instr.metadata;
            objects_.store(instr.object_id, instr.generation, CopyRole::Persistent, StorageDomain::LocalPersistent,
                           &md, buffer.data(), buffer.size());
        } else {
            ContextMetadata md = instr.metadata;
            CopyRole stored_role = role == CopyRole::Authoritative ? CopyRole::Authoritative : CopyRole::Replica;
            objects_.store(instr.object_id, instr.generation, stored_role, StorageDomain::HostMemory, &md,
                           buffer.data(), buffer.size());
        }
    } catch (const CfError& e) {
        report(RpcResult::failure(e.kind(), e.what()), 0, 0, 0);
        return;
    }

    report(RpcResult::success(), buffer.size(), received.crc32c, received.sha);
}

void NodeRuntime::do_verify(const InstructVerifyReq& instr) {
    uint64_t t0 = now_unix_ms();
    try {
        objects_.verify(instr.object_id, instr.generation, instr.crc32c, instr.sha);
        send_data_result(instr.op_token, instr.object_id, instr.generation, RpcResult::success(),
                         instr.byte_size, now_unix_ms() - t0, instr.crc32c, instr.sha);
    } catch (const CfError& e) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(e.kind(), e.what()), 0, now_unix_ms() - t0, 0, 0);
    }
}

void NodeRuntime::do_self_persist(const InstructRecvReq& instr) {
    uint64_t t0 = steady_now_ms();
    try {
        std::vector<uint8_t> bytes = objects_.read(instr.object_id, instr.generation);
        if (instr.crc32c != 0 || instr.sha != 0) {
            IntegrityId idg = compute_integrity(bytes.data(), bytes.size());
            if (idg.crc32c != instr.crc32c || idg.sha != instr.sha) {
                throw_error(ErrKind::Integrity, "self-persist source checksum mismatch");
            }
        }
        objects_.store(instr.object_id, instr.generation, CopyRole::Persistent, StorageDomain::LocalPersistent,
                       &instr.metadata, bytes.data(), bytes.size());
        uint64_t duration = steady_now_ms() - t0;
        IntegrityId idg = compute_integrity(bytes.data(), bytes.size());
        send_data_result(instr.op_token, instr.object_id, instr.generation, RpcResult::success(), bytes.size(),
                         duration, idg.crc32c, idg.sha);
    } catch (const CfError& e) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(e.kind(), e.what()), 0, 0, 0, 0);
    }
}

void NodeRuntime::do_self_restore(const InstructRecvReq& instr) {
    uint64_t t0 = steady_now_ms();
    try {
        if (!persistent_) throw_error(ErrKind::NotSupported, "no persistent backend");
        std::vector<uint8_t> bytes = persistent_->get(instr.object_id, instr.generation);
        if (instr.crc32c != 0 || instr.sha != 0) {
            IntegrityId idg = compute_integrity(bytes.data(), bytes.size());
            if (idg.crc32c != instr.crc32c || idg.sha != instr.sha) {
                throw_error(ErrKind::Integrity, "restore source checksum mismatch");
            }
        }
        objects_.store(instr.object_id, instr.generation, CopyRole::Authoritative, StorageDomain::HostMemory,
                       &instr.metadata, bytes.data(), bytes.size());
        uint64_t duration = steady_now_ms() - t0;
        IntegrityId idg = compute_integrity(bytes.data(), bytes.size());
        send_data_result(instr.op_token, instr.object_id, instr.generation, RpcResult::success(), bytes.size(),
                         duration, idg.crc32c, idg.sha);
    } catch (const CfError& e) {
        send_data_result(instr.op_token, instr.object_id, instr.generation,
                         RpcResult::failure(e.kind(), e.what()), 0, 0, 0, 0);
    }
}

}  // namespace ctf