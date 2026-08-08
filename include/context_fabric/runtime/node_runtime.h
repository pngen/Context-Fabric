#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "context_fabric/core/node.h"
#include "context_fabric/core/telemetry.h"
#include "context_fabric/core/types.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/protocol/messages.h"
#include "context_fabric/runtime/object_store.h"
#include "context_fabric/storage/host_memory_backend.h"
#include "context_fabric/storage/persistent_backend.h"
#include "context_fabric/storage/accelerator_backend.h"
#include "context_fabric/transport/socket.h"
#include "context_fabric/transport/tcp_transport.h"

namespace ctf {

// ---------------------------------------------------------------------------
// NodeRuntime: one node process.
//
// A node owns an independent process, memory budget, persistent storage root,
// telemetry stream, and capability set. It registers with the coordinator,
// heartbeats, and executes data-plane instructions (send / receive / verify /
// promote / dispose) brokered by the coordinator. Transfers between nodes flow
// over the real transport protocol on the data-plane listener.
// ---------------------------------------------------------------------------
struct NodeConfig {
    NodeId node_id;
    std::string label;
    std::string coord_host = "127.0.0.1";
    uint16_t coord_port = 0;

    std::string store_root;  // persistent storage root ("" = no persistent domain)
    uint64_t host_capacity = 0;
    uint64_t persistent_capacity = 0;
    uint64_t accelerator_capacity = 0;
    bool enable_cuda = false;

    std::string data_bind_host = "127.0.0.1";
    uint16_t data_port = 0;  // 0 = ephemeral

    std::string telemetry_path;
    uint64_t heartbeat_interval_ms = 1000;
    uint64_t op_timeout_ms = 60000;
    size_t transfer_workers = 4;
    size_t max_pending_recv = 64;
    size_t job_queue_capacity = 256;
};

class NodeRuntime {
public:
    NodeRuntime(NodeConfig cfg, Telemetry& telemetry);
    ~NodeRuntime();

    NodeRuntime(const NodeRuntime&) = delete;
    NodeRuntime& operator=(const NodeRuntime&) = delete;

    // Start backends, data-plane listener, and register with the coordinator.
    bool start();
    // Blocks until request_shutdown().
    void run();
    void request_shutdown();
    // Deterministic join of all threads.
    void shutdown();

    [[nodiscard]] NodeId node_id() const noexcept { return cfg_.node_id; }
    [[nodiscard]] uint16_t data_port() const noexcept { return data_port_.load(); }
    [[nodiscard]] NodeAddress data_addr() const;
    [[nodiscard]] NodeCapabilities capabilities() const noexcept { return caps_; }
    [[nodiscard]] NodeObjectStore& objects() noexcept { return objects_; }
    [[nodiscard]] HostMemoryBackend& host() noexcept { return *host_; }
    [[nodiscard]] LocalPersistentBackend* persistent() noexcept { return persistent_.get(); }
    [[nodiscard]] AcceleratorBackend* accelerator() noexcept { return accelerator_.get(); }
    [[nodiscard]] const Telemetry& telemetry() const noexcept { return telemetry_; }

private:
    struct PendingRecv {
        InstructRecvReq instr;
        uint64_t created_ms = 0;
    };
    struct Job {
        enum Kind { kRecv, kSend, kVerify } kind = kRecv;
        Socket sock;                 // for kRecv
        InstructSendReq send;        // for kSend
        InstructVerifyReq verify;    // for kVerify
    };

    void control_loop();
    void heartbeat_loop();
    void accept_loop();
    void worker_loop();

    bool register_with_coordinator();
    void send_data_result(uint64_t op_token, const ObjectId& id, const Generation& gen,
                          const RpcResult& result, uint64_t bytes, uint64_t duration_ms, uint64_t crc,
                          uint64_t sha);

    void handle_instruction(MessageType type, const std::vector<uint8_t>& payload);
    void handle_instruct_recv(const InstructRecvReq& instr);
    void handle_instruct_abort(const InstructAbortReq& instr);
    void handle_instruct_promote(const InstructPromoteReq& instr);
    void handle_instruct_dispose(const InstructDisposeReq& instr);

    void do_send(const InstructSendReq& instr);
    void do_recv(Socket sock);
    void do_verify(const InstructVerifyReq& instr);

    void enqueue_job(Job&& job);
    Job dequeue_job();

    // Inlined self-load operations (persist / restore self-load).
    void do_self_persist(const InstructRecvReq& instr);
    void do_self_restore(const InstructRecvReq& instr);

    NodeConfig cfg_;
    Telemetry& telemetry_;
    NodeCapabilities caps_;

    std::unique_ptr<HostMemoryBackend> host_;
    std::unique_ptr<LocalPersistentBackend> persistent_;
    std::unique_ptr<AcceleratorBackend> accelerator_;
    NodeObjectStore objects_;

    std::atomic<bool> running_{false};
    std::atomic<uint16_t> data_port_{0};
    std::atomic<uint64_t> epoch_{0};
    std::atomic<uint64_t> session_id_{0};
    std::atomic<uint64_t> session_epoch_{0};

    Socket ctrl_sock_;
    std::mutex ctrl_socket_lifecycle_mutex_;
    std::mutex ctrl_send_mutex_;
    std::atomic<uint64_t> req_seq_{1};

    Socket listener_;
    std::mutex active_socket_mutex_;
    std::unordered_set<Socket*> active_sockets_;
    std::thread control_thread_;
    std::thread heartbeat_thread_;
    std::thread accept_thread_;
    std::vector<std::thread> worker_threads_;

    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::deque<Job> queue_;

    std::mutex mutex_;
    std::condition_variable pending_cv_;
    std::unordered_map<uint64_t, PendingRecv> pending_recv_;

    std::mutex lifecycle_mutex_;
    std::mutex shutdown_mutex_;
    std::condition_variable shutdown_cv_;

    uint64_t chunk_size_ = 1024 * 1024;
};

}  // namespace ctf
