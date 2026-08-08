#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "context_fabric/core/clock.h"
#include "context_fabric/core/telemetry.h"
#include "context_fabric/authority/coordinator_state.h"
#include "context_fabric/authority/coordinator_store.h"
#include "context_fabric/transport/socket.h"
#include "context_fabric/transport/frame.h"
#include "context_fabric/protocol/messages.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Metadata coordinator (v1).
//
// Design: a single elected/configured coordinator for the local fabric. It is
// deliberately NOT a replicated consensus service. Guarantees:
//   - exactly one authority per object at a time (enforced by serialized ops)
//   - lease tokens embed the coordinator epoch so stale nodes/leases are
//     rejected after restart or ownership change
//   - durable metadata via atomic snapshot commits => deterministic recovery
//   - in-flight operations are never committed partially; restart aborts them
//   - conflicting owners and stale leases are rejected with typed errors
//
// Concurrency: an accept thread, bounded reader threads, a bounded pool of
// driver threads, and one sweeper thread. All coordinator state is guarded by
// a single mutex; requests are handled with the mutex held only for the state
// mutation critical section, and the orchestration of multi-node operations is
// serialized per object via an explicit active-operation marker. This explicit,
// deliberately simple model is preferred over premature consensus machinery.
// ---------------------------------------------------------------------------
class Coordinator {
public:
    struct Config {
        std::string bind_host = "127.0.0.1";
        uint16_t control_port = 0;  // 0 = ephemeral
        std::string store_path;     // durable metadata path
        std::string telemetry_path; // JSONL telemetry path ("" = stderr only)
        uint64_t heartbeat_interval_ms = 1000;
        uint64_t session_timeout_ms = 5000;
        uint64_t lease_default_ms = 30000;
        uint64_t op_timeout_ms = 60000;
        uint64_t create_timeout_ms = 30000;
        size_t max_connections = 64;
        size_t driver_threads = 4;
        size_t queue_capacity = 2048;
    };

    Coordinator(Config cfg, Telemetry& telemetry);
    ~Coordinator();

    Coordinator(const Coordinator&) = delete;
    Coordinator& operator=(const Coordinator&) = delete;

    // Bind the control listener and start worker threads.
    bool start();
    // Blocks the calling thread until request_shutdown() is called.
    void run();
    void request_shutdown();
    // Deterministic join of all threads.
    void shutdown();

    [[nodiscard]] uint16_t bound_port() const noexcept { return bound_port_; }
    [[nodiscard]] uint32_t epoch() const noexcept;
    [[nodiscard]] bool running() const noexcept { return running_.load(); }

    // Test exposure: number of registered online nodes.
    [[nodiscard]] size_t online_node_count();

private:
    struct Conn;
    struct PendingOp;

    // Work items consumed by driver threads.
    struct WorkItem {
        enum Kind { kRequest, kClose, kShutdown } kind = kRequest;
        uint64_t conn_id = 0;
        FrameHeader hdr;
        std::vector<uint8_t> payload;
    };

    void accept_loop();
    void reader_loop(std::shared_ptr<Conn> conn);
    void driver_loop();
    void sweep_loop();
    void sweep_once();

    void enqueue(WorkItem&& item);
    WorkItem dequeue();

    // ---- request handlers (driver threads) ----
    void handle_request(const std::shared_ptr<Conn>& conn, const FrameHeader& hdr,
                        const std::vector<uint8_t>& payload);
    void handle_close(uint64_t conn_id);

    void handle_hello(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_register(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_heartbeat(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_node_shutdown(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_data_result(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_object_create(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_object_write(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_object_inspect(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_object_list(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_lineage(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_object_delete(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_lease_acquire(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_lease_release(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_replicate(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_migrate(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_persist(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_restore(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_verify(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_placement_query(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_decision_query(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);
    void handle_coord_shutdown_node(const std::shared_ptr<Conn>& conn, uint64_t req_id, Reader& r);

    // ---- helpers ----
    void send_to_conn(const std::shared_ptr<Conn>& conn, MessageType type, uint64_t req_id,
                      const std::vector<uint8_t>& payload);
    void send_instruction(uint64_t node_id, MessageType type, const std::vector<uint8_t>& payload);
    bool node_online(uint64_t node_id) const;
    const CoordNode* find_node(uint64_t node_id) const;

    // Orchestration. Returns the response payload to send to the client.
    std::vector<uint8_t> orchestrate_transfer(const std::string& kind, ObjectId object_id,
                                              uint64_t target_node, StorageDomain target_domain,
                                              bool keep_source, uint64_t on_node);

    // Commit/abort of a pending data operation. The shared_ptr is held by value
// so the op stays alive even after it is removed from the pending map.
    void commit_pending_op(std::shared_ptr<PendingOp> op);
    void abort_pending_op(std::shared_ptr<PendingOp> op, const std::string& reason);

    // Node disconnect cleanup.
    void node_disconnected(uint64_t node_id);

    // Recovery at startup.
    void reconcile_at_startup();

    void persist_if_dirty();
    void mark_dirty();

    // Telemetry emit helper.
    void emit(const std::string& event, const JsonObject& doc);

    Config cfg_;
    Telemetry& telemetry_;

    std::atomic<bool> running_{false};
    std::atomic<uint16_t> bound_port_{0};

    // Networking.
    Socket listener_;
    std::thread accept_thread_;
    std::thread sweep_thread_;
    std::vector<std::thread> driver_threads_;
    std::vector<std::thread> reader_threads_;

    // Work queue.
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::deque<WorkItem> queue_;
    size_t queue_size_ = 0;

    // State (guarded by state_mutex_).
    std::mutex state_mutex_;
    // Connection maps (guarded by conn_mutex_; a leaf lock that never acquires
    // state_mutex_, so it may be taken while state_mutex_ is held).
    std::mutex conn_mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<Conn>> conns_;          // conn_id -> conn
    std::unordered_map<uint64_t, uint64_t> node_conns_;                  // node_id -> conn_id
    uint32_t epoch_ = 0;
    uint64_t seq_ = 0;
    uint64_t conn_next_id_ = 1;
    std::map<uint64_t, CoordNode> nodes_;          // node_id -> node
    std::unordered_map<ObjectId, CoordObject> objects_;
    std::unordered_map<ObjectId, Lease> write_leases_;
    std::unordered_map<ObjectId, std::vector<Lease>> read_leases_;
    std::unordered_map<uint64_t, std::shared_ptr<PendingOp>> pending_ops_;  // op_token -> op
    std::map<uint64_t, CoordObject*> pending_by_object_;    // object_id -> op (for sweep)
    bool dirty_ = false;
    uint64_t last_persist_ms_ = 0;

    // Idempotency cache (bounded). key = (conn_id, request_id).
    struct IdemEntry {
        uint64_t key = 0;
        MessageType type = 0;
        std::vector<uint8_t> payload;
    };
    std::mutex idem_mutex_;
    std::deque<IdemEntry> idem_cache_;
    std::unordered_map<uint64_t, size_t> idem_index_;  // key -> position in deque

    CoordinatorStore store_;
};

}  // namespace ctf