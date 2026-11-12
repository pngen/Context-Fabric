#include "test_harness.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "context_fabric/api/context_fabric.h"
#include "context_fabric/authority/client.h"
#include "context_fabric/authority/coordinator.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/runtime/node_runtime.h"

using namespace ctf;

namespace fs = std::filesystem;

namespace {

struct TestCoordinator {
    std::string store_path;
    Telemetry telemetry;
    Coordinator coord;

    TestCoordinator() : store_path(make_store()), coord(make_cfg(store_path), telemetry) {
        telemetry.set_stderr_enabled(false);
        if (!coord.start()) throw CfError(ErrKind::Internal, "coordinator start failed");
    }
    ~TestCoordinator() {
        coord.shutdown();
        std::error_code ec;
        fs::remove(store_path, ec);
    }

    static std::string make_store() {
        return (fs::temp_directory_path() / ("ctf-coord-" + ObjectId::random().to_hex())).string() + ".store";
    }
    static Coordinator::Config make_cfg(const std::string& store) {
        Coordinator::Config cfg;
        cfg.bind_host = "127.0.0.1";
        cfg.control_port = 0;
        cfg.store_path = store;
        cfg.telemetry_path = "";
        cfg.heartbeat_interval_ms = 500;
        cfg.session_timeout_ms = 3000;
        cfg.lease_default_ms = 2000;
        cfg.op_timeout_ms = 20000;
        cfg.create_timeout_ms = 10000;
        cfg.driver_threads = 4;
        return cfg;
    }
    [[nodiscard]] uint16_t port() const { return coord.bound_port(); }
};

struct TestNode {
    std::string root;
    Telemetry telemetry;
    NodeRuntime node;

    TestNode(const std::string& name, uint16_t coord_port, uint64_t host_cap, bool persistent)
        : root(make_root(name)), node(make_cfg(name, coord_port, host_cap, persistent, root), telemetry) {
        telemetry.set_stderr_enabled(false);
        if (!node.start()) throw CfError(ErrKind::Internal, "node start failed");
    }
    ~TestNode() {
        node.shutdown();
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    static std::string make_root(const std::string& name) {
        auto r = (fs::temp_directory_path() / ("ctf-node-" + name + "-" + ObjectId::random().to_hex())).string();
        fs::create_directories(r);
        return r;
    }
    static NodeConfig make_cfg(const std::string& name, uint16_t coord_port, uint64_t host_cap,
                               bool persistent, const std::string& root) {
        NodeConfig cfg;
        cfg.node_id = NodeId::deterministic(name);
        cfg.label = name;
        cfg.coord_host = "127.0.0.1";
        cfg.coord_port = coord_port;
        cfg.host_capacity = host_cap;
        if (persistent) {
            cfg.store_root = root;
            cfg.persistent_capacity = 256ull * 1024 * 1024;
        }
        cfg.heartbeat_interval_ms = 500;
        cfg.transfer_workers = 4;
        return cfg;
    }
};

ContextMetadata md_with(uint64_t bytes) {
    ContextMetadata md;
    md.namespace_id = "test";
    md.semantic_class = SemanticClass::KvCache;
    md.byte_size = bytes;
    md.logical_size = bytes;
    md.sharing = SharingPolicy::NamespaceShared;
    md.persistence = PersistencePolicy::Persistent;
    md.recompute_seconds = 5.0;
    md.model_id = "test-model";
    md.architecture_namespace = "test";
    md.compatibility_fingerprint = md.compute_compatibility_fingerprint();
    return md;
}

template <typename F>
bool wait_until(F&& f, uint64_t timeout_ms, uint64_t step_ms = 50) {
    uint64_t waited = 0;
    while (waited < timeout_ms) {
        if (f()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
        waited += step_ms;
    }
    return f();
}

// A node the coordinator believes is online but which never completes a
// transfer: its data plane accepts a connection and hangs up without
// acknowledging anything, and it never reports a result. An operation that
// targets it stays in flight until this node's control session closes, which
// makes the coordinator's handling of a disconnect during an in-flight
// operation deterministic to test.
struct SilentNode {
    std::string name;
    Socket data_listener;
    uint16_t data_port = 0;
    std::atomic<bool> accepting{true};
    std::thread acceptor;
    ClientSession control;

    SilentNode(const std::string& node_name, uint16_t coord_port) : name(node_name) {
        if (!data_listener.listen("127.0.0.1", 0, data_port)) {
            throw CfError(ErrKind::Internal, "silent node data listener failed");
        }
        acceptor = std::thread([this] {
            while (accepting.load()) {
                Socket peer = data_listener.accept(50);
                peer.close();
            }
        });
        try {
            if (!control.connect("127.0.0.1", coord_port, 5000)) {
                throw CfError(ErrKind::Internal, "silent node control connect failed");
            }
            control.hello();
            NodeRegisterReq reg;
            reg.node_id = id();
            reg.label = name;
            reg.addr.host = "127.0.0.1";
            reg.addr.port = data_port;
            reg.capabilities = kCapHostMemory;
            reg.max_host_bytes = 64ull * 1024 * 1024;
            Writer w;
            serialize_node_register(w, reg);
            std::vector<uint8_t> payload;
            control.call(kMsgNodeRegister, kMsgNodeRegisterResp, w.data(), payload);
            Reader r(payload.data(), payload.size());
            if (deserialize_node_register_resp(r).result.failed()) {
                throw CfError(ErrKind::Internal, "silent node registration failed");
            }
        } catch (...) {
            stop_acceptor();
            throw;
        }
    }
    ~SilentNode() {
        disconnect();
        stop_acceptor();
    }

    [[nodiscard]] uint64_t id() const { return NodeId::deterministic(name).value(); }
    void disconnect() { control.close(); }
    void stop_acceptor() {
        accepting.store(false);
        if (acceptor.joinable()) acceptor.join();
    }
};

// Runs one blocking replicate request on its own control connection and
// always joins, so a failed check can never leave a joinable thread behind.
struct BlockedReplicate {
    std::atomic<bool> failed{false};
    std::thread thread;

    BlockedReplicate(uint16_t coord_port, const ObjectId& id, uint64_t target) {
        thread = std::thread([this, coord_port, id, target] {
            try {
                ContextFabric blocked;
                if (!blocked.connect("127.0.0.1", coord_port, 5000)) {
                    failed.store(true);
                    return;
                }
                blocked.replicate(id, target);
            } catch (const std::exception&) {
                failed.store(true);
            }
        });
    }
    ~BlockedReplicate() { join(); }
    void join() {
        if (thread.joinable()) thread.join();
    }
};

uint32_t lifecycle_of(ContextFabric& cf, const ObjectId& id) {
    return cf.inspect(id).lifecycle;
}

constexpr uint32_t kAvailable = static_cast<uint32_t>(LifecycleState::Available);

}  // namespace

CF_TEST(authority_three_node_fabric_roundtrip) {
    TestCoordinator tc;
    TestNode na("node-a", tc.port(), 64ull * 1024 * 1024, false);
    TestNode nb("node-b", tc.port(), 64ull * 1024 * 1024, false);
    TestNode nc("node-c", tc.port(), 64ull * 1024 * 1024, true);

    // Wait for all three nodes to register.
    CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 3; }, 10000));

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));

    uint64_t node_a = NodeId::deterministic("node-a").value();
    uint64_t node_b = NodeId::deterministic("node-b").value();
    uint64_t node_c = NodeId::deterministic("node-c").value();

    // Create an object on node-a and upload real bytes.
    auto md = md_with(1024 * 1024);
    auto content = generate_content(100, 1, 1024 * 1024);
    ObjectId id = cf.create_object(md, node_a, content);
    CF_CHECK(id.valid());

    // Inspect: authoritative on node-a, generation 1, available.
    auto ir = cf.inspect(id);
    CF_CHECK(ir.has == 1);
    CF_CHECK(ir.authoritative_node == node_a);
    CF_CHECK(ir.generation.value() == 1);
    CF_CHECK(ir.lifecycle == static_cast<uint32_t>(LifecycleState::Available));

    // Verify integrity at the authoritative node.
    auto vr = cf.verify(id, node_a, true);
    CF_CHECK(vr.ok);
    CF_CHECK(vr.bytes == content.size());

    // Replicate to node-b.
    auto rep = cf.replicate(id, node_b);
    CF_CHECK(rep.node_id == node_b);
    auto vr2 = cf.verify(id, node_b, true);
    CF_CHECK(vr2.ok);
    auto ir2 = cf.inspect(id);
    CF_CHECK(std::find(ir2.replica_nodes.begin(), ir2.replica_nodes.end(), node_b) != ir2.replica_nodes.end());

    // Duplicate replicate must be rejected (already present).
    CF_CHECK_THROWS(cf.replicate(id, node_b), ErrKind::AlreadyExists);

    // Migrate to node-c (keep source).
    auto mig = cf.migrate(id, node_c, StorageDomain::HostMemory, true);
    CF_CHECK(mig.node_id == node_c);
    auto ir3 = cf.inspect(id);
    CF_CHECK(ir3.authoritative_node == node_c);
    CF_CHECK(std::find(ir3.replica_nodes.begin(), ir3.replica_nodes.end(), node_a) != ir3.replica_nodes.end());
    CF_CHECK(ir3.lease_generation >= ir2.lease_generation + 1);

    // Verify at the new authoritative node.
    auto vr3 = cf.verify(id, node_c, true);
    CF_CHECK(vr3.ok);

    // Persist on node-c (node-c has persistent storage).
    uint64_t pnode = cf.persist(id, node_c);
    CF_CHECK(pnode == node_c);
    auto ir4 = cf.inspect(id);
    CF_CHECK(std::find(ir4.persistent_nodes.begin(), ir4.persistent_nodes.end(), node_c) !=
             ir4.persistent_nodes.end());

    // Restore to node-a.
    auto res = cf.restore(id, node_a);
    CF_CHECK(res.node_id == node_a);
    auto ir5 = cf.inspect(id);
    CF_CHECK(ir5.authoritative_node == node_a);
    CF_CHECK(ir5.lifecycle == static_cast<uint32_t>(LifecycleState::Available));

    // Lineage survives movement.
    auto lin = cf.lineage(id);
    CF_CHECK(!lin.lineage.empty());
    CF_CHECK(lin.lineage[0].object == id);
    CF_CHECK(lin.lineage[0].generation == Generation::first());

    // Delete.
    cf.remove(id);
    auto ir_after = cf.inspect(id);
    CF_CHECK(ir_after.has == 0);
    CF_CHECK(wait_until([&] {
        return !na.node.objects().has_copy(id, Generation::first()) &&
               !nb.node.objects().has_copy(id, Generation::first()) &&
               !nc.node.objects().has_copy(id, Generation::first());
    }, 5000));
    CF_CHECK(!nc.node.persistent()->contains(id, Generation::first()));
}

CF_TEST(authority_leases_conflict_and_stale) {
    TestCoordinator tc;
    TestNode na("node-a", tc.port(), 64ull * 1024 * 1024, false);
    TestNode nb("node-b", tc.port(), 64ull * 1024 * 1024, false);
    CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 2; }, 10000));

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    uint64_t node_a = NodeId::deterministic("node-a").value();
    uint64_t node_b = NodeId::deterministic("node-b").value();

    auto md = md_with(4096);
    ObjectId id = cf.create_object(md, node_a, generate_content(1, 1, 4096));
    CF_REQUIRE(id.valid());

    // Acquire a write lease on node-a.
    Lease wl = cf.acquire_write_lease(id, node_a, 10000);
    CF_CHECK(wl.lease_id != 0);
    CF_CHECK(wl.mode == LeaseMode::Write);

    // A second write lease must conflict.
    CF_CHECK_THROWS(cf.acquire_write_lease(id, node_b, 10000), ErrKind::Conflict);
    // A read lease while write-held must conflict.
    CF_CHECK_THROWS(cf.acquire_read_lease(id, node_b, 10000), ErrKind::Conflict);

    // Release the write lease.
    cf.release_lease(id, node_a, wl.lease_id);
    // Now a read lease works.
    Lease rl = cf.acquire_read_lease(id, node_b, 10000);
    CF_CHECK(rl.mode == LeaseMode::Read);
    // Write lease while read-held conflicts.
    CF_CHECK_THROWS(cf.acquire_write_lease(id, node_a, 10000), ErrKind::Conflict);
    cf.release_lease(id, node_b, rl.lease_id);

    // Releasing a nonexistent lease is a typed error.
    CF_CHECK_THROWS(cf.release_lease(id, node_a, 999999), ErrKind::NotFound);
}

CF_TEST(authority_write_generation_advances) {
    TestCoordinator tc;
    TestNode na("node-a", tc.port(), 64ull * 1024 * 1024, false);
    TestNode nb("node-b", tc.port(), 64ull * 1024 * 1024, false);
    TestNode nc("node-c", tc.port(), 64ull * 1024 * 1024, true);
    CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 3; }, 10000));

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    uint64_t node_a = NodeId::deterministic("node-a").value();
    uint64_t node_b = NodeId::deterministic("node-b").value();
    uint64_t node_c = NodeId::deterministic("node-c").value();

    auto md = md_with(4096);
    ObjectId id = cf.create_object(md, node_a, generate_content(1, 1, 4096));
    CF_CHECK(cf.replicate(id, node_b).node_id == node_b);
    CF_CHECK(cf.persist(id, node_c) == node_c);

    Lease wl = cf.acquire_write_lease(id, node_a, 10000);
    auto new_content = generate_content(2, 2, 8192);
    Generation g2 = cf.write_generation(id, wl, new_content);
    CF_CHECK(g2.value() == 2);

    auto ir = cf.inspect(id);
    CF_CHECK(ir.generation.value() == 2);
    auto vr = cf.verify(id, node_a, true);
    CF_CHECK(vr.ok);
    CF_CHECK(vr.bytes == new_content.size());
    CF_CHECK(wait_until([&] {
        return !na.node.objects().has_copy(id, Generation::first()) &&
               !nb.node.objects().has_copy(id, Generation::first()) &&
               !nc.node.objects().has_copy(id, Generation::first());
    }, 5000));
    CF_CHECK(na.node.objects().has_copy(id, g2));
    CF_CHECK(!nc.node.persistent()->contains(id, Generation::first()));

    // Writing with a wrong expected generation is rejected (conflict).
    CF_CHECK_THROWS(cf.write_generation(id, wl, generate_content(3, 3, 100)), ErrKind::Conflict);
    // Writing with a bogus lease id is rejected as stale.
    Lease bogus = wl;
    bogus.lease_id = wl.lease_id + 999;
    CF_CHECK_THROWS(cf.write_generation(id, bogus, generate_content(3, 3, 100)), ErrKind::StaleLease);
}

CF_TEST(authority_lifetime_expiry_revokes_authority_and_disposes_reachable_copies) {
    TestCoordinator tc;
    TestNode na("expiry-a", tc.port(), 64ull * 1024 * 1024, false);
    TestNode nb("expiry-b", tc.port(), 64ull * 1024 * 1024, false);
    TestNode nc("expiry-c", tc.port(), 64ull * 1024 * 1024, true);
    CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 3; }, 10000));

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    const uint64_t node_a = NodeId::deterministic("expiry-a").value();
    const uint64_t node_b = NodeId::deterministic("expiry-b").value();
    const uint64_t node_c = NodeId::deterministic("expiry-c").value();
    ContextMetadata md = md_with(4096);
    md.lifetime_seconds = 2;
    const ObjectId id = cf.create_object(md, node_a, generate_content(21, 22, 4096));
    CF_CHECK(cf.replicate(id, node_b).node_id == node_b);
    CF_CHECK(cf.persist(id, node_c) == node_c);

    CF_REQUIRE(wait_until([&] {
        return cf.inspect(id).lifecycle == static_cast<uint32_t>(LifecycleState::Expired);
    }, 7000));
    const auto expired = cf.inspect(id);
    CF_CHECK(expired.authoritative_node == 0);
    CF_CHECK(expired.authoritative_generation == 0);
    CF_CHECK(expired.authoritative_domain == static_cast<uint32_t>(StorageDomain::Unknown));
    CF_CHECK(expired.replica_nodes.empty());
    CF_CHECK(expired.persistent_nodes.empty());
    CF_CHECK_THROWS(cf.acquire_read_lease(id, node_a, 1000), ErrKind::State);
    CF_CHECK(wait_until([&] {
        return !na.node.objects().has_copy(id, Generation::first()) &&
               !nb.node.objects().has_copy(id, Generation::first()) &&
               !nc.node.objects().has_copy(id, Generation::first());
    }, 5000));
    CF_CHECK(!nc.node.persistent()->contains(id, Generation::first()));
}

CF_TEST(authority_retry_idempotent) {
    TestCoordinator tc;
    TestNode na("node-a", tc.port(), 64ull * 1024 * 1024, false);
    CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 1; }, 10000));

    ClientSession c;
    CF_REQUIRE(c.connect("127.0.0.1", tc.port(), 5000));
    c.hello();

    // Create with a fixed request id, then replay the same request id.
    ObjectCreateReq req;
    req.metadata = md_with(4096);
    req.owner_node = NodeId::deterministic("node-a").value();
    req.derivation_op = "idem";
    Writer w;
    serialize_object_create(w, req);
    std::vector<uint8_t> p1, p2;
    uint64_t rid = c.next_request_id();
    c.send_request_with_id(kMsgObjectCreate, kMsgObjectCreateResp, w.data(), p1, rid);
    c.send_request_with_id(kMsgObjectCreate, kMsgObjectCreateResp, w.data(), p2, rid);
    // Both responses must be identical (idempotent), object created once.
    CF_CHECK(p1 == p2);
    Reader r1(p1.data(), p1.size());
    ObjectCreateResp resp1 = deserialize_object_create_resp(r1);
    CF_CHECK(resp1.result.ok);
auto list = c.list("");
    uint64_t count = 0;
    for (const auto& oid : list.ids) {
        if (oid == resp1.object_id) ++count;
    }
    CF_CHECK(count == 1);
}

CF_TEST(authority_node_disconnect_recovery) {
    TestCoordinator tc;
    TestNode nb("node-b", tc.port(), 64ull * 1024 * 1024, false);
    ObjectId id;
    {
        TestNode na("node-a", tc.port(), 64ull * 1024 * 1024, false);
        CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 2; }, 10000));

        ContextFabric cf;
        CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
        uint64_t node_a = NodeId::deterministic("node-a").value();
        uint64_t node_b = NodeId::deterministic("node-b").value();

        auto md = md_with(8192);
        id = cf.create_object(md, node_a, generate_content(5, 5, 8192));
        // Replicate to node-b so a live replica exists at disconnect time.
        cf.replicate(id, node_b);
        // Wait for the replica to be committed.
        CF_REQUIRE(wait_until([&] {
            auto ir = cf.inspect(id);
            return std::find(ir.replica_nodes.begin(), ir.replica_nodes.end(), node_b) != ir.replica_nodes.end();
        }, 10000));
    }
    // node-a died; node-b holds a validated replica of the authoritative
    // generation, so authority must be deterministically promoted to node-b.
    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    auto ir = cf.inspect(id);
    CF_REQUIRE(ir.authoritative_node == NodeId::deterministic("node-b").value());
    CF_REQUIRE(ir.lifecycle == static_cast<uint32_t>(LifecycleState::Available));
    // The promoted authority must still verify on the live node-b.
    auto vr = cf.verify(id, NodeId::deterministic("node-b").value(), true);
    CF_CHECK(vr.ok);
}

CF_TEST(authority_placement_and_decision_queries) {
    TestCoordinator tc;
    TestNode na("node-a", tc.port(), 64ull * 1024 * 1024, false);
    TestNode nb("node-b", tc.port(), 64ull * 1024 * 1024, false);
    CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 2; }, 10000));

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    uint64_t node_a = NodeId::deterministic("node-a").value();
    uint64_t node_b = NodeId::deterministic("node-b").value();

    auto md = md_with(1024);
    auto d = cf.query_placement(md, 0, 0);
    CF_CHECK(d.location.node_id != 0);
    CF_CHECK(!d.reason.empty());

    auto dec = cf.decide_transfer_vs_recompute(md, node_a, node_b, 1e9);
    CF_CHECK(dec.policy == std::string("transfer-vs-recompute"));
    CF_CHECK(!dec.reason.empty());
}

CF_TEST(authority_source_disconnect_aborts_transfer_and_recovers_authority) {
    TestCoordinator tc;
    TestNode nb("src-loss-b", tc.port(), 64ull * 1024 * 1024, false);
    auto na = std::make_unique<TestNode>("src-loss-a", tc.port(), 64ull * 1024 * 1024, false);
    CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 2; }, 10000));
    const uint64_t node_a = NodeId::deterministic("src-loss-a").value();
    const uint64_t node_b = NodeId::deterministic("src-loss-b").value();

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    const ObjectId id = cf.create_object(md_with(8192), node_a, generate_content(31, 32, 8192));
    CF_REQUIRE(cf.replicate(id, node_b).node_id == node_b);

    // A second replication is now in flight with node-a as its source: the
    // target accepts the transfer and never completes it.
    SilentNode silent("src-loss-silent", tc.port());
    BlockedReplicate replicating(tc.port(), id, silent.id());
    CF_CHECK(wait_until([&] {
        return lifecycle_of(cf, id) == static_cast<uint32_t>(LifecycleState::Replicating);
    }, 5000));

    // The authoritative source dies while that operation is in flight.
    na.reset();

    // The operation references the dead node and must be aborted, and the
    // object must be recovered onto its live replica. It must never return to
    // service still naming the offline node as its authority.
    CF_CHECK(wait_until([&] {
        const auto ir = cf.inspect(id);
        return ir.authoritative_node == node_b && ir.lifecycle == kAvailable;
    }, 5000));

    silent.disconnect();
    replicating.join();
    CF_CHECK(replicating.failed.load());
    const auto settled = cf.inspect(id);
    CF_CHECK(settled.authoritative_node == node_b);
    CF_CHECK(settled.lifecycle == kAvailable);
    CF_CHECK(cf.verify(id, node_b, true).ok);
}

CF_TEST(authority_lease_holder_disconnect_returns_object_to_available) {
    TestCoordinator tc;
    TestNode na("lease-loss-a", tc.port(), 64ull * 1024 * 1024, false);
    CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 1; }, 10000));
    const uint64_t node_a = NodeId::deterministic("lease-loss-a").value();

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    const ObjectId id = cf.create_object(md_with(4096), node_a, generate_content(41, 42, 4096));

    // A read lease held by a node other than the authoritative one. The long
    // duration keeps lease expiry out of the picture.
    {
        TestNode reader("lease-loss-reader", tc.port(), 64ull * 1024 * 1024, false);
        CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 2; }, 10000));
        const Lease held =
            cf.acquire_read_lease(id, NodeId::deterministic("lease-loss-reader").value(), 60000);
        CF_CHECK(held.mode == LeaseMode::Read);
        CF_CHECK(lifecycle_of(cf, id) == static_cast<uint32_t>(LifecycleState::LeasedRead));
    }
    // The holder is gone and its lease revoked: the object is leasable again.
    CF_CHECK(wait_until([&] {
        return tc.coord.online_node_count() == 1 && lifecycle_of(cf, id) == kAvailable;
    }, 5000));
    Lease read_again;
    CF_CHECK_NOTHROW(read_again = cf.acquire_read_lease(id, node_a, 60000));
    if (read_again.lease_id != 0) cf.release_lease(id, node_a, read_again.lease_id);

    // The same holds for a write lease held by a non-authoritative node.
    const uint64_t lease_generation_before = cf.inspect(id).lease_generation;
    {
        TestNode writer("lease-loss-writer", tc.port(), 64ull * 1024 * 1024, false);
        CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 2; }, 10000));
        const Lease held =
            cf.acquire_write_lease(id, NodeId::deterministic("lease-loss-writer").value(), 60000);
        CF_CHECK(held.mode == LeaseMode::Write);
        CF_CHECK(lifecycle_of(cf, id) == static_cast<uint32_t>(LifecycleState::LeasedWrite));
    }
    CF_CHECK(wait_until([&] {
        return tc.coord.online_node_count() == 1 && lifecycle_of(cf, id) == kAvailable;
    }, 5000));
    CF_CHECK(cf.inspect(id).lease_generation > lease_generation_before);
    Lease write_again;
    CF_CHECK_NOTHROW(write_again = cf.acquire_write_lease(id, node_a, 60000));
    if (write_again.lease_id != 0) cf.release_lease(id, node_a, write_again.lease_id);
}

CF_TEST(authority_aborted_operation_does_not_restore_a_revoked_lease_lifecycle) {
    TestCoordinator tc;
    TestNode na("abort-lease-a", tc.port(), 64ull * 1024 * 1024, false);
    auto reader = std::make_unique<TestNode>("abort-lease-reader", tc.port(), 64ull * 1024 * 1024, false);
    CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 2; }, 10000));
    const uint64_t node_a = NodeId::deterministic("abort-lease-a").value();
    const uint64_t node_reader = NodeId::deterministic("abort-lease-reader").value();

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    const ObjectId id = cf.create_object(md_with(4096), node_a, generate_content(51, 52, 4096));
    const Lease held = cf.acquire_read_lease(id, node_reader, 60000);
    CF_REQUIRE(held.lease_id != 0);

    // A replication that stays in flight was started from the read-leased state.
    SilentNode silent("abort-lease-silent", tc.port());
    BlockedReplicate replicating(tc.port(), id, silent.id());
    CF_CHECK(wait_until([&] {
        return lifecycle_of(cf, id) == static_cast<uint32_t>(LifecycleState::Replicating);
    }, 5000));

    // The only lease holder dies mid-operation, then the operation aborts.
    reader.reset();
    CF_CHECK(wait_until([&] { return tc.coord.online_node_count() <= 2; }, 5000));
    silent.disconnect();
    replicating.join();
    CF_CHECK(replicating.failed.load());

    // Rolling back to "read-leased" with no lease left would refuse every
    // later lease request; the object must settle to Available instead.
    CF_CHECK(wait_until([&] { return lifecycle_of(cf, id) == kAvailable; }, 5000));
    Lease again;
    CF_CHECK_NOTHROW(again = cf.acquire_read_lease(id, node_a, 60000));
    if (again.lease_id != 0) cf.release_lease(id, node_a, again.lease_id);
}

CF_TEST(authority_keep_source_migration_records_the_real_source_as_replica) {
    TestCoordinator tc;
    TestNode nb("mig-src-b", tc.port(), 64ull * 1024 * 1024, false);
    TestNode nc("mig-src-c", tc.port(), 64ull * 1024 * 1024, false);
    const uint64_t node_a = NodeId::deterministic("mig-src-a").value();
    const uint64_t node_b = NodeId::deterministic("mig-src-b").value();
    const uint64_t node_c = NodeId::deterministic("mig-src-c").value();

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    ObjectId id;
    {
        TestNode na("mig-src-a", tc.port(), 64ull * 1024 * 1024, false);
        CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 3; }, 10000));
        id = cf.create_object(md_with(8192), node_a, generate_content(61, 62, 8192));
        CF_REQUIRE(cf.replicate(id, node_b).node_id == node_b);
    }
    // node-a died, so authority was promoted onto node-b's replica.
    CF_REQUIRE(wait_until([&] {
        const auto ir = cf.inspect(id);
        return ir.authoritative_node == node_b && ir.lifecycle == kAvailable;
    }, 10000));

    // Migrating on while keeping the source leaves the bytes on node-b. The
    // replica set must name node-b, never the long-gone original owner.
    CF_CHECK(cf.migrate(id, node_c, StorageDomain::HostMemory, true).node_id == node_c);
    const auto ir = cf.inspect(id);
    CF_CHECK(ir.authoritative_node == node_c);
    CF_CHECK(std::find(ir.replica_nodes.begin(), ir.replica_nodes.end(), node_b) != ir.replica_nodes.end());
    CF_CHECK(std::find(ir.replica_nodes.begin(), ir.replica_nodes.end(), node_a) == ir.replica_nodes.end());
    CF_CHECK(cf.verify(id, node_b, true).ok);
}
