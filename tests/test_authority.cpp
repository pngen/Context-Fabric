#include "test_harness.h"

#include <chrono>
#include <filesystem>
#include <thread>

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
    Coordinator coord;
    Telemetry telemetry;

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
        : root(make_root(name)), node(make_cfg(name, coord_port, host_cap, persistent), telemetry) {
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
                               bool persistent) {
        NodeConfig cfg;
        cfg.node_id = NodeId::deterministic(name);
        cfg.label = name;
        cfg.coord_host = "127.0.0.1";
        cfg.coord_port = coord_port;
        cfg.host_capacity = host_cap;
        if (persistent) {
            cfg.store_root = make_root(name + "-store");
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
    CF_REQUIRE(wait_until([&] { return tc.coord.online_node_count() == 2; }, 10000));

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    uint64_t node_a = NodeId::deterministic("node-a").value();

    auto md = md_with(4096);
    ObjectId id = cf.create_object(md, node_a, generate_content(1, 1, 4096));

    Lease wl = cf.acquire_write_lease(id, node_a, 10000);
    auto new_content = generate_content(2, 2, 8192);
    Generation g2 = cf.write_generation(id, wl, new_content);
    CF_CHECK(g2.value() == 2);

    auto ir = cf.inspect(id);
    CF_CHECK(ir.generation.value() == 2);
    auto vr = cf.verify(id, node_a, true);
    CF_CHECK(vr.ok);
    CF_CHECK(vr.bytes == new_content.size());

    // Writing with a wrong expected generation is rejected (conflict).
    CF_CHECK_THROWS(cf.write_generation(id, wl, generate_content(3, 3, 100)), ErrKind::Conflict);
    // Writing with a bogus lease id is rejected as stale.
    Lease bogus = wl;
    bogus.lease_id = wl.lease_id + 999;
    CF_CHECK_THROWS(cf.write_generation(id, bogus, generate_content(3, 3, 100)), ErrKind::StaleLease);
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