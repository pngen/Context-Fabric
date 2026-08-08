// persistent_state: persist, restart/restore, and verify an object.
//
// Demonstrates durable local persistence: the object is persisted to the
// node's on-disk store, the fabric is torn down, a fresh coordinator + node
// are started over the same store root, and the object is restored and
// verified byte-exact.

#include <cstdio>
#include <filesystem>
#include <memory>

#include "context_fabric/api/context_fabric.h"
#include "context_fabric/authority/coordinator.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/runtime/node_runtime.h"

using namespace ctf;

namespace fs = std::filesystem;

int main() {
    std::string node_store = (fs::temp_directory_path() / "cf-persist-node").string();
    std::string coord_store = (fs::temp_directory_path() / "cf-persist-coord.store").string();
    fs::remove_all(node_store);
    fs::remove(coord_store);

    ObjectId id;
    auto content = generate_content(4, 4, 4ull * 1024 * 1024);

    {
        Telemetry ct;
        ct.set_stderr_enabled(false);
        Coordinator::Config cc;
        cc.bind_host = "127.0.0.1";
        cc.control_port = 0;
        cc.store_path = coord_store;
        cc.telemetry_path = "";
        Coordinator coord(cc, ct);
        if (!coord.start()) return 1;
        uint16_t port = coord.bound_port();

        Telemetry nt;
        nt.set_stderr_enabled(false);
        NodeConfig nc;
        nc.node_id = NodeId::deterministic("persist-node");
        nc.coord_host = "127.0.0.1";
        nc.coord_port = port;
        nc.store_root = node_store;
        nc.host_capacity = 256ull * 1024 * 1024;
        nc.persistent_capacity = 512ull * 1024 * 1024;
        nc.heartbeat_interval_ms = 500;
        NodeRuntime node(nc, nt);
        if (!node.start()) return 1;

        ContextFabric cf;
        if (!cf.connect("127.0.0.1", port, 5000)) return 1;
        uint64_t n = NodeId::deterministic("persist-node").value();

        ContextMetadata md;
        md.namespace_id = "example";
        md.semantic_class = SemanticClass::Checkpoint;
        md.byte_size = content.size();
        md.logical_size = content.size();
        md.sharing = SharingPolicy::NamespaceShared;
        md.persistence = PersistencePolicy::Persistent;
        md.compatibility_fingerprint = md.compute_compatibility_fingerprint();

        id = cf.create_object(md, n, content);
        std::printf("created %s\n", id.to_hex().c_str());
        cf.persist(id, n);
        std::printf("persisted\n");
    }
    // Fabric torn down. The node store root and coordinator store survive.

    {
        Telemetry ct;
        ct.set_stderr_enabled(false);
        Coordinator::Config cc;
        cc.bind_host = "127.0.0.1";
        cc.control_port = 0;
        cc.store_path = coord_store;
        cc.telemetry_path = "";
        Coordinator coord(cc, ct);
        if (!coord.start()) return 1;
        uint16_t port = coord.bound_port();

        Telemetry nt;
        nt.set_stderr_enabled(false);
        NodeConfig nc;
        nc.node_id = NodeId::deterministic("persist-node");
        nc.coord_host = "127.0.0.1";
        nc.coord_port = port;
        nc.store_root = node_store;
        nc.host_capacity = 256ull * 1024 * 1024;
        nc.persistent_capacity = 512ull * 1024 * 1024;
        nc.heartbeat_interval_ms = 500;
        NodeRuntime node(nc, nt);
        if (!node.start()) return 1;

        ContextFabric cf;
        if (!cf.connect("127.0.0.1", port, 5000)) return 1;
        uint64_t n = NodeId::deterministic("persist-node").value();

        auto ir = cf.inspect(id);
        std::printf("after restart: lifecycle=%s authoritative=%llu\n",
                    lifecycle_state_name(static_cast<LifecycleState>(ir.lifecycle)),
                    static_cast<unsigned long long>(ir.authoritative_node));

        auto res = cf.restore(id, n);
        std::printf("restored to node %llu\n", static_cast<unsigned long long>(res.node_id));

        auto vr = cf.verify(id, n, true);
        std::printf("verify after restore: %s (bytes=%llu)\n", vr.ok ? "OK" : "FAIL",
                    static_cast<unsigned long long>(vr.bytes));
        fs::remove_all(node_store);
        fs::remove(coord_store);
        return vr.ok ? 0 : 1;
    }
}