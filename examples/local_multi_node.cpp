// local_multi_node: launch/connect multiple nodes and replicate an object.
//
// Usage: example_local_multi_node [coordinator_host:port]
// If no coordinator is given, starts an in-process coordinator + 3 nodes.

#include <cstdio>
#include <exception>
#include <filesystem>
#include <memory>

#include "context_fabric/api/context_fabric.h"
#include "context_fabric/authority/coordinator.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/runtime/node_runtime.h"
#include "example_util.h"

using namespace ctf;

int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    uint16_t port = 0;
    if (argc > 2) {
        std::fprintf(stderr, "usage: example_local_multi_node [coordinator_host:port]\n");
        return 2;
    }
    if (argc == 2 && !parse_example_endpoint(argv[1], host, port)) {
        std::fprintf(stderr, "error: coordinator must be a valid host:port\n");
        return 2;
    }

    std::unique_ptr<ExampleTempDirectory> workspace;
    std::unique_ptr<Telemetry> coord_t;
    std::unique_ptr<Coordinator> coord;
    std::vector<std::unique_ptr<Telemetry>> node_ts;
    std::vector<std::unique_ptr<NodeRuntime>> nodes;
    try {
    if (port == 0) {
        workspace = std::make_unique<ExampleTempDirectory>("multi-node");
        coord_t = std::make_unique<Telemetry>();
        coord_t->set_stderr_enabled(false);
        Coordinator::Config cc;
        cc.bind_host = "127.0.0.1";
        cc.control_port = 0;
        cc.store_path = (workspace->path() / "coordinator.store").string();
        cc.telemetry_path = "";
        coord = std::make_unique<Coordinator>(cc, *coord_t);
        if (!coord->start()) return 1;
        port = coord->bound_port();
        for (int i = 0; i < 3; ++i) {
            auto t = std::make_unique<Telemetry>();
            t->set_stderr_enabled(false);
            NodeConfig nc;
            nc.node_id = NodeId::deterministic("example-node-" + std::to_string(i));
            nc.label = "example-node-" + std::to_string(i);
            nc.coord_host = "127.0.0.1";
            nc.coord_port = port;
            nc.host_capacity = 256ull * 1024 * 1024;
            nc.heartbeat_interval_ms = 500;
            auto n = std::make_unique<NodeRuntime>(nc, *t);
            if (!n->start()) return 1;
            node_ts.push_back(std::move(t));
            nodes.push_back(std::move(n));
        }
    }

    ContextFabric cf;
    if (!cf.connect(host, port, 5000)) return 1;

    uint64_t a = NodeId::deterministic("example-node-0").value();
    uint64_t b = NodeId::deterministic("example-node-1").value();

    ContextMetadata md;
    md.namespace_id = "example";
    md.semantic_class = SemanticClass::KvCache;
    md.byte_size = 4ull * 1024 * 1024;
    md.logical_size = md.byte_size;
    md.sharing = SharingPolicy::NamespaceShared;
    md.persistence = PersistencePolicy::Ephemeral;
    md.compatibility_fingerprint = md.compute_compatibility_fingerprint();

    auto content = generate_content(2, 2, md.byte_size);
    ObjectId id = create_object_wait_registered(cf, md, a, content);
    std::printf("created %s on node-a\n", id.to_hex().c_str());

    replicate_wait_registered(cf, id, b);
    std::printf("replicated to node-b\n");

    auto ir = cf.inspect(id);
    std::printf("replicas:");
    for (uint64_t r : ir.replica_nodes) std::printf(" %llu", static_cast<unsigned long long>(r));
    std::printf("\n");
    std::fflush(stdout);
    bool found_replica = false;
    for (uint64_t replica : ir.replica_nodes) found_replica = found_replica || replica == b;
    if (!found_replica) {
        std::fprintf(stderr, "replica was not recorded on node-b\n");
        return 1;
    }

    // Explicit, deterministic teardown.
    for (auto& n : nodes) {
        if (n) {
            n->shutdown();
            n.reset();
        }
    }
    nodes.clear();
    node_ts.clear();
    if (coord) {
        coord->shutdown();
        coord.reset();
    }
    return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "example error: %s\n", e.what());
        return 1;
    }
}
