// minimal_object: create and retrieve a generic computational-state object
// through the public API.
//
// Usage: example_minimal_object [coordinator_host:port]
// (Connects to a running coordinator; if none is given, starts an in-process
// coordinator + one node so the example runs standalone.)

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
        std::fprintf(stderr, "usage: example_minimal_object [coordinator_host:port]\n");
        return 2;
    }
    if (argc == 2 && !parse_example_endpoint(argv[1], host, port)) {
        std::fprintf(stderr, "error: coordinator must be a valid host:port\n");
        return 2;
    }

    std::unique_ptr<ExampleTempDirectory> workspace;
    std::unique_ptr<Telemetry> coord_telemetry;
    std::unique_ptr<Coordinator> coord;
    std::unique_ptr<Telemetry> node_telemetry;
    std::unique_ptr<NodeRuntime> node;
    try {
        if (port == 0) {
            workspace = std::make_unique<ExampleTempDirectory>("minimal");
            std::printf("starting in-process coordinator + node\n");
            coord_telemetry = std::make_unique<Telemetry>();
            coord_telemetry->set_stderr_enabled(false);
            Coordinator::Config cc;
            cc.bind_host = "127.0.0.1";
            cc.control_port = 0;
            cc.store_path = (workspace->path() / "coordinator.store").string();
            cc.telemetry_path = "";
            coord = std::make_unique<Coordinator>(cc, *coord_telemetry);
            if (!coord->start()) {
                std::printf("coordinator failed to start\n");
                return 1;
            }
            port = coord->bound_port();

            node_telemetry = std::make_unique<Telemetry>();
            node_telemetry->set_stderr_enabled(false);
            NodeConfig nc;
            nc.node_id = NodeId::deterministic("minimal-node");
            nc.label = "minimal-node";
            nc.coord_host = "127.0.0.1";
            nc.coord_port = port;
            nc.host_capacity = 64ull * 1024 * 1024;
            nc.heartbeat_interval_ms = 500;
            node = std::make_unique<NodeRuntime>(nc, *node_telemetry);
            if (!node->start()) {
                std::printf("node failed to start\n");
                return 1;
            }
        }

        ContextFabric cf;
        if (!cf.connect(host, port, 5000)) {
            std::printf("cannot connect to coordinator\n");
            return 1;
        }

    NodeId nid = NodeId::deterministic("minimal-node");
    ContextMetadata md;
    md.namespace_id = "example";
    md.semantic_class = SemanticClass::Generic;
    md.byte_size = 4096;
    md.logical_size = 4096;
    md.sharing = SharingPolicy::NamespaceShared;
    md.producer = "minimal-object-example";
    md.model_id = "example-model";
    md.compatibility_fingerprint = md.compute_compatibility_fingerprint();

    auto content = generate_content(1, 1, 4096);
        ObjectId id = create_object_wait_registered(cf, md, nid.value(), content);
        std::printf("created object %s\n", id.to_hex().c_str());

        auto ir = cf.inspect(id);
        if (!ir.has) throw CfError(ErrKind::NotFound, "created object cannot be inspected");

        auto vr = cf.verify(id, nid.value(), true);
        std::printf("verify: %s (crc=%016llx)\n", vr.ok ? "OK" : "FAIL",
                    static_cast<unsigned long long>(vr.crc32c));
        if (!vr.ok) throw CfError(ErrKind::Integrity, "verification failed");

        cf.remove(id);
        std::printf("removed object\n");
    } catch (const std::exception& e) {
        std::fprintf(stderr, "example error: %s\n", e.what());
        return 1;
    }
    if (node) {
        node->shutdown();
        node.reset();
    }
    if (coord) {
        coord->shutdown();
        coord.reset();
    }
    return 0;
}
