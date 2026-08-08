// migrate_state: move an authoritative object between nodes and verify the
// bytes at the destination.

#include <cstdio>
#include <filesystem>
#include <memory>

#include "context_fabric/api/context_fabric.h"
#include "context_fabric/authority/coordinator.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/runtime/node_runtime.h"
#include "example_util.h"

using namespace ctf;

int main() {
    Telemetry coord_t;
    coord_t.set_stderr_enabled(false);
    Coordinator::Config cc;
    cc.bind_host = "127.0.0.1";
    cc.control_port = 0;
    cc.store_path = (std::filesystem::temp_directory_path() / "cf-migrate-coord.store").string();
    cc.telemetry_path = "";
    Coordinator coord(cc, coord_t);
    if (!coord.start()) return 1;
    uint16_t port = coord.bound_port();

    Telemetry t0, t1;
    t0.set_stderr_enabled(false);
    t1.set_stderr_enabled(false);
    NodeConfig n0;
    n0.node_id = NodeId::deterministic("migrate-a");
    n0.coord_host = "127.0.0.1";
    n0.coord_port = port;
    n0.host_capacity = 256ull * 1024 * 1024;
    n0.heartbeat_interval_ms = 500;
    NodeRuntime node_a(n0, t0);
    NodeConfig n1;
    n1.node_id = NodeId::deterministic("migrate-b");
    n1.coord_host = "127.0.0.1";
    n1.coord_port = port;
    n1.host_capacity = 256ull * 1024 * 1024;
    n1.heartbeat_interval_ms = 500;
    NodeRuntime node_b(n1, t1);
    if (!node_a.start() || !node_b.start()) return 1;

    ContextFabric cf;
    if (!cf.connect("127.0.0.1", port, 5000)) return 1;
    uint64_t a = NodeId::deterministic("migrate-a").value();
    uint64_t b = NodeId::deterministic("migrate-b").value();

    ContextMetadata md;
    md.namespace_id = "example";
    md.semantic_class = SemanticClass::Tensor;
    md.byte_size = 8ull * 1024 * 1024;
    md.logical_size = md.byte_size;
    md.sharing = SharingPolicy::NamespaceShared;
    md.compatibility_fingerprint = md.compute_compatibility_fingerprint();

    auto content = generate_content(3, 3, md.byte_size);
    ObjectId id = create_object_wait_registered(cf, md, a, content);
    std::printf("created %s on node-a\n", id.to_hex().c_str());

    Location loc;
    detail::retry_while_registrating(
        [&] { loc = cf.migrate(id, b, StorageDomain::HostMemory, false); }, "migration");
    std::printf("migrated to node-b (%s)\n", storage_domain_name(loc.domain));

    auto ir = cf.inspect(id);
    std::printf("authoritative now node %llu\n", static_cast<unsigned long long>(ir.authoritative_node));

    auto vr = cf.verify(id, b, true);
    std::printf("verify at destination: %s (bytes=%llu)\n", vr.ok ? "OK" : "FAIL",
                static_cast<unsigned long long>(vr.bytes));
    return vr.ok ? 0 : 1;
}