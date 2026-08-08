#include "fabric_launcher.h"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <thread>

#include "context_fabric/api/context_fabric.h"
#include "context_fabric/authority/client.h"
#include "context_fabric/core/byte_size_util.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/core/version.h"
#if defined(CONTEXT_FABRIC_BUILD_BENCHMARKS)
#include "bench_suite.h"
#endif
#include "cli.h"

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

namespace ctf {

namespace fs = std::filesystem;

namespace {

// Wait until a TCP connection to host:port succeeds (coordinator ready).
bool wait_ready(const std::string& host, uint16_t port, uint64_t timeout_ms) {
    uint64_t waited = 0;
    while (waited < timeout_ms) {
        ClientSession probe;
        if (probe.connect(host, port, 2000)) {
            try {
                probe.hello();
                return true;
            } catch (const CfError&) {
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        waited += 100;
    }
    return false;
}

uint16_t pick_free_port() {
    Socket s;
    uint16_t port = 0;
    if (s.listen("127.0.0.1", 0, port)) {
        s.close();
        return port;
    }
    return 0;
}

}  // namespace

std::string FabricLauncher::exe_path(const std::string& argv0_hint) const {
    // Prefer the current executable path (argv[0]).
    if (!argv0_hint.empty()) {
        std::error_code ec;
        fs::path p = fs::absolute(argv0_hint, ec);
        if (!ec && fs::exists(p)) return p.string();
    }
    return "context-fabric";
}

int FabricLauncher::run(const Config& cfg) {
    // Determine the executable path.
#if defined(_WIN32)
    char buf[4096];
    DWORD n = GetModuleFileNameA(nullptr, buf, sizeof(buf));
    std::string exe(n > 0 ? buf : "");
#else
    std::string exe = "";
#endif
    if (exe.empty()) exe = "context-fabric";

    // Create a root directory.
    std::string root = cfg.root;
    bool temp_root = root.empty();
    if (temp_root) {
        root = (fs::temp_directory_path() / ("context-fabric-" + std::to_string(current_pid()))).string();
    }
    std::error_code mkec;
    fs::create_directories(root, mkec);
    if (mkec) {
        std::fprintf(stderr, "fabric: cannot create root directory %s\n", root.c_str());
        return 1;
    }

    uint16_t coord_port = cfg.coord_port != 0 ? static_cast<uint16_t>(cfg.coord_port) : pick_free_port();
    if (coord_port == 0) {
        std::fprintf(stderr, "fabric: cannot pick a coordinator port\n");
        return 1;
    }

    std::printf("context-fabric: launching local fabric\n");
    std::printf("  coordinator port: %u\n", static_cast<unsigned>(coord_port));
    std::printf("  nodes:            %zu\n", cfg.node_count);
    std::printf("  cuda node:        %s\n", cfg.enable_cuda_node ? "yes" : "no");
    std::printf("  root:             %s\n", root.c_str());
    std::fflush(stdout);

    std::vector<ProcessHandle> procs;
    std::string coord_store = (fs::path(root) / "coordinator.store").string();
    std::string coord_telemetry = (fs::path(root) / "coordinator.jsonl").string();
    std::string coord_log = (fs::path(root) / "coordinator.log").string();

    // Coordinator.
    ProcessHandle coord = spawn_process(
        exe, {"coordinator", "--port", std::to_string(coord_port), "--store", coord_store,
              "--telemetry", coord_telemetry, "--quiet"},
        coord_log, root);
    if (!coord.valid()) {
        std::fprintf(stderr, "fabric: cannot spawn coordinator\n");
        return 1;
    }
    procs.push_back(coord);

    if (!wait_ready("127.0.0.1", coord_port, 15000)) {
        std::fprintf(stderr, "fabric: coordinator did not become ready\n");
        for (auto& p : procs) terminate_process(p);
        return 1;
    }

    // Nodes.
    std::vector<uint64_t> node_ids;
    const std::string caps_default = "256MiB";
    for (size_t i = 0; i < cfg.node_count; ++i) {
        std::string name = "node-" + std::string(1, static_cast<char>('a' + i));
        std::string node_root = (fs::path(root) / name).string();
        fs::create_directories(node_root);
        std::vector<std::string> args = {
            "node", "--coordinator", "127.0.0.1:" + std::to_string(coord_port),
            "--id", name,
            "--store", node_root,
            "--host-cap", caps_default,
            "--persist-cap", "1GiB",
            "--accel-cap", "6GiB",
            "--telemetry", (fs::path(root) / (name + ".jsonl")).string(),
            "--quiet",
        };
        if (cfg.enable_cuda_node && i == 0) args.push_back("--cuda");
        ProcessHandle p = spawn_process(exe, args, (fs::path(root) / (name + ".log")).string(), root);
        if (!p.valid()) {
            std::fprintf(stderr, "fabric: cannot spawn node %s\n", name.c_str());
            for (auto& pr : procs) terminate_process(pr);
            return 1;
        }
        procs.push_back(p);
        node_ids.push_back(NodeId::deterministic(name).value());
    }

    // Wait for all nodes to register.
    bool registered = false;
    {
        ClientSession mon;
        if (mon.connect("127.0.0.1", coord_port, 2000)) {
            for (int i = 0; i < 200; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                // Count online nodes via list of registered nodes is not
                // exposed; use a placeholder check below.
            }
        }
    }
    // Robust registration check: try to create a trivial probe through the API.
    // We instead wait a fixed settling time and then run the workload which
    // will surface any missing node.
    std::this_thread::sleep_for(std::chrono::milliseconds(cfg.quick ? 800 : 1500));
    registered = true;
    (void)registered;

    // Run the workload.
    int result = 0;
    try {
        ContextFabric cf;
        if (!cf.connect("127.0.0.1", coord_port, 5000)) {
            std::fprintf(stderr, "fabric: cannot connect to coordinator\n");
            result = 1;
        } else {
            uint64_t size = cfg.workload_size != 0 ? cfg.workload_size
                             : (cfg.quick ? 2ull * 1024 * 1024 : 16ull * 1024 * 1024);
            std::printf("fabric: workload '%s' size=%llu\n", cfg.workload.c_str(),
                        static_cast<unsigned long long>(size));

            ContextMetadata md;
            md.namespace_id = "fabric";
            md.semantic_class = SemanticClass::KvCache;
            md.byte_size = size;
            md.logical_size = size;
            md.sharing = SharingPolicy::NamespaceShared;
            md.persistence = PersistencePolicy::Persistent;
            md.recompute_seconds = 2.0;
            md.recompute_resource_cost = 1.0;
            md.producer = "fabric-launcher";
            md.model_id = "workload-model";
            md.compatibility_fingerprint = md.compute_compatibility_fingerprint();

            auto content = generate_content(42, 7, size);

            // create on node-a
            ObjectId id = cf.create_object(md, node_ids[0], content);
            std::printf("fabric: created %s on node %llu\n", id.to_hex().c_str(),
                        static_cast<unsigned long long>(node_ids[0]));

            // verify integrity
            auto vr = cf.verify(id, node_ids[0], true);
            std::printf("fabric: verify authoritative: %s (crc=%016llx sha=%016llx)\n",
                        vr.ok ? "OK" : "FAIL", static_cast<unsigned long long>(vr.crc32c),
                        static_cast<unsigned long long>(vr.sha));
            if (!vr.ok) result = 1;

            // replicate to node-b
            if (cfg.node_count >= 2) {
                auto loc = cf.replicate(id, node_ids[1]);
                std::printf("fabric: replicated to node %llu\n", static_cast<unsigned long long>(loc.node_id));
                auto vr2 = cf.verify(id, node_ids[1], true);
                std::printf("fabric: verify replica: %s\n", vr2.ok ? "OK" : "FAIL");
                if (!vr2.ok) result = 1;
            }

            // migrate to node-c (keep source)
            size_t last = cfg.node_count - 1;
            if (cfg.node_count >= 3) {
                auto loc = cf.migrate(id, node_ids[2], StorageDomain::HostMemory, true);
                std::printf("fabric: migrated to node %llu\n", static_cast<unsigned long long>(loc.node_id));
                auto ir = cf.inspect(id);
                std::printf("fabric: authoritative now node %llu lifecycle=%s\n",
                            static_cast<unsigned long long>(ir.authoritative_node),
                            lifecycle_state_name(static_cast<LifecycleState>(ir.lifecycle)));
            }

            // persist + restore
            uint64_t pnode = cf.persist(id, node_ids[last]);
            std::printf("fabric: persisted on node %llu\n", static_cast<unsigned long long>(pnode));
            auto loc = cf.restore(id, node_ids[0]);
            std::printf("fabric: restored to node %llu\n", static_cast<unsigned long long>(loc.node_id));

            // transfer versus recompute decision
            auto dec = cf.decide_transfer_vs_recompute(md, node_ids[0], node_ids[last], 1e9);
            std::printf("fabric: decision=%s reason=%s\n", dec.chose_transfer ? "transfer" : "recompute",
                        dec.reason.c_str());

            std::printf("fabric: workload completed\n");
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "fabric: workload failed: %s\n", e.what());
        result = 1;
    }

    // Graceful shutdown: ask the coordinator to shut each node down.
    ClientSession admin;
    if (admin.connect("127.0.0.1", coord_port, 2000)) {
        for (uint64_t nid : node_ids) {
            try {
                admin.shutdown_node(nid);
            } catch (const CfError&) {
            }
        }
    }

    // Wait for node processes to exit.
    for (size_t i = 1; i < procs.size(); ++i) {
        int code = wait_process(procs[i], 15000);
        if (code < 0) {
            terminate_process(procs[i]);
            std::fprintf(stderr, "fabric: node %zu did not exit cleanly\n", i);
        }
    }

    // Stop the coordinator.
    terminate_process(procs[0]);

    if (temp_root && !cfg.retain) {
        std::error_code ec;
        fs::remove_all(root, ec);
    } else if (cfg.retain) {
        std::printf("fabric: retained working directory at %s\n", root.c_str());
    }

    std::printf("fabric: exit code %d\n", result);
    return result;
}

int run_benchmarks(const Args& args, const std::string& host, uint16_t port, bool have_coord) {
#if defined(CONTEXT_FABRIC_BUILD_BENCHMARKS)
    BenchConfig cfg;
    if (have_coord) {
        cfg.coord_host = host;
        cfg.coord_port = port;
    }
    cfg.quick = args.has("quick");
    cfg.cuda = args.has("cuda");
    if (args.has("size")) {
        auto s = args.value("size");
        if (s) {
            uint64_t v = 0;
            if (parse_byte_size(*s, v)) cfg.object_size = v;
        }
    }
    return run_benchmark_suite(cfg);
#else
    (void)args;
    (void)host;
    (void)port;
    (void)have_coord;
    std::printf("context-fabric benchmarks: build with CONTEXT_FABRIC_BUILD_BENCHMARKS=ON\n");
    return 0;
#endif
}

}  // namespace ctf