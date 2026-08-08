#include "fabric_launcher.h"

#include <chrono>
#include <charconv>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <string_view>
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

namespace ctf {

namespace fs = std::filesystem;

namespace {

constexpr size_t kMaxLocalNodes = 64;

class TemporaryRoot {
public:
    TemporaryRoot(std::string path, bool owned, bool retain)
        : path_(std::move(path)), owned_(owned), retain_(retain) {}

    ~TemporaryRoot() {
        if (!owned_ || retain_) return;
        std::error_code ec;
        fs::remove_all(path_, ec);
    }

    TemporaryRoot(const TemporaryRoot&) = delete;
    TemporaryRoot& operator=(const TemporaryRoot&) = delete;

private:
    std::string path_;
    bool owned_;
    bool retain_;
};

std::optional<uint16_t> coordinator_port_from_log(const std::string& log_path) {
    std::ifstream in(log_path);
    if (!in) return std::nullopt;
    std::string line;
    constexpr std::string_view marker = "coordinator listening on ";
    while (std::getline(in, line)) {
        size_t start = line.find(marker);
        if (start == std::string::npos) continue;
        start = line.find(':', start + marker.size());
        if (start == std::string::npos) continue;
        ++start;
        size_t end = line.find_first_not_of("0123456789", start);
        std::string_view digits(line.data() + start,
                                (end == std::string::npos ? line.size() : end) - start);
        uint32_t value = 0;
        auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), value);
        if (parsed.ec == std::errc{} && parsed.ptr == digits.data() + digits.size() && value > 0 &&
            value <= 65535) {
            return static_cast<uint16_t>(value);
        }
    }
    return std::nullopt;
}

// Wait until a TCP connection to host:port succeeds (coordinator ready).
bool wait_ready(const std::string& host, uint16_t port, ProcessHandle& process, uint64_t timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        int process_state = wait_process(process, 0);
        if (process_state != -1) {
            std::fprintf(stderr, "fabric: coordinator exited during startup (status %d)\n", process_state);
            return false;
        }
        ClientSession probe;
        probe.set_timeouts(1000, 1000);
        if (probe.connect(host, port, 500)) {
            try {
                probe.hello();
                return true;
            } catch (const CfError&) {
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

bool wait_for_log_event(const std::string& log_path, std::string_view event, ProcessHandle& process,
                        uint64_t timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        int process_state = wait_process(process, 0);
        if (process_state != -1) {
            std::fprintf(stderr, "fabric: child exited during startup (status %d, log %s)\n", process_state,
                         log_path.c_str());
            return false;
        }
        std::ifstream in(log_path);
        std::string line;
        while (std::getline(in, line)) {
            if (line.find(event) != std::string::npos) return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

}  // namespace

int FabricLauncher::run(const Config& cfg) {
    if (cfg.node_count == 0 || cfg.node_count > kMaxLocalNodes) {
        std::fprintf(stderr, "fabric: --nodes must be between 1 and %zu\n", kMaxLocalNodes);
        return 2;
    }
    if (cfg.coord_port > 65535) {
        std::fprintf(stderr, "fabric: coordinator port is out of range\n");
        return 2;
    }
    if (cfg.workload != "default") {
        std::fprintf(stderr, "fabric: unsupported workload '%s' (only 'default' is available)\n",
                     cfg.workload.c_str());
        return 2;
    }

    std::string exe = current_executable_path();
    if (exe.empty()) {
        std::fprintf(stderr, "fabric: cannot determine the current executable path\n");
        return 1;
    }

    // Create a root directory.
    std::string root = cfg.root;
    bool temp_root = root.empty();
    if (temp_root) {
        root = (fs::temp_directory_path() /
                ("context-fabric-" + std::to_string(current_pid()) + "-" + ObjectId::random().to_hex()))
                   .string();
    }
    std::error_code mkec;
    if (temp_root) {
        (void)fs::create_directory(root, mkec);
    } else {
        (void)fs::create_directories(root, mkec);
    }
    if (mkec) {
        std::fprintf(stderr, "fabric: cannot create root directory %s: %s\n", root.c_str(),
                     mkec.message().c_str());
        return 1;
    }
    TemporaryRoot root_cleanup(root, temp_root, cfg.retain);

    uint16_t coord_port = static_cast<uint16_t>(cfg.coord_port);
    const std::string displayed_port = coord_port == 0 ? "ephemeral" : std::to_string(coord_port);

    std::printf("context-fabric: launching local fabric\n");
    std::printf("  coordinator port: %s\n", displayed_port.c_str());
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
    procs.push_back(std::move(coord));

    if (coord_port == 0) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (std::chrono::steady_clock::now() < deadline && coord_port == 0) {
            int process_state = wait_process(procs[0], 0);
            if (process_state != -1) {
                std::fprintf(stderr, "fabric: coordinator exited during startup (status %d)\n",
                             process_state);
                return 1;
            }
            auto discovered = coordinator_port_from_log(coord_log);
            if (discovered) coord_port = *discovered;
            if (coord_port == 0) std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    if (coord_port == 0 || !wait_ready("127.0.0.1", coord_port, procs[0], 15000)) {
        std::fprintf(stderr, "fabric: coordinator did not become ready\n");
        return 1;
    }
    std::printf("  coordinator bound: %u\n", static_cast<unsigned>(coord_port));
    std::fflush(stdout);

    // Nodes.
    std::vector<uint64_t> node_ids;
    const std::string caps_default = "256MiB";
    for (size_t i = 0; i < cfg.node_count; ++i) {
        std::string name = "node-" + std::to_string(i);
        std::string node_root = (fs::path(root) / name).string();
        std::error_code node_ec;
        (void)fs::create_directories(node_root, node_ec);
        if (node_ec) {
            std::fprintf(stderr, "fabric: cannot create node directory %s: %s\n", node_root.c_str(),
                         node_ec.message().c_str());
            return 1;
        }
        std::string node_log = (fs::path(root) / (name + ".log")).string();
        std::vector<std::string> args = {
            "node", "--coordinator", "127.0.0.1:" + std::to_string(coord_port),
            "--id", name,
            "--store", node_root,
            "--host-cap", caps_default,
            "--persist-cap", "1GiB",
            "--accel-cap", "6GiB",
            "--telemetry", (fs::path(root) / (name + ".jsonl")).string(),
        };
        if (cfg.enable_cuda_node && i == 0) args.push_back("--cuda");
        ProcessHandle p = spawn_process(exe, args, node_log, root);
        if (!p.valid()) {
            std::fprintf(stderr, "fabric: cannot spawn node %s\n", name.c_str());
            return 1;
        }
        procs.push_back(std::move(p));
        node_ids.push_back(NodeId::deterministic(name).value());
        if (!wait_for_log_event(node_log, "\"event\":\"node.registered\"", procs.back(), 15000)) {
            std::fprintf(stderr, "fabric: node %s did not register in time\n", name.c_str());
            return 1;
        }
    }

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

            // Create on the first node.
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

            if (cfg.enable_cuda_node) {
                auto cuda_loc = cf.migrate(id, node_ids[0], StorageDomain::AcceleratorLocal, true);
                auto cuda_verify = cf.verify(id, node_ids[0], true);
                bool cuda_ok = cuda_loc.domain == StorageDomain::AcceleratorLocal && cuda_verify.ok;
                std::printf("fabric: CUDA residency and verification: %s\n", cuda_ok ? "OK" : "FAIL");
                if (!cuda_ok) result = 1;
            }

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
    admin.set_timeouts(2000, 2000);
    if (admin.connect("127.0.0.1", coord_port, 2000)) {
        for (uint64_t nid : node_ids) {
            try {
                admin.shutdown_node(nid);
            } catch (const CfError&) {
            }
        }
    }

    // Wait for all node processes against one global shutdown deadline.
    auto shutdown_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    for (size_t i = 1; i < procs.size(); ++i) {
        auto now = std::chrono::steady_clock::now();
        uint64_t remaining_ms = now >= shutdown_deadline
                                    ? 0
                                    : static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                                  shutdown_deadline - now)
                                                                  .count());
        int code = wait_process(procs[i], remaining_ms);
        if (code < 0) {
            bool reaped = terminate_process(procs[i]);
            std::fprintf(stderr, "fabric: node %zu did not exit cleanly\n", i);
            if (!reaped) std::fprintf(stderr, "fabric: node %zu process tree exit was not confirmed\n", i);
            result = 1;
        } else if (code != 0) {
            std::fprintf(stderr, "fabric: node %zu exited with status %d\n", i, code);
            result = 1;
        }
    }

    // Stop the coordinator.
    if (!terminate_process(procs[0])) {
        std::fprintf(stderr, "fabric: coordinator process tree exit was not confirmed\n");
        result = 1;
    }

    if (temp_root && !cfg.retain) {
        std::error_code ec;
        fs::remove_all(root, ec);
        if (ec) {
            std::fprintf(stderr, "fabric: cannot remove temporary root %s: %s\n", root.c_str(),
                         ec.message().c_str());
            result = 1;
        }
    } else if (cfg.retain) {
        std::printf("fabric: retained working directory at %s\n", root.c_str());
    }

    std::printf("fabric: exit code %d\n", result);
    return result;
}

int run_benchmarks(const Args& args) {
#if defined(CONTEXT_FABRIC_BUILD_BENCHMARKS)
#if !defined(CONTEXT_FABRIC_HAS_CUDA)
    if (args.has("cuda")) {
        std::fprintf(stderr, "context-fabric benchmarks: --cuda requested, but CUDA is not compiled in\n");
        return 2;
    }
#endif
    BenchConfig cfg;
    cfg.quick = args.has("quick");
    cfg.cuda = args.has("cuda");
    if (auto size = args.value("size")) {
        uint64_t value = 0;
        if (!parse_byte_size(*size, value) || value == 0 ||
            value > std::numeric_limits<uint64_t>::max() / 4) {
            std::fprintf(stderr, "context-fabric benchmarks: invalid --size '%s'\n", size->c_str());
            return 2;
        }
        cfg.object_size = value;
    }
    return run_benchmark_suite(cfg);
#else
    (void)args;
    std::fprintf(stderr, "context-fabric benchmarks: build with CONTEXT_FABRIC_BUILD_BENCHMARKS=ON\n");
    return 2;
#endif
}

}  // namespace ctf
