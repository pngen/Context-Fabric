#pragma once

#include <cstdint>
#include <string>

#include "cli_args.h"
#include "process.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Local multi-process fabric launcher.
//
// Launches a real coordinator process plus N real node processes on one
// machine using localhost TCP, waits for registration, runs a workload, and
// shuts everything down gracefully. This is the primary distributed proof:
// the nodes are independent OS processes, not in-process objects.
// ---------------------------------------------------------------------------
struct FabricLauncher {
    struct Config {
        size_t node_count = 3;
        bool enable_cuda_node = false;
        std::string workload = "default";
        std::string root;              // "" = temp dir
        uint64_t coord_port = 0;       // 0 = ephemeral
        uint64_t workload_size = 0;    // 0 = default per workload
        bool retain = false;
        bool quick = false;
    };

    // Runs the whole fabric and returns a process exit code.
    int run(const Config& cfg);

private:
    std::string exe_path(const std::string& argv0_hint) const;
};

// Benchmark runner used by `context-fabric benchmark`. When `have_coord` is
// false, it launches a temporary fabric itself.
int run_benchmarks(const Args& args, const std::string& host, uint16_t port, bool have_coord);

}  // namespace ctf