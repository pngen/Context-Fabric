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
// shuts the nodes down through the control plane, and deterministically reaps
// the complete owned process trees. The nodes are independent OS processes,
// not in-process objects.
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
};

// Benchmark runner used by `context-fabric benchmark`. Each runtime case owns
// an isolated in-process coordinator and node set.
int run_benchmarks(const Args& args);

}  // namespace ctf
