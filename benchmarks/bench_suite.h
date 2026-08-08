#pragma once

#include <cstdint>
#include <string>

namespace ctf {

// ---------------------------------------------------------------------------
// Benchmark suite. Every benchmark prints its exact configuration before it
// runs, uses deterministic seeded content, and reports measured values.
// Benchmarks run against a live fabric: either one you start (pass host/port)
// or an in-process fabric created by the runner.
// ---------------------------------------------------------------------------
struct BenchConfig {
    std::string coord_host = "127.0.0.1";
    uint16_t coord_port = 0;   // 0 = start an in-process fabric
    uint64_t object_size = 16ull * 1024 * 1024;  // default object size
    bool quick = false;        // smaller/faster runs
    bool cuda = false;         // include CUDA benchmarks
};

// Runs the full suite; returns 0 on success.
int run_benchmark_suite(const BenchConfig& cfg);

// Individual suites (exposed for targeted runs).
void bench_local_reuse(const BenchConfig& cfg);
void bench_remote_reuse(const BenchConfig& cfg);
void bench_replication(const BenchConfig& cfg);
void bench_migration(const BenchConfig& cfg);
void bench_persist_restore(const BenchConfig& cfg);
void bench_placement(const BenchConfig& cfg);
void bench_transfer_vs_recompute(const BenchConfig& cfg);
void bench_hot_cold(const BenchConfig& cfg);
void bench_failure_recovery(const BenchConfig& cfg);
void bench_cuda_roundtrip(const BenchConfig& cfg);
void bench_cuda_remote_path(const BenchConfig& cfg);
void bench_capacity_pressure(const BenchConfig& cfg);
void bench_multiprocess_fabric(const BenchConfig& cfg);

}  // namespace ctf