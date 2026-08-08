#pragma once

#include <cstdint>
#include <string>

namespace ctf {

// ---------------------------------------------------------------------------
// Benchmark suite. Every benchmark prints its exact configuration before it
// runs, uses deterministic seeded content, and reports measured values.
// Runtime benchmarks create and own an in-process fabric for each case.
// ---------------------------------------------------------------------------
struct BenchConfig {
    uint64_t object_size = 16ull * 1024 * 1024;  // default object size
    bool quick = false;        // smaller/faster runs
    bool cuda = false;         // include CUDA benchmarks
};

// Runs the full suite; returns 0 on success.
int run_benchmark_suite(const BenchConfig& cfg);

// Individual suites (exposed for targeted runs).
bool bench_local_reuse(const BenchConfig& cfg);
bool bench_remote_reuse(const BenchConfig& cfg);
bool bench_replication(const BenchConfig& cfg);
bool bench_migration(const BenchConfig& cfg);
bool bench_persist_restore(const BenchConfig& cfg);
bool bench_placement(const BenchConfig& cfg);
bool bench_transfer_vs_recompute(const BenchConfig& cfg);
bool bench_hot_cold(const BenchConfig& cfg);
bool bench_failure_recovery(const BenchConfig& cfg);
bool bench_cuda_roundtrip(const BenchConfig& cfg);
bool bench_cuda_remote_path(const BenchConfig& cfg);
bool bench_capacity_pressure(const BenchConfig& cfg);

}  // namespace ctf
