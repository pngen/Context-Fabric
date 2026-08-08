#pragma once

#include <cstdint>
#include <string>

#include "cli_args.h"

namespace ctf {

// CLI entry point (implemented in cli.cpp).
int cli_main(int argc, char** argv);

// Benchmark runner (implemented in fabric_launcher.cpp).
// Returns an exit code. `have_coord` indicates a coordinator was specified.
int run_benchmarks(const Args& args, const std::string& host, uint16_t port, bool have_coord);

// Cross-platform current process id helper.
[[nodiscard]] uint64_t current_pid();

}  // namespace ctf