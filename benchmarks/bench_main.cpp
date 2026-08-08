#include <cstdio>
#include <cstring>
#include <exception>

#include "bench_suite.h"
#include "context_fabric/core/byte_size_util.h"

int main(int argc, char** argv) {
    ctf::BenchConfig cfg;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--quick") {
            cfg.quick = true;
        } else if (arg == "--cuda") {
            cfg.cuda = true;
        } else if (arg == "--size" && i + 1 < argc) {
            uint64_t v = 0;
            if (ctf::parse_byte_size(argv[++i], v)) cfg.object_size = v;
        } else if (arg == "--coordinator" && i + 1 < argc) {
            std::string c = argv[++i];
            auto colon = c.rfind(':');
            if (colon != std::string::npos) {
                cfg.coord_host = c.substr(0, colon);
                cfg.coord_port = static_cast<uint16_t>(std::atoi(c.substr(colon + 1).c_str()));
            }
        } else if (arg == "--help" || arg == "-h") {
            std::printf("usage: context_fabric_bench [--quick] [--cuda] [--size BYTES] [--coordinator host:port]\n");
            return 0;
        }
    }
    try {
        return ctf::run_benchmark_suite(cfg);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "benchmark exception: %s\n", e.what());
        return 1;
    }
}