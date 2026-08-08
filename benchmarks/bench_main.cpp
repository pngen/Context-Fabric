#include <cstdio>
#include <exception>
#include <limits>
#include <string>

#include "bench_suite.h"
#include "context_fabric/core/byte_size_util.h"

int main(int argc, char** argv) {
    ctf::BenchConfig cfg;
    bool seen_quick = false;
    bool seen_cuda = false;
    bool seen_size = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--quick") {
            if (seen_quick) {
                std::fprintf(stderr, "error: duplicate --quick\n");
                return 2;
            }
            seen_quick = true;
            cfg.quick = true;
        } else if (arg == "--cuda") {
            if (seen_cuda) {
                std::fprintf(stderr, "error: duplicate --cuda\n");
                return 2;
            }
            seen_cuda = true;
            cfg.cuda = true;
        } else if (arg == "--size" || arg.rfind("--size=", 0) == 0) {
            if (seen_size) {
                std::fprintf(stderr, "error: duplicate --size\n");
                return 2;
            }
            seen_size = true;
            std::string text;
            if (arg == "--size") {
                if (i + 1 >= argc) {
                    std::fprintf(stderr, "error: --size requires a value\n");
                    return 2;
                }
                text = argv[++i];
            } else {
                text = arg.substr(7);
            }
            uint64_t v = 0;
            if (!ctf::parse_byte_size(text, v) || v == 0 ||
                v > std::numeric_limits<uint64_t>::max() / 4) {
                std::fprintf(stderr, "error: invalid --size '%s'\n", text.c_str());
                return 2;
            }
            cfg.object_size = v;
        } else if (arg == "--help" || arg == "-h") {
            if (argc != 2) {
                std::fprintf(stderr, "error: --help does not accept additional arguments\n");
                return 2;
            }
            std::printf("usage: context_fabric_bench [--quick] [--cuda] [--size SIZE]\n");
            return 0;
        } else {
            std::fprintf(stderr, "error: unknown argument '%s'\n", arg.c_str());
            return 2;
        }
    }
    try {
        return ctf::run_benchmark_suite(cfg);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "benchmark exception: %s\n", e.what());
        return 1;
    }
}
