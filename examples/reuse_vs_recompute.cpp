// reuse_vs_recompute: show the planner choosing between transferring reusable
// state and synthetic recomputation.

#include <cstdio>

#include "context_fabric/placement/cost_model.h"

using namespace ctf;

int main() {
    TransferVsRecompute planner;

    // Case 1: expensive state (recompute takes 120 s), cheap to transfer.
    ContextMetadata expensive;
    expensive.byte_size = 1024ull * 1024 * 1024;
    expensive.recompute_seconds = 120.0;
    expensive.recompute_resource_cost = 120.0;
    auto d1 = planner.decide(expensive, 1, 2, 1e9);  // 1 GByte/s path
    std::printf("expensive state (1 GiB, 120 s recompute): -> %s\n",
                d1.chose_transfer ? "TRANSFER" : "RECOMPUTE");
    std::printf("  %s\n", d1.reason.c_str());

    // Case 2: cheap state (recompute takes 1 ms), slow transfer path.
    ContextMetadata cheap;
    cheap.byte_size = 1024ull * 1024 * 1024;
    cheap.recompute_seconds = 0.001;
    cheap.recompute_resource_cost = 0.001;
    auto d2 = planner.decide(cheap, 1, 2, 1e5);  // 100 KBytes/s path
    std::printf("cheap state (1 GiB, 1 ms recompute):    -> %s\n",
                d2.chose_transfer ? "TRANSFER" : "RECOMPUTE");
    std::printf("  %s\n", d2.reason.c_str());

    // Case 3: hot state (100 expected reuses) makes transfer pay off.
    TransferVsRecompute hot(ReusePolicy{1.0, 1.0, 1e9, 100});
    ContextMetadata md;
    md.byte_size = 1024ull * 1024;
    md.recompute_seconds = 0.5;
    md.recompute_resource_cost = 0.5;
    auto d3 = hot.decide(md, 1, 2, 1e9);
    std::printf("hot state (1 MiB, 100 reuses):          -> %s\n",
                d3.chose_transfer ? "TRANSFER" : "RECOMPUTE");
    std::printf("  %s\n", d3.reason.c_str());
    bool expected = d1.chose_transfer && !d2.chose_transfer && d3.chose_transfer;
    if (!expected) std::fprintf(stderr, "planner choices did not match the demonstrated scenarios\n");
    return expected ? 0 : 1;
}
