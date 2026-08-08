#include "test_harness.h"

#include <limits>
#include <vector>

#include "context_fabric/placement/placement_engine.h"
#include "context_fabric/placement/cost_model.h"

using namespace ctf;

namespace {

ContextMetadata meta_with(uint64_t bytes, double recompute, double reuse) {
    ContextMetadata md;
    md.byte_size = bytes;
    md.logical_size = bytes;
    md.recompute_seconds = recompute;
    md.reuse_probability = reuse;
    return md;
}

std::vector<PlacementCandidate> candidates(uint64_t n1_free, uint64_t n1_total, uint64_t n2_free,
                                           uint64_t n2_total) {
    std::vector<PlacementCandidate> out;
    PlacementCandidate a;
    a.location.node_id = 1;
    a.location.domain = StorageDomain::HostMemory;
    a.caps.host_memory = true;
    a.used_bytes = n1_total > n1_free ? n1_total - n1_free : 0;
    a.total_bytes = n1_total;
    a.bandwidth_bps = 1e9;
    out.push_back(a);
    PlacementCandidate b;
    b.location.node_id = 2;
    b.location.domain = StorageDomain::HostMemory;
    b.caps.host_memory = true;
    b.used_bytes = n2_total > n2_free ? n2_total - n2_free : 0;
    b.total_bytes = n2_total;
    b.bandwidth_bps = 1e9;
    out.push_back(b);
    return out;
}

}  // namespace

CF_TEST(placement_baseline_chooses_current) {
    PlacementEngine engine;
    auto md = meta_with(1000, 1.0, 0.0);
    auto cands = candidates(100000, 100000, 100000, 100000);
    Location current;
    current.node_id = 2;
    current.domain = StorageDomain::HostMemory;
    auto d = engine.decide(md, current, cands, "baseline");
    CF_CHECK(d.location.node_id == 2);
    CF_CHECK(d.policy == std::string("baseline"));
}

CF_TEST(placement_baseline_largest_headroom) {
    PlacementEngine engine;
    auto md = meta_with(1000, 1.0, 0.0);
    auto cands = candidates(10000, 100000, 50000, 100000);
    Location current;  // invalid: new object
    auto d = engine.decide(md, current, cands, "baseline");
    CF_CHECK(d.location.node_id == 2);  // more free headroom
}

CF_TEST(placement_deterministic_stable_tiebreak) {
    PlacementEngine engine;
    auto md = meta_with(1000, 1.0, 0.0);
    auto d1 = engine.decide(md, Location(), candidates(100000, 100000, 100000, 100000), "baseline");
    auto d2 = engine.decide(md, Location(), candidates(100000, 100000, 100000, 100000), "baseline");
    CF_CHECK(d1.location.node_id == d2.location.node_id);
    CF_CHECK(d1.location.domain == d2.location.domain);
}

CF_TEST(placement_capacity_headroom_required) {
    PlacementEngine engine;
    auto md = meta_with(100000, 1.0, 0.0);
    // Node2 has only 1000 free bytes; must be excluded.
    auto cands = candidates(100000, 100000, 1000, 100000);
    auto d = engine.decide(md, Location(), cands, "cost-aware");
    CF_CHECK(d.location.node_id == 1);
}

CF_TEST(placement_no_candidate_throws) {
    PlacementEngine engine;
    auto md = meta_with(1000000, 1.0, 0.0);
    auto cands = candidates(1000, 1000, 1000, 1000);
    CF_CHECK_THROWS(engine.decide(md, Location(), cands, "baseline"), ErrKind::NotFound);
}

CF_TEST(placement_cost_aware_prefers_staying) {
    PlacementEngine engine;
    auto md = meta_with(10000, 1.0, 0.0);
    auto cands = candidates(100000, 100000, 100000, 100000);
    Location current;
    current.node_id = 2;
    current.domain = StorageDomain::HostMemory;
    auto d = engine.decide(md, current, cands, "cost-aware");
    CF_CHECK(d.location.node_id == 2);
    CF_CHECK(d.transfer.expected_seconds == 0.0);
}

CF_TEST(placement_capacity_arithmetic_cannot_wrap) {
    PlacementEngine engine;
    auto md = meta_with(16, 1.0, 0.0);
    auto cands = candidates(1000, 1000, 1000, 1000);
    cands[0].used_bytes = std::numeric_limits<uint64_t>::max() - 8;
    auto d = engine.decide(md, Location(), cands, "baseline");
    CF_CHECK(d.location.node_id == 2);
}

CF_TEST(placement_current_location_needs_no_duplicate_capacity) {
    PlacementEngine engine;
    auto md = meta_with(1000, 1.0, 0.0);
    auto cands = candidates(0, 1000, 0, 1000);
    Location current;
    current.node_id = 2;
    current.domain = StorageDomain::HostMemory;
    auto d = engine.decide(md, current, cands, "baseline");
    CF_CHECK(d.location.node_id == 2);
}

CF_TEST(placement_rejects_non_finite_metrics) {
    PlacementEngine engine;
    auto md = meta_with(1000, 1.0, 0.0);
    auto cands = candidates(1000, 1000, 1000, 1000);
    cands[0].bandwidth_bps = std::numeric_limits<double>::quiet_NaN();
    CF_CHECK_THROWS(engine.decide(md, Location(), cands, "cost-aware"), ErrKind::InvalidArgument);
}

CF_TEST(placement_rejects_invalid_candidate_and_metadata) {
    PlacementEngine engine;
    auto md = meta_with(1000, 1.0, 0.0);
    auto cands = candidates(1000, 1000, 1000, 1000);
    cands[0].location.node_id = 0;
    CF_CHECK_THROWS(engine.decide(md, Location(), cands, "baseline"), ErrKind::InvalidArgument);

    cands = candidates(1000, 1000, 1000, 1000);
    md.reuse_probability = 1.01;
    CF_CHECK_THROWS(engine.decide(md, Location(), cands, "cost-aware"), ErrKind::InvalidArgument);
}

CF_TEST(transfer_vs_recompute_cheap_recompute) {
    TransferVsRecompute planner;
    // Expensive to transfer (large, low bandwidth), cheap to recompute.
    ContextMetadata md;
    md.byte_size = 100ull * 1024 * 1024;
    md.recompute_seconds = 0.001;
    auto d = planner.decide(md, 1, 2, 1e6);
    CF_CHECK(!d.chose_transfer);
    CF_CHECK(d.reason.find("recompute") != std::string::npos);
}

CF_TEST(transfer_vs_recompute_expensive_recompute) {
    TransferVsRecompute planner;
    // Cheap to transfer, expensive to recompute.
    ContextMetadata md;
    md.byte_size = 1024;
    md.recompute_seconds = 100.0;
    md.recompute_resource_cost = 100.0;
    auto d = planner.decide(md, 1, 2, 1e9);
    CF_CHECK(d.chose_transfer);
    CF_CHECK(d.reason.find("transfer") != std::string::npos);
}

CF_TEST(transfer_vs_recompute_deterministic) {
    TransferVsRecompute planner;
    ContextMetadata md;
    md.byte_size = 4096;
    md.recompute_seconds = 1.0;
    auto d1 = planner.decide(md, 1, 2, 1e8);
    auto d2 = planner.decide(md, 1, 2, 1e8);
    CF_CHECK(d1.chose_transfer == d2.chose_transfer);
    CF_CHECK(d1.score == d2.score);
    CF_CHECK(d1.reason == d2.reason);
}

CF_TEST(transfer_vs_recompute_hot_state_prefers_transfer) {
    TransferVsRecompute planner;
    // High reuse: transferring once and keeping pays off vs recomputing every use.
    ContextMetadata md;
    md.byte_size = 1024;
    md.recompute_seconds = 0.5;
    md.recompute_resource_cost = 0.5;
    TransferVsRecompute hot(ReusePolicy{1.0, 1.0, 1e9, 100});  // 100 expected reuses
    auto d = hot.decide(md, 1, 2, 1e9);
    CF_CHECK(d.chose_transfer);
}

CF_TEST(transfer_vs_recompute_rejects_invalid_numeric_inputs) {
    TransferVsRecompute planner;
    ContextMetadata md;
    md.byte_size = 1024;
    md.recompute_seconds = 1.0;
    CF_CHECK_THROWS(planner.decide(md, 1, 2, -1.0), ErrKind::InvalidArgument);
    CF_CHECK_THROWS(planner.decide(md, 1, 2, std::numeric_limits<double>::quiet_NaN()),
                    ErrKind::InvalidArgument);
    TransferVsRecompute zero_reuses(ReusePolicy{1.0, 1.0, 1e9, 0});
    CF_CHECK_THROWS(zero_reuses.decide(md, 1, 2, 1e9), ErrKind::InvalidArgument);
}

CF_TEST(transfer_vs_recompute_discloses_reference_bandwidth_fallback) {
    TransferVsRecompute planner;
    ContextMetadata md;
    md.byte_size = 1024;
    md.recompute_seconds = 1.0;
    auto d = planner.decide(md, 1, 2, 0.0);
    CF_CHECK(d.reason.find("reference") != std::string::npos);
}

CF_TEST(transfer_vs_recompute_same_node_has_no_transfer_cost) {
    TransferVsRecompute planner;
    ContextMetadata md;
    md.byte_size = 100ull * 1024 * 1024;
    md.recompute_seconds = 0.001;
    auto d = planner.decide(md, 7, 7, 1.0);
    CF_CHECK(d.chose_transfer);
    CF_CHECK(d.transfer.expected_seconds == 0.0);
    CF_CHECK(d.transfer.expected_bytes == 0.0);
    CF_CHECK(d.transfer.resource_cost == 0.0);
    CF_CHECK_THROWS(planner.decide(md, 0, 7, 1e9), ErrKind::InvalidArgument);
}
