#include "context_fabric/placement/cost_model.h"

#include <cstdio>
#include <algorithm>

namespace ctf {

PlacementDecision TransferVsRecompute::decide(const ContextMetadata& meta, uint64_t current_node,
                                              uint64_t target_node, double bandwidth_bps) const {
    (void)current_node;
    PlacementDecision d;
    d.location.node_id = target_node;
    d.location.domain = StorageDomain::HostMemory;

    // Transfer cost: one fetch now plus each future reuse refetch if the state
    // is not pinned at the consumer. Recompute cost: recompute per expected
    // use. Both are expressed in normalized cost units.
    double effective_bw = bandwidth_bps > 0.0 ? bandwidth_bps : policy_.bandwidth_ref_bps;
    double transfer_seconds = static_cast<double>(meta.byte_size) / std::max(effective_bw, 1.0);
    double total_transfer = policy_.time_weight * transfer_seconds +
                            policy_.resource_weight * transfer_seconds * policy_.bandwidth_ref_bps / effective_bw;

    double recompute_seconds = meta.recompute_seconds > 0.0 ? meta.recompute_seconds : 1e9;
    double recompute_cost = meta.recompute_resource_cost > 0.0 ? meta.recompute_resource_cost : recompute_seconds;
    double total_recompute = policy_.time_weight * recompute_seconds * static_cast<double>(policy_.expected_reuses) +
                             policy_.resource_weight * recompute_cost * static_cast<double>(policy_.expected_reuses);

    d.transfer.expected_seconds = transfer_seconds;
    d.transfer.expected_bytes = static_cast<double>(meta.byte_size);
    d.transfer.resource_cost = transfer_seconds;
    d.recompute.expected_seconds = recompute_seconds;
    d.recompute.resource_cost = recompute_cost;

    if (total_transfer <= total_recompute) {
        d.chose_transfer = true;
        d.score = total_transfer;
        char buf[256];
        snprintf(buf, sizeof(buf),
                 "transfer: cost=%.4f (t=%.4fs,reuse=%llu) <= recompute cost=%.4f (t=%.4fs x%llu); bandwidth=%.0f B/s",
                 total_transfer, transfer_seconds,
                 static_cast<unsigned long long>(policy_.expected_reuses), total_recompute, recompute_seconds,
                 static_cast<unsigned long long>(policy_.expected_reuses), effective_bw);
        d.reason = buf;
    } else {
        d.chose_transfer = false;
        d.score = total_recompute;
        char buf[256];
        snprintf(buf, sizeof(buf),
                 "recompute: cost=%.4f (t=%.4fs x%llu) < transfer cost=%.4f (t=%.4fs); bandwidth=%.0f B/s",
                 total_recompute, recompute_seconds,
                 static_cast<unsigned long long>(policy_.expected_reuses), total_transfer, transfer_seconds,
                 effective_bw);
        d.reason = buf;
    }
    d.policy = "transfer-vs-recompute";
    return d;
}

}  // namespace ctf