#include "context_fabric/placement/cost_model.h"

#include <cmath>
#include <cstdio>

#include "context_fabric/core/error.h"

namespace ctf {

PlacementDecision TransferVsRecompute::decide(const ContextMetadata& meta, uint64_t current_node,
                                              uint64_t target_node, double bandwidth_bps) const {
    if (current_node == 0 || target_node == 0) {
        throw_error(ErrKind::InvalidArgument, "reuse planning requires valid current and target nodes");
    }
    const auto finite_nonnegative = [](double value) {
        return std::isfinite(value) && value >= 0.0;
    };
    if (!finite_nonnegative(bandwidth_bps) ||
        !finite_nonnegative(meta.recompute_seconds) ||
        !finite_nonnegative(meta.recompute_resource_cost) ||
        !finite_nonnegative(policy_.time_weight) ||
        !finite_nonnegative(policy_.resource_weight) ||
        !std::isfinite(policy_.bandwidth_ref_bps) || policy_.bandwidth_ref_bps <= 0.0 ||
        policy_.expected_reuses == 0) {
        throw_error(ErrKind::InvalidArgument,
                    "reuse planning inputs must be finite and non-negative, with positive bandwidth reference and reuse count");
    }
    PlacementDecision d;
    d.location.node_id = target_node;
    d.location.domain = StorageDomain::HostMemory;

    // Transfer cost: one fetch retained at the target. Recompute cost:
    // recomputation for each expected use. Both are expressed in normalized
    // cost units.
    const bool used_reference_bandwidth = bandwidth_bps == 0.0;
    double effective_bw = used_reference_bandwidth ? policy_.bandwidth_ref_bps : bandwidth_bps;
    const bool already_local = current_node == target_node;
    double transfer_seconds = already_local ? 0.0 : static_cast<double>(meta.byte_size) / effective_bw;
    double transfer_resource_cost =
        already_local ? 0.0 : transfer_seconds * policy_.bandwidth_ref_bps / effective_bw;
    double total_transfer = policy_.time_weight * transfer_seconds +
                            policy_.resource_weight * transfer_resource_cost;

    const bool used_unavailable_recompute_default = meta.recompute_seconds == 0.0;
    double recompute_seconds = used_unavailable_recompute_default ? 1e9 : meta.recompute_seconds;
    double recompute_cost = meta.recompute_resource_cost > 0.0 ? meta.recompute_resource_cost : recompute_seconds;
    double total_recompute = policy_.time_weight * recompute_seconds * static_cast<double>(policy_.expected_reuses) +
                             policy_.resource_weight * recompute_cost * static_cast<double>(policy_.expected_reuses);
    if (!std::isfinite(total_transfer) || !std::isfinite(total_recompute)) {
        throw_error(ErrKind::InvalidArgument, "reuse planning inputs produce a non-finite cost");
    }

    d.transfer.expected_seconds = transfer_seconds;
    d.transfer.expected_bytes = already_local ? 0.0 : static_cast<double>(meta.byte_size);
    d.transfer.resource_cost = transfer_resource_cost;
    d.recompute.expected_seconds = recompute_seconds;
    d.recompute.resource_cost = recompute_cost;

    if (total_transfer <= total_recompute) {
        d.chose_transfer = true;
        d.score = total_transfer;
        char buf[256];
        snprintf(buf, sizeof(buf),
                 "transfer: cost=%.4f (t=%.4fs,reuse=%llu) <= recompute cost=%.4f (t=%.4fs x%llu%s); bandwidth=%.0f B/s%s",
                 total_transfer, transfer_seconds,
                 static_cast<unsigned long long>(policy_.expected_reuses), total_recompute, recompute_seconds,
                 static_cast<unsigned long long>(policy_.expected_reuses),
                 used_unavailable_recompute_default ? ", unavailable-estimate default" : "", effective_bw,
                 used_reference_bandwidth ? " (reference)" : "");
        d.reason = buf;
    } else {
        d.chose_transfer = false;
        d.score = total_recompute;
        char buf[256];
        snprintf(buf, sizeof(buf),
                 "recompute: cost=%.4f (t=%.4fs x%llu%s) < transfer cost=%.4f (t=%.4fs); bandwidth=%.0f B/s%s",
                 total_recompute, recompute_seconds,
                 static_cast<unsigned long long>(policy_.expected_reuses),
                 used_unavailable_recompute_default ? ", unavailable-estimate default" : "", total_transfer,
                 transfer_seconds,
                 effective_bw, used_reference_bandwidth ? " (reference)" : "");
        d.reason = buf;
    }
    d.policy = "transfer-vs-recompute";
    return d;
}

}  // namespace ctf
