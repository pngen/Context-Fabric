#pragma once

#include <cstdint>
#include <string>

#include "context_fabric/core/types.h"
#include "context_fabric/core/metadata.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Transfer-versus-recompute planner.
//
// For reusable state, the runtime chooses between transferring an existing
// physical copy and recomputing an equivalent one. The planner never executes
// model recomputation itself; the application/adapter supplies estimates and
// the planner decides which path is cheaper under the configured weights.
// Every decision records its inputs and the reason, so decisions are auditable.
// ---------------------------------------------------------------------------
struct ReusePolicy {
    double time_weight = 1.0;        // relative weight of wall-clock time
    double resource_weight = 1.0;    // relative weight of resource cost
    double bandwidth_ref_bps = 1e9;  // reference bandwidth for cost normalization
    uint64_t expected_reuses = 1;    // expected number of future uses
};

class TransferVsRecompute {
public:
    TransferVsRecompute() = default;
    explicit TransferVsRecompute(ReusePolicy policy) : policy_(policy) {}

    // bandiwdth_bps is the measured path bandwidth current_node->target_node.
    // Returns a PlacementDecision whose `location` is the target node and whose
    // `chose_transfer` indicates whether fetching the state is cheaper than
    // recomputing it. `reason` explains the choice.
    PlacementDecision decide(const ContextMetadata& meta, uint64_t current_node,
                             uint64_t target_node, double bandwidth_bps) const;

private:
    ReusePolicy policy_{};
};

}  // namespace ctf