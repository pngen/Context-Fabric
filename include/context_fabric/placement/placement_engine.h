#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/core/types.h"
#include "context_fabric/core/metadata.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Placement engine.
//
// Context Fabric placement decides where accumulated reusable state should
// live. It is deliberately NOT execution scheduling (that is a future sibling
// Compute Fabric). The engine is deterministic: identical (state, metrics,
// hints) input always yields the identical decision, with stable tie-breaking
// by (score, node_id, domain).
// ---------------------------------------------------------------------------

// A candidate residency point for placement.
struct PlacementCandidate {
    Location location;         // node + domain
    NodeCapabilities caps;     // capabilities of the node
    uint64_t used_bytes = 0;   // current usage of this domain
    uint64_t total_bytes = 0;  // capacity of this domain
    double bandwidth_bps = 0.0;   // measured to/from the current location
    double latency_ms = 0.0;      // measured to/from the current location
    double storage_bps = 0.0;     // storage write bandwidth
    double accelerator_bps = 0.0; // accelerator transfer bandwidth (host<->device)
};

class PlacementEngine {
public:
    // `current` may be invalid (new object). Candidates not supporting the
    // object's needs are excluded. Throws CfError(NotFound) if no candidate
    // can host the object.
    PlacementDecision decide(const ContextMetadata& meta, const Location& current,
                             const std::vector<PlacementCandidate>& candidates,
                             const std::string& policy) const;

private:
    PlacementDecision decide_baseline(const ContextMetadata& meta, const Location& current,
                                      const std::vector<PlacementCandidate>& candidates) const;
    PlacementDecision decide_cost_aware(const ContextMetadata& meta, const Location& current,
                                        const std::vector<PlacementCandidate>& candidates) const;
};

// Available policy names.
inline constexpr const char* kPolicyBaseline = "baseline";
inline constexpr const char* kPolicyCostAware = "cost-aware";

}  // namespace ctf