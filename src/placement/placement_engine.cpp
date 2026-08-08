#include "context_fabric/placement/placement_engine.h"

#include <algorithm>
#include <cstdio>

#include "context_fabric/core/error.h"

namespace ctf {

namespace {

// A candidate is capable of hosting the object if its domain is supported by
// its capabilities and it has headroom for the object's bytes.
bool candidate_capable(const ContextMetadata& meta, const PlacementCandidate& c) {
    switch (c.location.domain) {
        case StorageDomain::HostMemory:
            if (!c.caps.host_memory) return false;
            break;
        case StorageDomain::LocalPersistent:
            if (!c.caps.local_persistent) return false;
            break;
        case StorageDomain::AcceleratorLocal:
            if (!c.caps.accelerator) return false;
            break;
        default:
            return false;
    }
    if (c.total_bytes != 0 && c.used_bytes + meta.byte_size > c.total_bytes) return false;
    return true;
}

double transfer_seconds(const ContextMetadata& meta, const PlacementCandidate& c) {
    if (c.bandwidth_bps <= 0.0) return 1e9;
    return static_cast<double>(meta.byte_size) / c.bandwidth_bps + c.latency_ms / 1000.0;
}

}  // namespace

PlacementDecision PlacementEngine::decide(const ContextMetadata& meta, const Location& current,
                                          const std::vector<PlacementCandidate>& candidates,
                                          const std::string& policy) const {
    std::vector<PlacementCandidate> capable;
    for (const auto& c : candidates) {
        if (candidate_capable(meta, c)) capable.push_back(c);
    }
    if (capable.empty()) {
        throw_error(ErrKind::NotFound, "no candidate can host the object");
    }
    if (policy == kPolicyBaseline) return decide_baseline(meta, current, capable);
    if (policy == kPolicyCostAware) return decide_cost_aware(meta, current, capable);
    throw_error(ErrKind::NotSupported, "unknown placement policy: " + policy);
}

PlacementDecision PlacementEngine::decide_baseline(const ContextMetadata& meta,
                                                   const Location& current,
                                                   const std::vector<PlacementCandidate>& candidates) const {
    (void)meta;
    // Baseline: prefer the current authoritative location when it can host the
    // object; otherwise the candidate with the most free headroom. Stable
    // tie-break by (node_id, domain).
    const PlacementCandidate* best = nullptr;
    for (const auto& c : candidates) {
        if (current.valid() && c.location.node_id == current.node_id &&
            c.location.domain == current.domain) {
            best = const_cast<PlacementCandidate*>(&c);
            break;
        }
    }
    if (best == nullptr) {
        for (const auto& c : candidates) {
            if (best == nullptr) {
                best = const_cast<PlacementCandidate*>(&c);
                continue;
            }
            uint64_t free_best = best->total_bytes > best->used_bytes ? best->total_bytes - best->used_bytes : 0;
            uint64_t free_c = c.total_bytes > c.used_bytes ? c.total_bytes - c.used_bytes : 0;
            if (free_c > free_best) {
                best = const_cast<PlacementCandidate*>(&c);
            } else if (free_c == free_best) {
                if (c.location.node_id < best->location.node_id ||
                    (c.location.node_id == best->location.node_id &&
                     static_cast<uint32_t>(c.location.domain) < static_cast<uint32_t>(best->location.domain))) {
                    best = const_cast<PlacementCandidate*>(&c);
                }
            }
        }
    }
    PlacementDecision d;
    d.location = best->location;
    d.policy = kPolicyBaseline;
    d.score = 0.0;
    d.reason = "baseline: largest-free-headroom (stay preferred)";
    return d;
}

PlacementDecision PlacementEngine::decide_cost_aware(const ContextMetadata& meta,
                                                     const Location& current,
                                                     const std::vector<PlacementCandidate>& candidates) const {
    // Cost-aware score per candidate (lower is better):
    //   score = transfer_seconds / ref + capacity_pressure + reuse_term
    //   - transfer_seconds: time to move the object here (0 if staying)
    //   - capacity_pressure: penalty when headroom is thin
    //   - reuse_term: if reuse is likely, prefer domains closer to the consumer;
    //     modeled here as a small bonus for accelerator residency when the
    //     object is hot.
    const double ref_sec = 1.0;
    const PlacementCandidate* best = nullptr;
    double best_score = 0.0;
    char reason_buf[256];

    for (const auto& c : candidates) {
        bool staying = current.valid() && c.location.node_id == current.node_id &&
                       c.location.domain == current.domain;
        double transfer = staying ? 0.0 : transfer_seconds(meta, c);

        double pressure = 0.0;
        if (c.total_bytes > 0) {
            double frac = static_cast<double>(c.used_bytes + meta.byte_size) / static_cast<double>(c.total_bytes);
            if (frac > 0.9) pressure = 10.0;
            else if (frac > 0.7) pressure = 2.0;
        }

        double reuse_term = 0.0;
        if (meta.reuse_probability > 0.5 && c.location.domain == StorageDomain::AcceleratorLocal) {
            reuse_term = -0.25;  // hot state prefers accelerator residency
        }

        double score = (transfer / ref_sec) + pressure + reuse_term;

        if (staying) score -= 1.0;  // strong preference to avoid gratuitous moves

        if (best == nullptr || score < best_score - 1e-12) {
            best = const_cast<PlacementCandidate*>(&c);
            best_score = score;
        } else if (score > best_score - 1e-12 && score < best_score + 1e-12) {
            // Stable tie-break.
            if (c.location.node_id < best->location.node_id ||
                (c.location.node_id == best->location.node_id &&
                 static_cast<uint32_t>(c.location.domain) < static_cast<uint32_t>(best->location.domain))) {
                best = const_cast<PlacementCandidate*>(&c);
                best_score = score;
            }
        }
    }

    snprintf(reason_buf, sizeof(reason_buf),
             "cost-aware: score=%.4f transfer=%.4fs pressure term and reuse term applied",
             best_score, transfer_seconds(meta, *best));

    PlacementDecision d;
    d.location = best->location;
    d.policy = kPolicyCostAware;
    d.score = best_score;
    d.reason = reason_buf;
    d.transfer.expected_seconds = transfer_seconds(meta, *best);
    d.transfer.expected_bytes = static_cast<double>(meta.byte_size);
    return d;
}

}  // namespace ctf