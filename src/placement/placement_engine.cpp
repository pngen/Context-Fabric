#include "context_fabric/placement/placement_engine.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <limits>

#include "context_fabric/core/error.h"

namespace ctf {

namespace {

bool same_location(const Location& lhs, const Location& rhs) {
    return lhs.valid() && rhs.valid() && lhs.node_id == rhs.node_id && lhs.domain == rhs.domain;
}

void validate_candidate_metrics(const PlacementCandidate& c) {
    const auto valid_metric = [](double value) { return std::isfinite(value) && value >= 0.0; };
    if (!c.location.valid()) {
        throw_error(ErrKind::InvalidArgument, "placement candidate location must be valid");
    }
    if (!valid_metric(c.bandwidth_bps) || !valid_metric(c.latency_ms) ||
        !valid_metric(c.storage_bps) || !valid_metric(c.accelerator_bps)) {
        throw_error(ErrKind::InvalidArgument, "placement metrics must be finite and non-negative");
    }
}

// A candidate is capable of hosting the object if its domain is supported by
// its capabilities and it has headroom for the object's bytes. An object that
// is already at the candidate location needs no additional capacity.
bool candidate_capable(const ContextMetadata& meta, const Location& current,
                       const PlacementCandidate& c) {
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
    if (c.total_bytes != 0) {
        if (c.used_bytes > c.total_bytes) return false;
        const uint64_t additional = same_location(current, c.location) ? 0 : meta.byte_size;
        if (additional > c.total_bytes - c.used_bytes) return false;
    }
    return true;
}

double transfer_seconds(const ContextMetadata& meta, const PlacementCandidate& c) {
    if (c.bandwidth_bps <= 0.0) return 1e9;
    const double seconds = static_cast<double>(meta.byte_size) / c.bandwidth_bps +
                           c.latency_ms / 1000.0;
    if (!std::isfinite(seconds)) {
        throw_error(ErrKind::InvalidArgument, "placement metrics produce a non-finite transfer time");
    }
    return seconds;
}

}  // namespace

PlacementDecision PlacementEngine::decide(const ContextMetadata& meta, const Location& current,
                                          const std::vector<PlacementCandidate>& candidates,
                                          const std::string& policy) const {
    if (!std::isfinite(meta.reuse_probability) || meta.reuse_probability < 0.0 ||
        meta.reuse_probability > 1.0) {
        throw_error(ErrKind::InvalidArgument, "reuse probability must be finite and between zero and one");
    }
    std::vector<PlacementCandidate> capable;
    for (const auto& c : candidates) {
        validate_candidate_metrics(c);
        if (candidate_capable(meta, current, c)) capable.push_back(c);
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
            best = &c;
            break;
        }
    }
    if (best == nullptr) {
        for (const auto& c : candidates) {
            if (best == nullptr) {
                best = &c;
                continue;
            }
            const uint64_t free_best = best->total_bytes == 0
                                           ? std::numeric_limits<uint64_t>::max()
                                           : best->total_bytes - best->used_bytes;
            const uint64_t free_c = c.total_bytes == 0
                                        ? std::numeric_limits<uint64_t>::max()
                                        : c.total_bytes - c.used_bytes;
            if (free_c > free_best) {
                best = &c;
            } else if (free_c == free_best) {
                if (c.location.node_id < best->location.node_id ||
                    (c.location.node_id == best->location.node_id &&
                     static_cast<uint32_t>(c.location.domain) < static_cast<uint32_t>(best->location.domain))) {
                    best = &c;
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
    double best_transfer = 0.0;
    char reason_buf[256];

    for (const auto& c : candidates) {
        const bool staying = same_location(current, c.location);
        double transfer = staying ? 0.0 : transfer_seconds(meta, c);

        double pressure = 0.0;
        if (c.total_bytes > 0) {
            const uint64_t additional = staying ? 0 : meta.byte_size;
            const double frac = static_cast<double>(c.used_bytes) / static_cast<double>(c.total_bytes) +
                                static_cast<double>(additional) / static_cast<double>(c.total_bytes);
            if (frac > 0.9) pressure = 10.0;
            else if (frac > 0.7) pressure = 2.0;
        }

        double reuse_term = 0.0;
        if (meta.reuse_probability > 0.5 && c.location.domain == StorageDomain::AcceleratorLocal) {
            reuse_term = -0.25;  // hot state prefers accelerator residency
        }

        double score = (transfer / ref_sec) + pressure + reuse_term;

        if (staying) score -= 1.0;  // strong preference to avoid gratuitous moves
        if (!std::isfinite(score)) {
            throw_error(ErrKind::InvalidArgument, "placement metrics produce a non-finite score");
        }

        if (best == nullptr || score < best_score - 1e-12) {
            best = &c;
            best_score = score;
            best_transfer = transfer;
        } else if (score > best_score - 1e-12 && score < best_score + 1e-12) {
            // Stable tie-break.
            if (c.location.node_id < best->location.node_id ||
                (c.location.node_id == best->location.node_id &&
                 static_cast<uint32_t>(c.location.domain) < static_cast<uint32_t>(best->location.domain))) {
                best = &c;
                best_score = score;
                best_transfer = transfer;
            }
        }
    }

    snprintf(reason_buf, sizeof(reason_buf),
             "cost-aware: score=%.4f transfer=%.4fs pressure term and reuse term applied",
             best_score, best_transfer);

    PlacementDecision d;
    d.location = best->location;
    d.policy = kPolicyCostAware;
    d.score = best_score;
    d.reason = reason_buf;
    d.transfer.expected_seconds = best_transfer;
    d.transfer.expected_bytes = best_transfer == 0.0 ? 0.0 : static_cast<double>(meta.byte_size);
    return d;
}

}  // namespace ctf
