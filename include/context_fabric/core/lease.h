#pragma once

#include <cstdint>
#include <string>

#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"
#include "context_fabric/core/node.h"
#include "context_fabric/core/types.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Lease: explicit, time-limited authority over an object.
//
// A lease token carries the coordinator epoch and an object lease generation.
// Authority only honors a lease when the token's epoch matches the current
// coordinator epoch and its lease generation matches the object's current
// lease generation. This is what makes a stale node's old lease unusable after
// a coordinator restart or an ownership change.
// ---------------------------------------------------------------------------
struct Lease {
    ObjectId object;
    Generation generation;      // object generation the lease refers to
    NodeId holder;
    LeaseMode mode = LeaseMode::None;
    uint64_t coordinator_epoch = 0;   // must equal current coordinator epoch
    uint64_t lease_generation = 0;    // must equal object's current lease generation
    uint64_t lease_id = 0;            // unique per lease
    uint64_t acquired_unix_ms = 0;
    uint64_t expires_unix_ms = 0;     // 0 = no expiry (never for write leases)

    [[nodiscard]] bool valid() const noexcept {
        return object.valid() && holder.valid() && mode != LeaseMode::None;
    }
    [[nodiscard]] bool expired(uint64_t now_unix_ms) const noexcept {
        return expires_unix_ms != 0 && now_unix_ms >= expires_unix_ms;
    }
    [[nodiscard]] std::string to_string() const;
};

}  // namespace ctf