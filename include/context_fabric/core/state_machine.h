#pragma once

#include <cstdint>
#include <string>

#include "context_fabric/core/types.h"
#include "context_fabric/core/error.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Object lifecycle state machine.
//
// Every legal transition is declared below. All lifecycle mutation must flow
// through this authority; ad hoc mutation outside the state-management
// authority is rejected. Illegal transitions throw CfError(State).
//
// Legal transitions (source -> destination):
//   Creating    -> Available     create published
//   Creating    -> Released      create aborted
//   Creating    -> Corrupt       integrity failure during create
//   Available   -> LeasedRead    read lease granted
//   Available   -> LeasedWrite   write lease granted
//   Available   -> Replicating   replica creation in progress
//   Available   -> Migrating     migration in progress
//   Available   -> Persisting    persistence in progress
//   Available   -> Invalidating  invalidation in progress
//   Available   -> Expired       lifetime elapsed
//   Available   -> Released      explicit release
//   Available   -> Corrupt       integrity failure detected
//   LeasedRead  -> Available     last read lease released
//   LeasedRead  -> Replicating   replica creation while read-leased
//   LeasedRead  -> Invalidating  invalidation while read-leased
//   LeasedRead  -> Expired       lifetime elapsed while read-leased
//   LeasedRead  -> Released      release while read-leased
//   LeasedRead  -> Corrupt       corruption while read-leased
//   LeasedWrite -> Available     write lease released
//   LeasedWrite -> Replicating   replica creation by write-lease holder
//   LeasedWrite -> Migrating     migration by write-lease holder
//   LeasedWrite -> Persisting    persistence by write-lease holder
//   LeasedWrite -> Recovering    entering recovery after authority change
//   LeasedWrite -> Invalidating  invalidation by write-lease holder
//   LeasedWrite -> Expired       expired while write-leased
//   LeasedWrite -> Released      release while write-leased
//   Replicating  -> Available    replica commit or abort
//   Replicating  -> Corrupt      replica integrity failure
//   Migrating    -> Available    migration commit or abort (source remains)
//   Migrating    -> Corrupt      migration integrity failure
//   Persisting   -> Available    persistence commit or abort
//   Persisting   -> Corrupt      persistence integrity failure
//   Restoring    -> Available    restore verified
//   Restoring    -> Corrupt      restore verification failed
//   Restoring    -> Recovering   reconcile during recovery
//   Invalidating -> Available    invalidation complete
//   Invalidating -> Released     invalidation leads to release
//   Invalidating -> Expired      invalidation of expired object
//   Recovering   -> Available    deterministic reconcile complete
//   Recovering   -> Corrupt      reconcile found uncorrectable corruption
//   Recovering   -> Released     reconcile determined release
//   Recovering   -> Expired      reconcile determined expiry
//   Corrupt      -> Released     quarantine then release
//   Expired      -> Released     cleanup of expired object
//   Expired      -> Available    resurrection (only via explicit authority op)
//   Released     -> (terminal)
// ---------------------------------------------------------------------------
class LifecycleAuthority {
public:
    // Returns true if (from, to) is a legal transition.
    static bool is_legal(LifecycleState from, LifecycleState to) noexcept;

    // Applies `to`; throws CfError(State) on illegal transition.
    static LifecycleState transition(LifecycleState from, LifecycleState to);

    [[nodiscard]] static const char* describe(LifecycleState from, LifecycleState to) noexcept;
};

}  // namespace ctf