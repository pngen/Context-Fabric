#include "context_fabric/core/state_machine.h"

namespace ctf {

// Transition table: rows are source states, columns encode the set of legal
// destinations. Implemented as a compact bitmask per source state.
namespace {

inline constexpr uint64_t bit(LifecycleState s) noexcept {
    return uint64_t(1) << static_cast<uint32_t>(s);
}

// Legal destination set for each source state.
inline constexpr uint64_t kTransitions[] = {
    /* Creating     */ bit(LifecycleState::Available) | bit(LifecycleState::Released) | bit(LifecycleState::Corrupt),
    /* Available    */ bit(LifecycleState::LeasedRead) | bit(LifecycleState::LeasedWrite) |
                       bit(LifecycleState::Replicating) | bit(LifecycleState::Migrating) |
                       bit(LifecycleState::Persisting) | bit(LifecycleState::Restoring) |
                       bit(LifecycleState::Invalidating) | bit(LifecycleState::Recovering) |
                       bit(LifecycleState::Expired) | bit(LifecycleState::Released) | bit(LifecycleState::Corrupt),
    /* LeasedRead   */ bit(LifecycleState::Available) | bit(LifecycleState::Replicating) |
                       bit(LifecycleState::Invalidating) | bit(LifecycleState::Expired) |
                       bit(LifecycleState::Released) | bit(LifecycleState::Corrupt) |
                       bit(LifecycleState::Recovering),
    /* LeasedWrite  */ bit(LifecycleState::Available) | bit(LifecycleState::Replicating) |
                       bit(LifecycleState::Migrating) | bit(LifecycleState::Persisting) |
                       bit(LifecycleState::Recovering) | bit(LifecycleState::Invalidating) |
                       bit(LifecycleState::Expired) | bit(LifecycleState::Released) | bit(LifecycleState::Corrupt),
    /* Replicating  */ bit(LifecycleState::Available) | bit(LifecycleState::Corrupt),
    /* Migrating    */ bit(LifecycleState::Available) | bit(LifecycleState::Corrupt),
    /* Persisting   */ bit(LifecycleState::Available) | bit(LifecycleState::Corrupt),
    /* Restoring    */ bit(LifecycleState::Available) | bit(LifecycleState::Corrupt) | bit(LifecycleState::Recovering),
    /* Invalidating */ bit(LifecycleState::Available) | bit(LifecycleState::Released) | bit(LifecycleState::Expired),
    /* Recovering   */ bit(LifecycleState::Available) | bit(LifecycleState::Corrupt) |
                       bit(LifecycleState::Released) | bit(LifecycleState::Expired) |
                       bit(LifecycleState::Restoring),
    /* Corrupt      */ bit(LifecycleState::Released),
    /* Expired      */ bit(LifecycleState::Released) | bit(LifecycleState::Available),
    /* Released     */ 0,
};

static_assert(sizeof(kTransitions) / sizeof(kTransitions[0]) ==
              static_cast<size_t>(LifecycleState::Released) + 1,
              "transition table must cover every state");

}  // namespace

bool LifecycleAuthority::is_legal(LifecycleState from, LifecycleState to) noexcept {
    uint32_t f = static_cast<uint32_t>(from);
    uint32_t t = static_cast<uint32_t>(to);
    if (f >= sizeof(kTransitions) / sizeof(kTransitions[0])) return false;
    if (t >= sizeof(kTransitions) / sizeof(kTransitions[0])) return false;
    return (kTransitions[f] & bit(to)) != 0;
}

LifecycleState LifecycleAuthority::transition(LifecycleState from, LifecycleState to) {
    if (!is_legal(from, to)) {
        throw_error(ErrKind::State, std::string("illegal lifecycle transition: ") +
                                        lifecycle_state_name(from) + " -> " + lifecycle_state_name(to));
    }
    return to;
}

const char* LifecycleAuthority::describe(LifecycleState from, LifecycleState to) noexcept {
    static thread_local char buf[64];
    snprintf(buf, sizeof(buf), "%s -> %s", lifecycle_state_name(from), lifecycle_state_name(to));
    return buf;
}

}  // namespace ctf