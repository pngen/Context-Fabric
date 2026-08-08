#include "context_fabric/storage/flashtier_adapter.h"

namespace ctf {

// This repository exposes the interface boundary only. Configuration fails
// explicitly if FlashTier integration is requested, so returning nullptr here
// cannot silently downgrade an enabled build.
FlashTierResidency* create_flashtier_adapter() {
    return nullptr;
}

}  // namespace ctf
