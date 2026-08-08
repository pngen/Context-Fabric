#include "context_fabric/storage/flashtier_adapter.h"

namespace ctf {

// By default no FlashTier adapter is linked; Context Fabric is fully
// functional without it. A sibling repository may supply a real implementation
// of FlashTierResidency and this factory.
FlashTierResidency* create_flashtier_adapter() {
    return nullptr;
}

}  // namespace ctf