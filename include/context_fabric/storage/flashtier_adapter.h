#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"
#include "context_fabric/core/types.h"

namespace ctf {

// ---------------------------------------------------------------------------
// FlashTier adapter boundary.
//
// FlashTier is a separate sibling project that answers "where should this
// physical working page reside on one machine?" (VRAM / pinned host RAM /
// NVMe). Context Fabric answers distributed questions (identity, ownership,
// discovery, replication, migration, persistence policy, placement between
// nodes). This narrow interface lets a Context Fabric node delegate
// node-local physical residency management to FlashTier.
//
// IMPORTANT:
//  - Context Fabric builds and runs completely WITHOUT FlashTier.
//  - No FlashTier source is copied here.
//  - This header documents and implements the CONTRACT. A concrete adapter
//    that links against a FlashTier library may be supplied externally via
//    CONTEXT_FABRIC_FLASHTIER_DIR.
//  - When no adapter is present, the node's own HostMemoryBackend /
//    LocalPersistentBackend / CudaAcceleratorBackend are used, exactly as in
//    the CPU-only and CUDA validation runs.
// ---------------------------------------------------------------------------

// Capabilities a FlashTier-backed residency domain can provide.
struct FlashTierCaps {
    bool vram = false;
    bool pinned_host = false;
    bool nvme = false;
    uint64_t max_vram_bytes = 0;
    uint64_t max_pinned_bytes = 0;
    uint64_t max_nvme_bytes = 0;
};

// The adapter a node would use to place a physical copy of an object in a
// specific FlashTier media tier. Implementations are provided by the sibling
// FlashTier project; Context Fabric defines the interface only.
class FlashTierResidency {
public:
    virtual ~FlashTierResidency() = default;

    virtual const char* name() const noexcept = 0;
    [[nodiscard]] virtual bool available() const noexcept = 0;
    [[nodiscard]] virtual FlashTierCaps capabilities() const noexcept = 0;

    // Place `size` bytes of `data` into the given media tier, returning a
    // residency handle. The handle is opaque to Context Fabric.
    virtual uint64_t place(const ObjectId& id, const Generation& gen, StorageDomain target,
                           const uint8_t* data, uint64_t size) = 0;

    // Read the placed bytes back into memory.
    virtual std::vector<uint8_t> read(const ObjectId& id, const Generation& gen, uint64_t handle) = 0;

    // Release a residency handle.
    virtual void release(uint64_t handle) = 0;

    [[nodiscard]] virtual uint64_t used_bytes() const noexcept = 0;
    [[nodiscard]] virtual uint64_t high_water_bytes() const noexcept = 0;
};

// Optional factory. Returns nullptr when no FlashTier adapter is linked.
// The CLI/runtime wires this in only when CONTEXT_FABRIC_FLASHTIER_DIR is
// configured at build time.
FlashTierResidency* create_flashtier_adapter();

}  // namespace ctf