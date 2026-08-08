#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/core/types.h"

namespace ctf {

// ---------------------------------------------------------------------------
// AcceleratorBackend: real accelerator memory domain.
//
// The implementation is CUDA-based when CONTEXT_FABRIC_HAS_CUDA is defined at
// compile time; otherwise this interface reports NotSupported. Capabilities
// are discovered at runtime (number of devices, memory, compute capability) —
// never inferred from marketing names.
// ---------------------------------------------------------------------------
class AcceleratorBackend {
public:
    virtual ~AcceleratorBackend() = default;

    [[nodiscard]] virtual const char* name() const noexcept = 0;
    [[nodiscard]] virtual bool available() const noexcept = 0;

    // Device enumeration and capability discovery.
    [[nodiscard]] virtual uint64_t device_count() const noexcept = 0;
    [[nodiscard]] virtual std::string device_name(uint64_t index) const = 0;
    [[nodiscard]] virtual uint64_t device_total_memory(uint64_t index) const = 0;
    [[nodiscard]] virtual uint64_t device_compute_capability_major(uint64_t index) const = 0;
    [[nodiscard]] virtual uint64_t device_compute_capability_minor(uint64_t index) const = 0;

    [[nodiscard]] virtual uint64_t capacity_bytes() const noexcept = 0;
    [[nodiscard]] virtual uint64_t used_bytes() const noexcept = 0;
    [[nodiscard]] virtual uint64_t high_water_bytes() const noexcept = 0;

    // Select the active device for subsequent operations.
    virtual void set_device(uint64_t index) = 0;

    // Explicit allocation/free of device memory. Uses a private stream.
    // Throws CfError(Cuda) with typed codes on failure.
    virtual void allocate(uint8_t*& ptr, uint64_t size) = 0;
    virtual void free(uint8_t* ptr) = 0;

    // Host<->device copies on the private stream.
    virtual void copy_h2d(uint8_t* dst_device, const uint8_t* src_host, uint64_t size) = 0;
    virtual void copy_d2h(uint8_t* dst_host, const uint8_t* src_device, uint64_t size) = 0;

    // Round-trip integrity verification: copy device->host and compare against
    // expected checksums. Throws CfError(Integrity) on mismatch.
    virtual void verify(const uint8_t* device, uint64_t size, uint64_t expected_crc,
                        uint64_t expected_sha) = 0;

    // Synchronize the private stream.
    virtual void synchronize() = 0;

    virtual void shutdown() = 0;
};

// Factory. Returns nullptr when accelerator support is not compiled in. A
// zero capacity selects the backend's conservative device-derived default.
[[nodiscard]] AcceleratorBackend* create_accelerator_backend(uint64_t accelerator_capacity_bytes);

}  // namespace ctf
