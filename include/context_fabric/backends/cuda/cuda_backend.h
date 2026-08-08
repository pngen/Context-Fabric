#pragma once

#include <cstdint>
#include <string>

#include "context_fabric/storage/accelerator_backend.h"

namespace ctf {

// ---------------------------------------------------------------------------
// CUDA accelerator backend. Compiled only when CONTEXT_FABRIC_HAS_CUDA is
// defined. Capabilities are discovered at runtime (device count, memory,
// compute capability) — never inferred from the GPU marketing name.
// Allocations and transfers run on a private stream; used bytes and
// high-water marks are tracked under a mutex.
// ---------------------------------------------------------------------------
class CudaAcceleratorBackend : public AcceleratorBackend {
public:
    explicit CudaAcceleratorBackend(uint64_t host_capacity_bytes);
    ~CudaAcceleratorBackend() override;

    CudaAcceleratorBackend(const CudaAcceleratorBackend&) = delete;
    CudaAcceleratorBackend& operator=(const CudaAcceleratorBackend&) = delete;

    const char* name() const noexcept override { return "cuda"; }
    bool available() const noexcept override;

    uint64_t device_count() const noexcept override;
    std::string device_name(uint64_t index) const override;
    uint64_t device_total_memory(uint64_t index) const override;
    uint64_t device_compute_capability_major(uint64_t index) const override;
    uint64_t device_compute_capability_minor(uint64_t index) const override;

    uint64_t capacity_bytes() const noexcept override;
    uint64_t used_bytes() const noexcept override;
    uint64_t high_water_bytes() const noexcept override;

    void set_device(uint64_t index) override;
    void allocate(uint8_t*& ptr, uint64_t size) override;
    void free(uint8_t* ptr) override;
    void copy_h2d(uint8_t* dst_device, const uint8_t* src_host, uint64_t size) override;
    void copy_d2h(uint8_t* dst_host, const uint8_t* src_device, uint64_t size) override;
    void verify(const uint8_t* device, uint64_t size, uint64_t expected_crc,
                uint64_t expected_sha) override;
    void synchronize() override;
    void shutdown() override;

private:
    void* stream_ = nullptr;  // cudaStream_t
    int device_ = 0;
    uint64_t capacity_ = 0;
    uint64_t used_ = 0;
    uint64_t high_water_ = 0;
    bool available_ = false;
    bool shutdown_ = false;
};

}  // namespace ctf