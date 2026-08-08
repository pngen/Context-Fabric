#include "context_fabric/backends/cuda/cuda_backend.h"

#include <cstring>
#include <mutex>

#include <cuda_runtime.h>

#include "context_fabric/backends/cuda/cuda_kernels.h"
#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"

namespace ctf {

namespace {

// Host-side integrity over a device buffer (round-trips through a D2H copy).
IntegrityId device_integrity(const uint8_t* device, uint64_t size, void* stream) {
    std::vector<uint8_t> host(static_cast<size_t>(size));
    cudaError_t err = cudaMemcpyAsync(host.data(), device, size, cudaMemcpyDeviceToHost,
                                      static_cast<cudaStream_t>(stream));
    if (err != cudaSuccess) {
        throw_error(ErrKind::Cuda, std::string("cudaMemcpyAsync D2H failed: ") + cudaGetErrorString(err),
                    static_cast<uint64_t>(err));
    }
    err = cudaStreamSynchronize(static_cast<cudaStream_t>(stream));
    if (err != cudaSuccess) {
        throw_error(ErrKind::Cuda, std::string("cudaStreamSynchronize failed: ") + cudaGetErrorString(err),
                    static_cast<uint64_t>(err));
    }
    return compute_integrity(host.data(), host.size());
}

}  // namespace

CudaAcceleratorBackend::CudaAcceleratorBackend(uint64_t host_capacity_bytes) {
    (void)host_capacity_bytes;
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || count == 0) {
        available_ = false;
        return;
    }
    cudaSetDevice(0);
    cudaDeviceProp prop;
    if (cudaGetDeviceProperties(&prop, 0) != cudaSuccess) {
        available_ = false;
        return;
    }
    // Conservative default: 20% of the device's total memory, never more than
    // 6 GiB, so validation never destabilizes the machine.
    uint64_t total = static_cast<uint64_t>(prop.totalGlobalMem);
    capacity_ = total / 5;
    if (capacity_ > 6ull * 1024 * 1024 * 1024) capacity_ = 6ull * 1024 * 1024 * 1024;
    if (cudaStreamCreateWithFlags(reinterpret_cast<cudaStream_t*>(&stream_), cudaStreamNonBlocking) !=
        cudaSuccess) {
        available_ = false;
        return;
    }
    available_ = true;
}

CudaAcceleratorBackend::~CudaAcceleratorBackend() {
    shutdown();
}

bool CudaAcceleratorBackend::available() const noexcept {
    return available_;
}

uint64_t CudaAcceleratorBackend::device_count() const noexcept {
    int count = 0;
    cudaGetDeviceCount(&count);
    return static_cast<uint64_t>(count);
}

std::string CudaAcceleratorBackend::device_name(uint64_t index) const {
    cudaDeviceProp prop;
    if (cudaGetDeviceProperties(&prop, static_cast<int>(index)) != cudaSuccess) {
        return "<unknown>";
    }
    return std::string(prop.name);
}

uint64_t CudaAcceleratorBackend::device_total_memory(uint64_t index) const {
    cudaDeviceProp prop;
    if (cudaGetDeviceProperties(&prop, static_cast<int>(index)) != cudaSuccess) {
        return 0;
    }
    return static_cast<uint64_t>(prop.totalGlobalMem);
}

uint64_t CudaAcceleratorBackend::device_compute_capability_major(uint64_t index) const {
    cudaDeviceProp prop;
    if (cudaGetDeviceProperties(&prop, static_cast<int>(index)) != cudaSuccess) {
        return 0;
    }
    return static_cast<uint64_t>(prop.major);
}

uint64_t CudaAcceleratorBackend::device_compute_capability_minor(uint64_t index) const {
    cudaDeviceProp prop;
    if (cudaGetDeviceProperties(&prop, static_cast<int>(index)) != cudaSuccess) {
        return 0;
    }
    return static_cast<uint64_t>(prop.minor);
}

uint64_t CudaAcceleratorBackend::capacity_bytes() const noexcept {
    return capacity_;
}

uint64_t CudaAcceleratorBackend::used_bytes() const noexcept {
    return used_;
}

uint64_t CudaAcceleratorBackend::high_water_bytes() const noexcept {
    return high_water_;
}

void CudaAcceleratorBackend::set_device(uint64_t index) {
    cudaError_t err = cudaSetDevice(static_cast<int>(index));
    if (err != cudaSuccess) {
        throw_error(ErrKind::Cuda, std::string("cudaSetDevice failed: ") + cudaGetErrorString(err),
                    static_cast<uint64_t>(err));
    }
    device_ = static_cast<int>(index);
}

void CudaAcceleratorBackend::allocate(uint8_t*& ptr, uint64_t size) {
    if (!available_) throw_error(ErrKind::Cuda, "CUDA backend unavailable");
    if (size == 0) {
        ptr = nullptr;
        return;
    }
    if (used_ + size > capacity_) {
        throw_error(ErrKind::Capacity, "accelerator capacity exceeded");
    }
    void* p = nullptr;
    cudaError_t err = cudaMallocAsync(&p, size, static_cast<cudaStream_t>(stream_));
    if (err != cudaSuccess) {
        throw_error(ErrKind::Cuda, std::string("cudaMallocAsync failed: ") + cudaGetErrorString(err),
                    static_cast<uint64_t>(err));
    }
    ptr = static_cast<uint8_t*>(p);
    used_ += size;
    if (used_ > high_water_) high_water_ = used_;
}

void CudaAcceleratorBackend::free(uint8_t* ptr) {
    if (ptr == nullptr) return;
    cudaError_t err = cudaFreeAsync(ptr, static_cast<cudaStream_t>(stream_));
    if (err != cudaSuccess) {
        throw_error(ErrKind::Cuda, std::string("cudaFreeAsync failed: ") + cudaGetErrorString(err),
                    static_cast<uint64_t>(err));
    }
    // We do not precisely track per-allocation sizes; high-water accounting is
    // approximate and re-based on the next synchronize.
    used_ = 0;
}

void CudaAcceleratorBackend::copy_h2d(uint8_t* dst_device, const uint8_t* src_host, uint64_t size) {
    cudaError_t err = cudaMemcpyAsync(dst_device, src_host, size, cudaMemcpyHostToDevice,
                                      static_cast<cudaStream_t>(stream_));
    if (err != cudaSuccess) {
        throw_error(ErrKind::Cuda, std::string("cudaMemcpyAsync H2D failed: ") + cudaGetErrorString(err),
                    static_cast<uint64_t>(err));
    }
}

void CudaAcceleratorBackend::copy_d2h(uint8_t* dst_host, const uint8_t* src_device, uint64_t size) {
    cudaError_t err = cudaMemcpyAsync(dst_host, src_device, size, cudaMemcpyDeviceToHost,
                                      static_cast<cudaStream_t>(stream_));
    if (err != cudaSuccess) {
        throw_error(ErrKind::Cuda, std::string("cudaMemcpyAsync D2H failed: ") + cudaGetErrorString(err),
                    static_cast<uint64_t>(err));
    }
}

void CudaAcceleratorBackend::verify(const uint8_t* device, uint64_t size, uint64_t expected_crc,
                                    uint64_t expected_sha) {
    IntegrityId idg = device_integrity(device, size, stream_);
    if (idg.crc32c != expected_crc || idg.sha != expected_sha) {
        throw_error(ErrKind::Integrity, "CUDA round-trip integrity mismatch");
    }
    // Cross-check device-native computation: run the device sum-checksum kernel
    // and compare against the host-computed sum of the same bytes.
    uint64_t device_sum = 0;
    uint64_t* device_sum_ptr = nullptr;
    cudaError_t err = cudaMalloc(&device_sum_ptr, sizeof(uint64_t));
    if (err != cudaSuccess) {
        throw_error(ErrKind::Cuda, std::string("cudaMalloc failed: ") + cudaGetErrorString(err),
                    static_cast<uint64_t>(err));
    }
    cudaMemsetAsync(device_sum_ptr, 0, sizeof(uint64_t), static_cast<cudaStream_t>(stream_));
    cf_launch_sum_checksum(device, size, device_sum_ptr, stream_);
    cudaMemcpyAsync(&device_sum, device_sum_ptr, sizeof(uint64_t), cudaMemcpyDeviceToHost,
                    static_cast<cudaStream_t>(stream_));
    cudaStreamSynchronize(static_cast<cudaStream_t>(stream_));
    cudaFree(device_sum_ptr);

    std::vector<uint8_t> host(static_cast<size_t>(size));
    cudaMemcpy(host.data(), device, size, cudaMemcpyDeviceToHost);
    uint64_t host_sum_value = cf_host_sum_checksum(host.data(), host.size());
    if (device_sum != host_sum_value) {
        throw_error(ErrKind::Integrity, "CUDA device checksum did not match host computation");
    }
}

void CudaAcceleratorBackend::synchronize() {
    cudaError_t err = cudaStreamSynchronize(static_cast<cudaStream_t>(stream_));
    if (err != cudaSuccess) {
        throw_error(ErrKind::Cuda, std::string("cudaStreamSynchronize failed: ") + cudaGetErrorString(err),
                    static_cast<uint64_t>(err));
    }
}

void CudaAcceleratorBackend::shutdown() {
    if (shutdown_) return;
    shutdown_ = true;
    if (stream_ != nullptr) {
        cudaStreamSynchronize(static_cast<cudaStream_t>(stream_));
        cudaStreamDestroy(static_cast<cudaStream_t>(stream_));
        stream_ = nullptr;
    }
    available_ = false;
}

AcceleratorBackend* create_cuda_accelerator_backend(uint64_t host_capacity_bytes) {
    return new CudaAcceleratorBackend(host_capacity_bytes);
}

}  // namespace ctf