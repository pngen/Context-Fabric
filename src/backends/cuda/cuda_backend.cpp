#include "context_fabric/backends/cuda/cuda_backend.h"

#include <algorithm>
#include <limits>
#include <new>
#include <vector>

#include <cuda_runtime.h>

#include "context_fabric/backends/cuda/cuda_kernels.h"
#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"

namespace ctf {

namespace {

[[noreturn]] void throw_cuda_failure(cudaError_t err, const char* operation) {
    throw_error(ErrKind::Cuda, std::string(operation) + " failed: " + cudaGetErrorString(err),
                static_cast<uint64_t>(err));
}

void check_cuda(cudaError_t err, const char* operation) {
    if (err != cudaSuccess) throw_cuda_failure(err, operation);
}

uint64_t safe_capacity(uint64_t device_total, uint64_t configured) {
    if (configured != 0) {
        // An explicit operator limit is authoritative, subject only to the
        // physical device size. Allocation failures still surface as CUDA
        // errors when other processes or the driver consume device memory.
        return std::min(configured, device_total);
    }

    uint64_t conservative = device_total / 5;
    constexpr uint64_t kMaximumDefault = 6ull * 1024 * 1024 * 1024;
    conservative = std::min(conservative, kMaximumDefault);
    return conservative;
}

std::vector<uint8_t> copy_device_to_host(const uint8_t* device, uint64_t size,
                                         cudaStream_t stream) {
    if (size == 0) return {};
    if (size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        throw_error(ErrKind::Capacity, "CUDA transfer exceeds host addressable size");
    }
    std::vector<uint8_t> host(static_cast<size_t>(size));
    check_cuda(cudaMemcpyAsync(host.data(), device, static_cast<size_t>(size),
                               cudaMemcpyDeviceToHost, stream),
               "cudaMemcpyAsync D2H");
    check_cuda(cudaStreamSynchronize(stream), "cudaStreamSynchronize");
    return host;
}

}  // namespace

CudaAcceleratorBackend::CudaAcceleratorBackend(uint64_t accelerator_capacity_bytes)
    : configured_capacity_(accelerator_capacity_bytes) {
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || count <= 0) return;
    if (cudaSetDevice(device_) != cudaSuccess) return;

    cudaDeviceProp prop{};
    if (cudaGetDeviceProperties(&prop, device_) != cudaSuccess) return;
    capacity_.store(safe_capacity(static_cast<uint64_t>(prop.totalGlobalMem), configured_capacity_),
                    std::memory_order_relaxed);

    cudaStream_t stream = nullptr;
    if (cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking) != cudaSuccess) return;
    stream_ = stream;
    available_.store(true, std::memory_order_release);
}

CudaAcceleratorBackend::~CudaAcceleratorBackend() {
    shutdown();
}

bool CudaAcceleratorBackend::available() const noexcept {
    return available_.load(std::memory_order_acquire);
}

uint64_t CudaAcceleratorBackend::device_count() const noexcept {
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || count < 0) return 0;
    return static_cast<uint64_t>(count);
}

std::string CudaAcceleratorBackend::device_name(uint64_t index) const {
    if (index > static_cast<uint64_t>(std::numeric_limits<int>::max())) return "<unknown>";
    cudaDeviceProp prop{};
    if (cudaGetDeviceProperties(&prop, static_cast<int>(index)) != cudaSuccess) return "<unknown>";
    return std::string(prop.name);
}

uint64_t CudaAcceleratorBackend::device_total_memory(uint64_t index) const {
    if (index > static_cast<uint64_t>(std::numeric_limits<int>::max())) return 0;
    cudaDeviceProp prop{};
    if (cudaGetDeviceProperties(&prop, static_cast<int>(index)) != cudaSuccess) return 0;
    return static_cast<uint64_t>(prop.totalGlobalMem);
}

uint64_t CudaAcceleratorBackend::device_compute_capability_major(uint64_t index) const {
    if (index > static_cast<uint64_t>(std::numeric_limits<int>::max())) return 0;
    cudaDeviceProp prop{};
    if (cudaGetDeviceProperties(&prop, static_cast<int>(index)) != cudaSuccess) return 0;
    return static_cast<uint64_t>(prop.major);
}

uint64_t CudaAcceleratorBackend::device_compute_capability_minor(uint64_t index) const {
    if (index > static_cast<uint64_t>(std::numeric_limits<int>::max())) return 0;
    cudaDeviceProp prop{};
    if (cudaGetDeviceProperties(&prop, static_cast<int>(index)) != cudaSuccess) return 0;
    return static_cast<uint64_t>(prop.minor);
}

uint64_t CudaAcceleratorBackend::capacity_bytes() const noexcept {
    return capacity_.load(std::memory_order_relaxed);
}

uint64_t CudaAcceleratorBackend::used_bytes() const noexcept {
    return used_.load(std::memory_order_relaxed);
}

uint64_t CudaAcceleratorBackend::high_water_bytes() const noexcept {
    return high_water_.load(std::memory_order_relaxed);
}

void CudaAcceleratorBackend::select_device_for_thread_locked() {
    check_cuda(cudaSetDevice(device_), "cudaSetDevice");
}

void CudaAcceleratorBackend::require_allocation_locked(const uint8_t* ptr, uint64_t size,
                                                       const char* operation) const {
    if (size == 0) return;
    if (ptr == nullptr) {
        throw_error(ErrKind::InvalidArgument, std::string(operation) + " requires a non-null device pointer");
    }
    auto it = allocations_.find(const_cast<uint8_t*>(ptr));
    if (it == allocations_.end() || size > it->second) {
        throw_error(ErrKind::InvalidArgument,
                    std::string(operation) + " exceeds or does not reference an owned allocation");
    }
}

void CudaAcceleratorBackend::set_device(uint64_t index) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!available_.load(std::memory_order_acquire) || shutdown_) {
        throw_error(ErrKind::Cuda, "CUDA backend unavailable");
    }
    // A node owns one CUDA device in v1. Device enumeration is exposed for
    // discovery, but multi-GPU node residency is an explicit future feature.
    if (index != static_cast<uint64_t>(device_)) {
        throw_error(ErrKind::NotSupported, "multi-GPU device selection is not supported");
    }
    select_device_for_thread_locked();
}

void CudaAcceleratorBackend::allocate(uint8_t*& ptr, uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!available_.load(std::memory_order_acquire) || shutdown_) {
        throw_error(ErrKind::Cuda, "CUDA backend unavailable");
    }
    if (size == 0) {
        ptr = nullptr;
        return;
    }
    if (size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        throw_error(ErrKind::Capacity, "CUDA allocation exceeds host addressable size");
    }

    const uint64_t used = used_.load(std::memory_order_relaxed);
    const uint64_t capacity = capacity_.load(std::memory_order_relaxed);
    if (used > capacity || size > capacity - used) {
        throw_error(ErrKind::Capacity, "accelerator capacity exceeded");
    }

    select_device_for_thread_locked();
    void* allocation = nullptr;
    check_cuda(cudaMallocAsync(&allocation, static_cast<size_t>(size),
                               static_cast<cudaStream_t>(stream_)),
               "cudaMallocAsync");

    auto* device_ptr = static_cast<uint8_t*>(allocation);
    try {
        allocations_.emplace(device_ptr, size);
    } catch (const std::bad_alloc&) {
        (void)cudaFreeAsync(device_ptr, static_cast<cudaStream_t>(stream_));
        (void)cudaStreamSynchronize(static_cast<cudaStream_t>(stream_));
        throw_error(ErrKind::Capacity, "unable to track CUDA allocation");
    }

    const uint64_t new_used = used + size;
    used_.store(new_used, std::memory_order_relaxed);
    high_water_.store(std::max(high_water_.load(std::memory_order_relaxed), new_used),
                      std::memory_order_relaxed);
    ptr = device_ptr;
}

void CudaAcceleratorBackend::free(uint8_t* ptr) {
    if (ptr == nullptr) return;
    std::lock_guard<std::mutex> lock(mutex_);
    if (!available_.load(std::memory_order_acquire) || shutdown_) {
        throw_error(ErrKind::Cuda, "CUDA backend unavailable");
    }
    auto it = allocations_.find(ptr);
    if (it == allocations_.end()) {
        throw_error(ErrKind::InvalidArgument, "attempted to free an unknown CUDA allocation");
    }

    select_device_for_thread_locked();
    check_cuda(cudaFreeAsync(ptr, static_cast<cudaStream_t>(stream_)), "cudaFreeAsync");
    const uint64_t size = it->second;
    allocations_.erase(it);
    const uint64_t used = used_.load(std::memory_order_relaxed);
    used_.store(size <= used ? used - size : 0, std::memory_order_relaxed);
}

void CudaAcceleratorBackend::copy_h2d(uint8_t* dst_device, const uint8_t* src_host, uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!available_.load(std::memory_order_acquire) || shutdown_) {
        throw_error(ErrKind::Cuda, "CUDA backend unavailable");
    }
    require_allocation_locked(dst_device, size, "CUDA host-to-device copy");
    if (size == 0) return;
    if (src_host == nullptr) throw_error(ErrKind::InvalidArgument, "CUDA copy requires non-null host data");
    select_device_for_thread_locked();
    check_cuda(cudaMemcpyAsync(dst_device, src_host, static_cast<size_t>(size),
                               cudaMemcpyHostToDevice, static_cast<cudaStream_t>(stream_)),
               "cudaMemcpyAsync H2D");
}

void CudaAcceleratorBackend::copy_d2h(uint8_t* dst_host, const uint8_t* src_device, uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!available_.load(std::memory_order_acquire) || shutdown_) {
        throw_error(ErrKind::Cuda, "CUDA backend unavailable");
    }
    require_allocation_locked(src_device, size, "CUDA device-to-host copy");
    if (size == 0) return;
    if (dst_host == nullptr) throw_error(ErrKind::InvalidArgument, "CUDA copy requires non-null host data");
    select_device_for_thread_locked();
    check_cuda(cudaMemcpyAsync(dst_host, src_device, static_cast<size_t>(size),
                               cudaMemcpyDeviceToHost, static_cast<cudaStream_t>(stream_)),
               "cudaMemcpyAsync D2H");
}

void CudaAcceleratorBackend::verify(const uint8_t* device, uint64_t size, uint64_t expected_crc,
                                    uint64_t expected_sha) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!available_.load(std::memory_order_acquire) || shutdown_) {
        throw_error(ErrKind::Cuda, "CUDA backend unavailable");
    }
    require_allocation_locked(device, size, "CUDA verification");
    select_device_for_thread_locked();
    const cudaStream_t stream = static_cast<cudaStream_t>(stream_);

    std::vector<uint8_t> host = copy_device_to_host(device, size, stream);
    IntegrityId actual = compute_integrity(host.data(), host.size());
    if (actual.crc32c != expected_crc || actual.sha != expected_sha) {
        throw_error(ErrKind::Integrity, "CUDA round-trip integrity mismatch");
    }

    uint64_t device_sum = 0;
    uint64_t* device_sum_ptr = nullptr;
    void* checksum_allocation = nullptr;
    check_cuda(cudaMallocAsync(&checksum_allocation, sizeof(uint64_t), stream),
               "cudaMallocAsync checksum");
    device_sum_ptr = static_cast<uint64_t*>(checksum_allocation);
    try {
        check_cuda(cudaMemsetAsync(device_sum_ptr, 0, sizeof(uint64_t), stream),
                   "cudaMemsetAsync checksum");
        cf_launch_sum_checksum(device, size, device_sum_ptr, stream_);
        check_cuda(cudaPeekAtLastError(), "CUDA checksum kernel launch");
        check_cuda(cudaMemcpyAsync(&device_sum, device_sum_ptr, sizeof(uint64_t),
                                   cudaMemcpyDeviceToHost, stream),
                   "cudaMemcpyAsync checksum D2H");
        check_cuda(cudaStreamSynchronize(stream), "cudaStreamSynchronize checksum");
        check_cuda(cudaFreeAsync(device_sum_ptr, stream), "cudaFreeAsync checksum");
        device_sum_ptr = nullptr;
        check_cuda(cudaStreamSynchronize(stream), "cudaStreamSynchronize checksum free");
    } catch (...) {
        if (device_sum_ptr != nullptr) {
            (void)cudaFreeAsync(device_sum_ptr, stream);
            (void)cudaStreamSynchronize(stream);
        }
        throw;
    }

    if (device_sum != cf_host_sum_checksum(host.data(), host.size())) {
        throw_error(ErrKind::Integrity, "CUDA device checksum did not match host computation");
    }
}

void CudaAcceleratorBackend::synchronize() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!available_.load(std::memory_order_acquire) || shutdown_) {
        throw_error(ErrKind::Cuda, "CUDA backend unavailable");
    }
    select_device_for_thread_locked();
    check_cuda(cudaStreamSynchronize(static_cast<cudaStream_t>(stream_)), "cudaStreamSynchronize");
}

void CudaAcceleratorBackend::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) return;
    shutdown_ = true;

    if (stream_ != nullptr && cudaSetDevice(device_) == cudaSuccess) {
        (void)cudaStreamSynchronize(static_cast<cudaStream_t>(stream_));
        for (const auto& [ptr, size] : allocations_) {
            (void)size;
            (void)cudaFreeAsync(ptr, static_cast<cudaStream_t>(stream_));
        }
        (void)cudaStreamSynchronize(static_cast<cudaStream_t>(stream_));
        (void)cudaStreamDestroy(static_cast<cudaStream_t>(stream_));
    }
    allocations_.clear();
    used_.store(0, std::memory_order_relaxed);
    stream_ = nullptr;
    available_.store(false, std::memory_order_release);
}

AcceleratorBackend* create_cuda_accelerator_backend(uint64_t accelerator_capacity_bytes) {
    return new CudaAcceleratorBackend(accelerator_capacity_bytes);
}

}  // namespace ctf
