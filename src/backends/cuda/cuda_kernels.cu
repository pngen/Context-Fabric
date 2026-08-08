#include <cstdint>
#include <cuda_runtime.h>

// ---------------------------------------------------------------------------
// Device-side checksum kernel used for accelerator-native integrity checks.
// Computes a simple 64-bit sum-of-words checksum over the buffer on the GPU so
// that verification is genuinely device-backed (not only a host round-trip).
// ---------------------------------------------------------------------------
extern "C" __global__ void cf_device_sum_checksum(const uint8_t* data, uint64_t size, uint64_t* out) {
    uint64_t sum = 0;
    for (uint64_t i = blockIdx.x * blockDim.x + threadIdx.x; i < size;
         i += static_cast<uint64_t>(gridDim.x) * blockDim.x) {
        sum += static_cast<uint64_t>(data[i]) * (i + 1);
    }
    if (sum != 0) {
        atomicAdd(out, sum);
    }
}

extern "C" void cf_launch_sum_checksum(const uint8_t* data, uint64_t size, uint64_t* out, void* stream) {
    if (size == 0) return;
    uint64_t blocks = size / 256 + (size % 256 != 0 ? 1 : 0);
    if (blocks > 1024) blocks = 1024;
    cf_device_sum_checksum<<<static_cast<unsigned>(blocks), 256, 0, static_cast<cudaStream_t>(stream)>>>(
        data, size, out);
}

extern "C" uint64_t cf_host_sum_checksum(const uint8_t* data, uint64_t size) {
    uint64_t sum = 0;
    for (uint64_t i = 0; i < size; ++i) {
        sum += static_cast<uint64_t>(data[i]) * (i + 1);
    }
    return sum;
}
