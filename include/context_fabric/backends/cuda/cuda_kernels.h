#pragma once

#include <cstdint>

extern "C" {

// Device-side sum checksum (implemented in cuda_kernels.cu). Launches a kernel
// that computes a 64-bit sum-of-words checksum on the device; used to
// cross-check device computation against host computation in accelerator
// verification.
void cf_launch_sum_checksum(const uint8_t* data, uint64_t size, uint64_t* out, void* stream);

// Host-side equivalent of the device sum checksum.
uint64_t cf_host_sum_checksum(const uint8_t* data, uint64_t size);

}  // extern "C"