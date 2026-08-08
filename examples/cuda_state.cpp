// cuda_state: upload/download deterministic state through CUDA when available.
//
// Uses the accelerator backend directly: host -> device -> host with
// integrity verification at every boundary. Skips gracefully when no CUDA
// device is present.

#include <cstdio>
#include <exception>
#include <memory>

#include "context_fabric/core/integrity.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/storage/accelerator_backend.h"

using namespace ctf;

int main() {
    std::unique_ptr<AcceleratorBackend> acc(create_accelerator_backend(1ull * 1024 * 1024 * 1024));
    if (!acc || !acc->available()) {
        std::printf("no CUDA device available; skipping\n");
        return 0;
    }
    try {
    acc->set_device(0);
    std::printf("device: %s (compute capability %llu.%llu, %llu bytes)\n", acc->device_name(0).c_str(),
                static_cast<unsigned long long>(acc->device_compute_capability_major(0)),
                static_cast<unsigned long long>(acc->device_compute_capability_minor(0)),
                static_cast<unsigned long long>(acc->device_total_memory(0)));

    const uint64_t size = 8ull * 1024 * 1024;
    auto content = generate_content(5, 5, size);
    IntegrityId expected = compute_integrity(content.data(), content.size());

    uint8_t* device = nullptr;
    acc->allocate(device, size);
    struct DeviceAllocation {
        AcceleratorBackend* backend;
        uint8_t*& pointer;
        ~DeviceAllocation() {
            if (pointer == nullptr) return;
            try {
                backend->free(pointer);
            } catch (...) {
            }
            pointer = nullptr;
        }
    } allocation{acc.get(), device};
    if (!device) {
        std::printf("allocation failed\n");
        return 1;
    }

    acc->copy_h2d(device, content.data(), size);
    acc->synchronize();
    std::printf("host -> device: %llu bytes\n", static_cast<unsigned long long>(size));

    acc->verify(device, size, expected.crc32c, expected.sha);
    std::printf("device integrity verified (crc=%016llx sha=%016llx)\n",
                static_cast<unsigned long long>(expected.crc32c),
                static_cast<unsigned long long>(expected.sha));

    std::vector<uint8_t> back(size);
    acc->copy_d2h(back.data(), device, size);
    acc->synchronize();

    bool exact = back == content;
    std::printf("device -> host: byte-exact=%s\n", exact ? "yes" : "NO");

    acc->free(device);
    device = nullptr;
    acc->shutdown();
    return exact ? 0 : 1;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "CUDA example error: %s\n", e.what());
        try {
            acc->shutdown();
        } catch (...) {
        }
        return 1;
    }
}
