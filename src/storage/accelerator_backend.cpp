#include "context_fabric/storage/accelerator_backend.h"

namespace ctf {

// The CUDA backend is compiled only when CONTEXT_FABRIC_HAS_CUDA is defined.
// Its factory is declared in cuda/cuda_backend.h.
#if defined(CONTEXT_FABRIC_HAS_CUDA)
AcceleratorBackend* create_cuda_accelerator_backend(uint64_t accelerator_capacity_bytes);
#endif

AcceleratorBackend* create_accelerator_backend(uint64_t accelerator_capacity_bytes) {
#if defined(CONTEXT_FABRIC_HAS_CUDA)
    return create_cuda_accelerator_backend(accelerator_capacity_bytes);
#else
    (void)accelerator_capacity_bytes;
    return nullptr;
#endif
}

}  // namespace ctf
