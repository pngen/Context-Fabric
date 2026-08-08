# ---------------------------------------------------------------------------
# core
# ---------------------------------------------------------------------------
add_library(context_fabric_core STATIC
  core/error.cpp
  core/types.cpp
  core/object_id.cpp
  core/generation.cpp
  core/node.cpp
  core/metadata.cpp
  core/serializer.cpp
  core/integrity.cpp
  core/prng.cpp
  core/state_machine.cpp
  core/lineage.cpp
  core/json.cpp
  core/telemetry.cpp
  core/clock.cpp
)
target_include_directories(context_fabric_core PUBLIC ${PROJECT_SOURCE_DIR}/include)
context_fabric_enable_warnings(context_fabric_core)
target_compile_definitions(context_fabric_core PUBLIC CONTEXT_FABRIC_VERSION="${PROJECT_VERSION}")

# ---------------------------------------------------------------------------
# transport + protocol
# ---------------------------------------------------------------------------
add_library(context_fabric_transport STATIC
  protocol/messages.cpp
  transport/socket.cpp
  transport/frame.cpp
  transport/tcp_transport.cpp
)
target_include_directories(context_fabric_transport PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(context_fabric_transport PUBLIC context_fabric_core)
context_fabric_enable_warnings(context_fabric_transport)

# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------
add_library(context_fabric_storage STATIC
  storage/host_memory_backend.cpp
  storage/persistent_backend.cpp
  storage/accelerator_backend.cpp
  storage/flashtier_adapter.cpp
)
target_include_directories(context_fabric_storage PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(context_fabric_storage PUBLIC context_fabric_core)
context_fabric_enable_warnings(context_fabric_storage)
if(CONTEXT_FABRIC_HAS_CUDA)
  target_compile_definitions(context_fabric_storage PUBLIC CONTEXT_FABRIC_HAS_CUDA=1)
endif()

# ---------------------------------------------------------------------------
# placement
# ---------------------------------------------------------------------------
add_library(context_fabric_placement STATIC
  placement/placement_engine.cpp
  placement/cost_model.cpp
)
target_include_directories(context_fabric_placement PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(context_fabric_placement PUBLIC context_fabric_core)
context_fabric_enable_warnings(context_fabric_placement)

# ---------------------------------------------------------------------------
# authority (coordinator + node runtime)
# ---------------------------------------------------------------------------
add_library(context_fabric_authority STATIC
  authority/coordinator.cpp
  authority/coordinator_store.cpp
  authority/coordinator_state.cpp
  authority/client.cpp
  runtime/object_store.cpp
  runtime/node_runtime.cpp
)
target_include_directories(context_fabric_authority PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(context_fabric_authority PUBLIC
  context_fabric_core
  context_fabric_transport
  context_fabric_storage
  context_fabric_placement
)
context_fabric_enable_warnings(context_fabric_authority)

# ---------------------------------------------------------------------------
# CUDA accelerator backend (optional)
# ---------------------------------------------------------------------------
if(CONTEXT_FABRIC_HAS_CUDA)
  add_library(context_fabric_cuda STATIC
    backends/cuda/cuda_backend.cpp
    backends/cuda/cuda_kernels.cu
  )
  target_include_directories(context_fabric_cuda PUBLIC ${PROJECT_SOURCE_DIR}/include)
  target_include_directories(context_fabric_cuda PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
  target_link_libraries(context_fabric_cuda PUBLIC context_fabric_core ${CUDAToolkit_LIBRARIES})
  context_fabric_enable_warnings(context_fabric_cuda)
  context_fabric_enable_cuda_warnings(context_fabric_cuda)
  target_compile_definitions(context_fabric_cuda PUBLIC CONTEXT_FABRIC_HAS_CUDA=1)
  if(MSVC)
    # nvcc does not tolerate MSVC's /FS (edit-and-continue) PDB flag.
    # Set the per-target debug format to Embedded (/Z7) which avoids /FS.
    set_property(TARGET context_fabric_cuda PROPERTY MSVC_DEBUG_INFORMATION_FORMAT Embedded)
  endif()
endif()

# ---------------------------------------------------------------------------
# umbrella library exposing the public API
# ---------------------------------------------------------------------------
add_library(context_fabric STATIC
  api/context_fabric.cpp
)
target_include_directories(context_fabric PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(context_fabric PUBLIC
  context_fabric_core
  context_fabric_transport
  context_fabric_storage
  context_fabric_placement
  context_fabric_authority
)
context_fabric_enable_warnings(context_fabric)
if(CONTEXT_FABRIC_HAS_CUDA AND TARGET context_fabric_cuda)
  target_link_libraries(context_fabric PUBLIC context_fabric_cuda)
endif()

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if(CONTEXT_FABRIC_BUILD_CLI)
  add_executable(context-fabric
    cli/main.cpp
    cli/cli.cpp
    cli/fabric_launcher.cpp
    cli/process.cpp
  )
  target_include_directories(context-fabric PRIVATE ${PROJECT_SOURCE_DIR}/include)
  target_include_directories(context-fabric PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cli)
  if(CONTEXT_FABRIC_BUILD_BENCHMARKS)
    target_include_directories(context-fabric PRIVATE ${PROJECT_SOURCE_DIR}/benchmarks)
    target_link_libraries(context-fabric PRIVATE context_fabric_benchsuite)
    target_compile_definitions(context-fabric PRIVATE CONTEXT_FABRIC_BUILD_BENCHMARKS=1)
  endif()
  target_link_libraries(context-fabric PRIVATE context_fabric)
  context_fabric_enable_warnings(context-fabric)
endif()