cmake_minimum_required(VERSION 3.24)

project(context_fabric
        VERSION 1.0.0
        DESCRIPTION "Context Fabric: a vendor-neutral distributed runtime for reusable AI computational state"
        LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(CONTEXT_FABRIC_BUILD_TESTS      "Build Context Fabric native tests"           ON)
option(CONTEXT_FABRIC_BUILD_BENCHMARKS "Build Context Fabric benchmarks"             OFF)
option(CONTEXT_FABRIC_BUILD_EXAMPLES   "Build Context Fabric examples"               ON)
option(CONTEXT_FABRIC_BUILD_CLI        "Build the context-fabric CLI"                ON)
option(CONTEXT_FABRIC_ENABLE_CUDA      "Enable the CUDA accelerator backend"         ON)
option(CONTEXT_FABRIC_ENABLE_FLASHTIER "Enable the optional FlashTier adapter"       OFF)
option(CONTEXT_FABRIC_WARNINGS_AS_ERRORS "Treat project-owned compiler warnings as errors" ON)

set(CONTEXT_FABRIC_FLASHTIER_DIR "" CACHE PATH
    "Path to a sibling FlashTier repository to enable the node-local residency adapter")

# ---------------------------------------------------------------------------
# Toolchain / standards
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---------------------------------------------------------------------------
# CUDA (optional)
# ---------------------------------------------------------------------------
set(CONTEXT_FABRIC_HAS_CUDA OFF)
if(CONTEXT_FABRIC_ENABLE_CUDA)
  find_package(CUDAToolkit QUIET)
  if(CUDAToolkit_FOUND)
    set(CONTEXT_FABRIC_HAS_CUDA ON)
    enable_language(CUDA)
    set(CMAKE_CUDA_STANDARD 17)
    set(CMAKE_CUDA_STANDARD_REQUIRED ON)
    # Default to sm_120 (RTX 50-series / Blackwell). Override for other GPUs,
    # e.g. -DCONTEXT_FABRIC_CUDA_ARCHS=90 or "75;89".
    set(CONTEXT_FABRIC_CUDA_ARCHS "120" CACHE STRING "CUDA architectures to compile for")
    set(CMAKE_CUDA_ARCHITECTURES ${CONTEXT_FABRIC_CUDA_ARCHS})
    message(STATUS "Context Fabric: CUDA toolkit found (${CUDAToolkit_VERSION}), archs=${CMAKE_CUDA_ARCHITECTURES}")
  else()
    message(STATUS "Context Fabric: CUDA requested but toolkit not found; building CPU-only")
  endif()
else()
  message(STATUS "Context Fabric: CUDA disabled by configuration; building CPU-only")
endif()

# ---------------------------------------------------------------------------
# Common compiler setup
# ---------------------------------------------------------------------------
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CompilerWarnings.cmake)

# ---------------------------------------------------------------------------
# Libraries
# ---------------------------------------------------------------------------
add_subdirectory(src)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(CONTEXT_FABRIC_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
if(CONTEXT_FABRIC_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------
if(CONTEXT_FABRIC_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
message(STATUS "Context Fabric configuration summary:")
message(STATUS "  Version                     : ${PROJECT_VERSION}")
message(STATUS "  Build type                  : ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ standard                : C++${CMAKE_CXX_STANDARD}")
message(STATUS "  CUDA backend                : ${CONTEXT_FABRIC_HAS_CUDA}")
message(STATUS "  CUDA architectures          : ${CMAKE_CUDA_ARCHITECTURES}")
message(STATUS "  FlashTier adapter           : ${CONTEXT_FABRIC_ENABLE_FLASHTIER}")
message(STATUS "  Tests                       : ${CONTEXT_FABRIC_BUILD_TESTS}")
message(STATUS "  Benchmarks                  : ${CONTEXT_FABRIC_BUILD_BENCHMARKS}")
message(STATUS "  Examples                    : ${CONTEXT_FABRIC_BUILD_EXAMPLES}")