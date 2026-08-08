# Context Fabric native tests. See tests/test_harness.h for the lightweight
# project-owned harness. Tests are registered with CTest.
add_executable(context_fabric_tests
  test_main.cpp
  test_core.cpp
  test_protocol.cpp
  test_transport.cpp
  test_storage.cpp
  test_placement.cpp
  test_authority.cpp
  test_fabric_integration.cpp
)
if(CONTEXT_FABRIC_HAS_CUDA)
  target_sources(context_fabric_tests PRIVATE test_cuda.cpp)
endif()
target_include_directories(context_fabric_tests PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(context_fabric_tests PRIVATE context_fabric)
context_fabric_enable_warnings(context_fabric_tests)

add_test(NAME context_fabric_tests COMMAND context_fabric_tests)
set_tests_properties(context_fabric_tests PROPERTIES TIMEOUT 180)

# The launcher process primitive is intentionally tested separately so the
# test can spawn itself and verify argument fidelity, timeout semantics, and
# descendant-tree cleanup without exposing process.h as a public API.
add_executable(context_fabric_process_tests
  test_process.cpp
  ${PROJECT_SOURCE_DIR}/src/cli/process.cpp
)
target_include_directories(context_fabric_process_tests PRIVATE ${PROJECT_SOURCE_DIR}/src/cli)
context_fabric_enable_warnings(context_fabric_process_tests)
add_test(NAME context_fabric_process_tests COMMAND context_fabric_process_tests)
set_tests_properties(context_fabric_process_tests PROPERTIES TIMEOUT 30)

if(CONTEXT_FABRIC_BUILD_CLI)
  add_test(NAME context_fabric_cli_fabric_local
           COMMAND context-fabric fabric local --nodes 3 --quick)
  set_tests_properties(context_fabric_cli_fabric_local
                       PROPERTIES TIMEOUT 90 LABELS "multiprocess;lifecycle")
endif()
