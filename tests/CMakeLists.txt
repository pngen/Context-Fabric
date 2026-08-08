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
set_tests_properties(context_fabric_tests PROPERTIES TIMEOUT 600)