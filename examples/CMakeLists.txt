add_executable(example_minimal_object minimal_object.cpp)
target_link_libraries(example_minimal_object PRIVATE context_fabric)
context_fabric_enable_warnings(example_minimal_object)

add_executable(example_local_multi_node local_multi_node.cpp)
target_link_libraries(example_local_multi_node PRIVATE context_fabric)
context_fabric_enable_warnings(example_local_multi_node)

add_executable(example_migrate_state migrate_state.cpp)
target_link_libraries(example_migrate_state PRIVATE context_fabric)
context_fabric_enable_warnings(example_migrate_state)

add_executable(example_persistent_state persistent_state.cpp)
target_link_libraries(example_persistent_state PRIVATE context_fabric)
context_fabric_enable_warnings(example_persistent_state)

add_executable(example_cuda_state cuda_state.cpp)
target_link_libraries(example_cuda_state PRIVATE context_fabric)
context_fabric_enable_warnings(example_cuda_state)

add_executable(example_reuse_vs_recompute reuse_vs_recompute.cpp)
target_link_libraries(example_reuse_vs_recompute PRIVATE context_fabric)
context_fabric_enable_warnings(example_reuse_vs_recompute)

if(CONTEXT_FABRIC_BUILD_TESTS)
  add_test(NAME example_minimal_object COMMAND example_minimal_object)
  add_test(NAME example_local_multi_node COMMAND example_local_multi_node)
  add_test(NAME example_migrate_state COMMAND example_migrate_state)
  add_test(NAME example_persistent_state COMMAND example_persistent_state)
  add_test(NAME example_cuda_state COMMAND example_cuda_state)
  add_test(NAME example_reuse_vs_recompute COMMAND example_reuse_vs_recompute)
  set_tests_properties(
    example_minimal_object
    example_local_multi_node
    example_migrate_state
    example_persistent_state
    example_cuda_state
    example_reuse_vs_recompute
    PROPERTIES TIMEOUT 60 LABELS "examples")
endif()
