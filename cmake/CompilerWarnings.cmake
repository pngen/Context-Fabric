# Per-compiler warning policy for project-owned code.
#
# All project-owned translation units build with an aggressive warning set.
# When CONTEXT_FABRIC_WARNINGS_AS_ERRORS is ON (the default for release-shaped
# configurations) warnings are promoted to errors.

set(_CF_WARN_TARGETS "")

function(context_fabric_enable_warnings target)
  if(MSVC)
    target_compile_options(${target} PRIVATE $<$<COMPILE_LANGUAGE:CXX>:/W4 /permissive->)
    if(CONTEXT_FABRIC_WARNINGS_AS_ERRORS)
      target_compile_options(${target} PRIVATE $<$<COMPILE_LANGUAGE:CXX>:/WX>)
    endif()
    # Disable a small, well-understood set of third-party/toolchain noise that
    # is not produced by project-owned code.
    target_compile_options(${target} PRIVATE $<$<COMPILE_LANGUAGE:CXX>:/wd4819>)
  else()
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
    if(CONTEXT_FABRIC_WARNINGS_AS_ERRORS)
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
endfunction()

function(context_fabric_enable_cuda_warnings target)
  if(NOT CONTEXT_FABRIC_HAS_CUDA)
    return()
  endif()
  if(MSVC)
    target_compile_options(${target} PRIVATE
      $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/W4>
      $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/WX>)
    # MSVC's own intrinsics headers and host compiler emit some harmless
    # "unreferenced local function" / name-lookup noise under nvcc; those are
    # toolchain artifacts, not project-owned warnings.
    target_compile_options(${target} PRIVATE
      $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/wd4819>)
  else()
    target_compile_options(${target} PRIVATE
      $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=-Wall>
      $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=-Wextra>)
  endif()
endfunction()