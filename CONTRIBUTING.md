# Contributing

Thank you for considering contributing to Context Fabric.

## Ground rules

- Context Fabric is a serious systems infrastructure project. Favor
  correctness, explicit state, deterministic behavior, inspectability, and
  honest evidence over cleverness.
- The core must remain workload-independent. Framework/model-specific adapters
  live above the core abstractions.
- Do not claim functionality that is not implemented. Unsupported capabilities
  must report unsupported.
- Do not weaken tests to make them pass; fix the implementation.
- Keep the project warning-clean with warnings-as-errors in release builds.

## Development workflow

1. Fork the repository and create a feature branch.
2. Make focused changes with clear commit messages.
3. Build and run the test suites:

   ```sh
   cmake --preset cpu-tests && cmake --build build/cpu-tests
   ctest --preset cpu-tests
   # and, if you have a CUDA toolkit:
   cmake --preset cuda-tests && cmake --build build/cuda-tests
   ctest --preset cuda-tests
   ```

4. Run the benchmark suite to confirm no gross regressions:

   ```sh
   cmake --preset cpu-release --build build/cpu-release
   build/cpu-release/benchmarks/context_fabric_bench --quick
   ```

5. Open a pull request.

## Code style

- Modern C++20. Prefer the C++ standard library and OS APIs.
- Avoid heavyweight dependencies; do not add Boost casually.
- Keep platform-specific code behind narrow interfaces (see the socket
  abstraction and the accelerator backend interface).
- Do not add comments unless they clarify non-obvious invariants.
- Follow the naming conventions in the existing code (`Context*` public types,
  `snake_case` members, `kConstant` constants).

## Testing expectations

- New invariants should be covered by native tests in `tests/`.
- Multi-node behavior should be exercised through the real multi-process
  fabric, not only in-process mocks.
- Every benchmark and validation claim must correspond to code that actually
  ran.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0 (see `LICENSE`). No CLA is required.