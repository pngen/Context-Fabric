#include "test_harness.h"

#if defined(CONTEXT_FABRIC_HAS_CUDA)

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>

#include "context_fabric/api/context_fabric.h"
#include "context_fabric/authority/coordinator.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/runtime/node_runtime.h"
#include "context_fabric/storage/accelerator_backend.h"

using namespace ctf;

namespace {

struct CudaCoord {
    std::string store_path;
    Telemetry telemetry;
    Coordinator coord;

    CudaCoord() : store_path(make_store()), telemetry(), coord(make_cfg(store_path), telemetry) {
        telemetry.set_stderr_enabled(false);
        if (!coord.start()) throw CfError(ErrKind::Internal, "coordinator start failed");
    }
    ~CudaCoord() {
        coord.shutdown();
        std::error_code ec;
        std::filesystem::remove(store_path, ec);
        std::filesystem::remove(store_path + ".tmp", ec);
    }
    static std::string make_store() {
        return (std::filesystem::temp_directory_path() / ("ctf-cudac-" + ObjectId::random().to_hex())).string() +
               ".store";
    }
    static Coordinator::Config make_cfg(const std::string& store) {
        Coordinator::Config cfg;
        cfg.bind_host = "127.0.0.1";
        cfg.control_port = 0;
        cfg.store_path = store;
        cfg.telemetry_path = "";
        cfg.heartbeat_interval_ms = 500;
        cfg.session_timeout_ms = 3000;
        cfg.lease_default_ms = 2000;
        cfg.op_timeout_ms = 20000;
        cfg.create_timeout_ms = 10000;
        cfg.driver_threads = 4;
        return cfg;
    }
    uint16_t port() const { return coord.bound_port(); }
};

struct CudaNode {
    std::string root;
    Telemetry telemetry;
    NodeRuntime node;

    CudaNode(const std::string& name, uint16_t coord_port, bool cuda)
        : root((std::filesystem::temp_directory_path() / ("ctf-cudan-" + name + "-" + ObjectId::random().to_hex()))
                   .string()),
          node(make_cfg(name, coord_port, cuda), telemetry) {
        std::filesystem::create_directories(root);
        telemetry.set_stderr_enabled(false);
        if (!node.start()) throw CfError(ErrKind::Internal, "node start failed");
    }
    ~CudaNode() {
        node.shutdown();
        std::error_code ec;
        std::filesystem::remove_all(root, ec);
    }
    static NodeConfig make_cfg(const std::string& name, uint16_t coord_port, bool cuda) {
        NodeConfig cfg;
        cfg.node_id = NodeId::deterministic(name);
        cfg.label = name;
        cfg.coord_host = "127.0.0.1";
        cfg.coord_port = coord_port;
        cfg.host_capacity = 64ull * 1024 * 1024;
        cfg.accelerator_capacity = 1ull * 1024 * 1024 * 1024;
        cfg.enable_cuda = cuda;
        cfg.heartbeat_interval_ms = 500;
        cfg.transfer_workers = 4;
        return cfg;
    }
};

template <typename F>
bool cwait(F&& f, uint64_t timeout_ms, uint64_t step_ms = 50) {
    uint64_t waited = 0;
    while (waited < timeout_ms) {
        if (f()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
        waited += step_ms;
    }
    return f();
}

ContextMetadata cudamd(uint64_t bytes) {
    ContextMetadata md;
    md.namespace_id = "cuda";
    md.semantic_class = SemanticClass::Tensor;
    md.byte_size = bytes;
    md.logical_size = bytes;
    md.sharing = SharingPolicy::NamespaceShared;
    md.persistence = PersistencePolicy::Volatile;
    md.model_id = "cuda-test";
    md.compatibility_fingerprint = md.compute_compatibility_fingerprint();
    return md;
}

}  // namespace

CF_TEST(cuda_device_discovery) {
    std::unique_ptr<AcceleratorBackend> acc(create_accelerator_backend(1ull * 1024 * 1024 * 1024));
    CF_REQUIRE(acc != nullptr);
    CF_REQUIRE(acc->available());
    CF_CHECK(acc->device_count() >= 1);
    std::string name = acc->device_name(0);
    CF_CHECK(!name.empty());
    CF_CHECK(acc->device_compute_capability_major(0) > 0);
    CF_CHECK(acc->capacity_bytes() > 0);
    CF_CHECK(acc->used_bytes() == 0);
    acc->shutdown();
}

CF_TEST(cuda_roundtrip_integrity) {
    std::unique_ptr<AcceleratorBackend> acc(create_accelerator_backend(1ull * 1024 * 1024 * 1024));
    CF_REQUIRE(acc && acc->available());
    acc->set_device(0);

    const uint64_t size = 8ull * 1024 * 1024;
    auto content = generate_content(777, 42, size);
    IntegrityId expected = compute_integrity(content.data(), content.size());

    uint8_t* device = nullptr;
    acc->allocate(device, size);
    CF_REQUIRE(device != nullptr);
    acc->copy_h2d(device, content.data(), size);
    acc->synchronize();
    CF_CHECK_NOTHROW(acc->verify(device, size, expected.crc32c, expected.sha));

    std::vector<uint8_t> back(size);
    acc->copy_d2h(back.data(), device, size);
    acc->synchronize();
    CF_CHECK(back == content);

    // Capacity accounting.
    CF_CHECK(acc->used_bytes() >= size);
    CF_CHECK(acc->high_water_bytes() >= size);

    acc->free(device);
    acc->shutdown();
}

CF_TEST(cuda_capacity_bounded) {
    std::unique_ptr<AcceleratorBackend> acc(create_accelerator_backend(1ull * 1024 * 1024 * 1024));
    CF_REQUIRE(acc && acc->available());
    uint8_t* device = nullptr;
    CF_CHECK_THROWS(acc->allocate(device, acc->capacity_bytes() + 1), ErrKind::Capacity);
    acc->shutdown();
}

CF_TEST(cuda_multiple_allocations_accounted_and_owned) {
    constexpr uint64_t capacity = 32ull * 1024 * 1024;
    std::unique_ptr<AcceleratorBackend> acc(create_accelerator_backend(capacity));
    CF_REQUIRE(acc && acc->available());
    CF_CHECK(acc->capacity_bytes() > 0);
    CF_CHECK(acc->capacity_bytes() <= capacity);

    uint8_t* first = nullptr;
    uint8_t* second = nullptr;
    acc->allocate(first, 1024 * 1024);
    acc->allocate(second, 2 * 1024 * 1024);
    CF_CHECK(acc->used_bytes() == 3 * 1024 * 1024);
    acc->free(first);
    CF_CHECK(acc->used_bytes() == 2 * 1024 * 1024);
    CF_CHECK_THROWS(acc->free(first), ErrKind::InvalidArgument);
    acc->free(second);
    CF_CHECK(acc->used_bytes() == 0);
    acc->shutdown();
}

CF_TEST(cuda_rejects_unowned_ranges_and_null_host_buffers) {
    std::unique_ptr<AcceleratorBackend> acc(create_accelerator_backend(32ull * 1024 * 1024));
    CF_REQUIRE(acc && acc->available());
    uint8_t* device = nullptr;
    acc->allocate(device, 1024);
    std::vector<uint8_t> host(1024, 7);

    CF_CHECK_THROWS(acc->copy_h2d(device, nullptr, 1), ErrKind::InvalidArgument);
    CF_CHECK_THROWS(acc->copy_d2h(nullptr, device, 1), ErrKind::InvalidArgument);
    CF_CHECK_THROWS(acc->copy_h2d(device, host.data(), 1025), ErrKind::InvalidArgument);
    CF_CHECK_THROWS(acc->copy_d2h(host.data(), device + 1, 1), ErrKind::InvalidArgument);

    acc->free(device);
    acc->shutdown();
}

CF_TEST(cuda_backend_serializes_concurrent_ownership_updates) {
    constexpr size_t thread_count = 8;
    constexpr uint64_t allocation_size = 1024 * 1024;
    std::unique_ptr<AcceleratorBackend> acc(create_accelerator_backend(64ull * 1024 * 1024));
    CF_REQUIRE(acc && acc->available());
    std::vector<uint8_t*> pointers(thread_count, nullptr);
    std::atomic<uint64_t> failures{0};
    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_count; ++i) {
        threads.emplace_back([&, i] {
            try {
                acc->allocate(pointers[i], allocation_size);
            } catch (...) {
                failures.fetch_add(1);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    CF_REQUIRE(failures.load() == 0);
    CF_CHECK(acc->used_bytes() == thread_count * allocation_size);

    threads.clear();
    for (size_t i = 0; i < thread_count; ++i) {
        threads.emplace_back([&, i] {
            try {
                acc->free(pointers[i]);
            } catch (...) {
                failures.fetch_add(1);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    CF_CHECK(failures.load() == 0);
    CF_CHECK(acc->used_bytes() == 0);
    acc->shutdown();
}

CF_TEST(cuda_shutdown_reclaims_outstanding_allocations) {
    std::unique_ptr<AcceleratorBackend> acc(create_accelerator_backend(32ull * 1024 * 1024));
    CF_REQUIRE(acc && acc->available());
    uint8_t* device = nullptr;
    acc->allocate(device, 1024 * 1024);
    CF_CHECK(acc->used_bytes() == 1024 * 1024);
    acc->shutdown();
    CF_CHECK(acc->used_bytes() == 0);
    CF_CHECK(!acc->available());
    CF_CHECK_NOTHROW(acc->shutdown());
}

CF_TEST(cuda_node_migrate_to_accelerator) {
    CudaCoord tc;
    CudaNode node_a("cuda-node-a", tc.port(), true);
    CudaNode node_b("cuda-node-b", tc.port(), false);
    CF_REQUIRE(cwait([&] { return tc.coord.online_node_count() == 2; }, 10000));

    ContextFabric cf;
    CF_REQUIRE(cf.connect("127.0.0.1", tc.port(), 5000));
    uint64_t node_a_id = NodeId::deterministic("cuda-node-a").value();

    auto md = cudamd(4ull * 1024 * 1024);
    auto content = generate_content(555, 7, 4ull * 1024 * 1024);
    ObjectId id = cf.create_object(md, node_a_id, content);
    CF_REQUIRE(id.valid());

    // The CUDA-capable node must report accelerator capability.
    CF_CHECK(node_a.node.accelerator() != nullptr);

    // Migrate host -> accelerator on the same CUDA node.
    auto loc = cf.migrate(id, node_a_id, StorageDomain::AcceleratorLocal, false);
    CF_CHECK(loc.node_id == node_a_id);
    CF_CHECK(loc.domain == StorageDomain::AcceleratorLocal);

    // Verify the accelerator-resident copy.
    auto vr = cf.verify(id, node_a_id, true);
    CF_CHECK(vr.ok);
    CF_CHECK(vr.bytes == content.size());

    // Move back to host memory.
    auto back = cf.migrate(id, node_a_id, StorageDomain::HostMemory, false);
    CF_CHECK(back.domain == StorageDomain::HostMemory);
    auto vr2 = cf.verify(id, node_a_id, true);
    CF_CHECK(vr2.ok);
}

#endif  // CONTEXT_FABRIC_HAS_CUDA
