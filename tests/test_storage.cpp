#include "test_harness.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "context_fabric/storage/flashtier_adapter.h"
#include "context_fabric/storage/host_memory_backend.h"
#include "context_fabric/storage/persistent_backend.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/core/telemetry.h"
#include "context_fabric/runtime/node_runtime.h"
#include "context_fabric/runtime/object_store.h"

using namespace ctf;

namespace fs = std::filesystem;

namespace {

class TestAccelerator final : public AcceleratorBackend {
public:
    ~TestAccelerator() override { shutdown(); }

    const char* name() const noexcept override { return "test-accelerator"; }
    bool available() const noexcept override { return true; }
    uint64_t device_count() const noexcept override { return 1; }
    std::string device_name(uint64_t) const override { return "test-device"; }
    uint64_t device_total_memory(uint64_t) const override { return capacity_; }
    uint64_t device_compute_capability_major(uint64_t) const override { return 1; }
    uint64_t device_compute_capability_minor(uint64_t) const override { return 0; }
    uint64_t capacity_bytes() const noexcept override { return capacity_; }
    uint64_t used_bytes() const noexcept override { return used_; }
    uint64_t high_water_bytes() const noexcept override { return high_water_; }
    void set_device(uint64_t index) override {
        if (index != 0) throw_error(ErrKind::InvalidArgument, "invalid test device");
    }
    void allocate(uint8_t*& ptr, uint64_t size) override {
        if (size > capacity_ - used_) throw_error(ErrKind::Capacity, "test accelerator full");
        ptr = new uint8_t[static_cast<size_t>(size)];
        allocations_.emplace(ptr, size);
        used_ += size;
        high_water_ = (std::max)(high_water_, used_);
    }
    void free(uint8_t* ptr) override {
        auto it = allocations_.find(ptr);
        if (it == allocations_.end()) throw_error(ErrKind::State, "unknown test allocation");
        used_ -= it->second;
        delete[] ptr;
        allocations_.erase(it);
    }
    void copy_h2d(uint8_t* dst, const uint8_t* src, uint64_t size) override {
        std::memcpy(dst, src, static_cast<size_t>(size));
    }
    void copy_d2h(uint8_t* dst, const uint8_t* src, uint64_t size) override {
        std::memcpy(dst, src, static_cast<size_t>(size));
    }
    void verify(const uint8_t* device, uint64_t size, uint64_t crc, uint64_t sha) override {
        const IntegrityId actual = compute_integrity(device, static_cast<size_t>(size));
        if (actual.crc32c != crc || actual.sha != sha) {
            throw_error(ErrKind::Integrity, "test accelerator checksum mismatch");
        }
    }
    void synchronize() override {}
    void shutdown() override {
        for (auto& allocation : allocations_) delete[] allocation.first;
        allocations_.clear();
        used_ = 0;
    }

private:
    uint64_t capacity_ = 1024 * 1024;
    uint64_t used_ = 0;
    uint64_t high_water_ = 0;
    std::unordered_map<uint8_t*, uint64_t> allocations_;
};

std::string temp_store(const char* label) {
    return (fs::temp_directory_path() /
            (std::string("ctf-test-") + label + "-" + ObjectId::random().to_hex())).string();
}

}  // namespace

CF_TEST(flashtier_adapter_absent_by_default) {
    // Context Fabric builds and runs without FlashTier; the factory returns
    // nullptr and the node keeps using its own backends.
    FlashTierResidency* adapter = create_flashtier_adapter();
    CF_CHECK(adapter == nullptr);
}

CF_TEST(host_memory_capacity_enforced) {
    HostMemoryBackend host(1024);
    ObjectId id1 = ObjectId::deterministic("ns", "a");
    ObjectId id2 = ObjectId::deterministic("ns", "b");
    auto data = generate_content(1, 1, 512);
    host.put(id1, Generation::first(), data.data(), data.size());
    CF_CHECK(host.used_bytes() == 512);
    CF_CHECK(host.contains(id1, Generation::first()));
    auto data2 = generate_content(2, 2, 512);
    host.put(id2, Generation::first(), data2.data(), data2.size());
    CF_CHECK(host.used_bytes() == 1024);
    auto data3 = generate_content(3, 3, 128);
    CF_CHECK_THROWS(host.put(ObjectId::deterministic("ns", "c"), Generation::first(), data3.data(),
                             data3.size()),
                    ErrKind::Capacity);
    CF_CHECK(host.high_water_bytes() == 1024);
    auto got = host.get(id1, Generation::first());
    CF_CHECK(got == data);
    host.remove(id1, Generation::first());
    CF_CHECK(!host.contains(id1, Generation::first()));
    CF_CHECK(host.used_bytes() == 512);
}

CF_TEST(host_memory_verify_and_missing) {
    HostMemoryBackend host(4096);
    ObjectId id = ObjectId::deterministic("ns", "v");
    auto data = generate_content(5, 5, 2048);
    host.put(id, Generation::first(), data.data(), data.size());
    IntegrityId idg = compute_integrity(data.data(), data.size());
    CF_CHECK_NOTHROW(host.verify(id, Generation::first(), idg.crc32c, idg.sha));
    CF_CHECK_THROWS(host.verify(id, Generation::first(), idg.crc32c + 1, idg.sha), ErrKind::Integrity);
    CF_CHECK_THROWS(host.get(ObjectId::deterministic("ns", "missing"), Generation::first()),
                    ErrKind::NotFound);
}

CF_TEST(host_memory_replacement_is_transactional) {
    HostMemoryBackend host(16);
    const ObjectId id = ObjectId::deterministic("ns", "replace");
    const Generation gen = Generation::first();
    const auto original = generate_content(1, 1, 8);
    const auto too_large = generate_content(2, 2, 17);
    host.put(id, gen, original.data(), original.size());
    CF_CHECK_THROWS(host.put(id, gen, too_large.data(), too_large.size()), ErrKind::Capacity);
    CF_CHECK(host.get(id, gen) == original);
    CF_CHECK(host.used_bytes() == original.size());
    CF_CHECK_THROWS(host.put(ObjectId(), gen, original.data(), original.size()), ErrKind::InvalidArgument);
    CF_CHECK_THROWS(host.put(id, gen, nullptr, 1), ErrKind::InvalidArgument);
}

CF_TEST(persistent_roundtrip_verify) {
    std::string root = (fs::temp_directory_path() / ("ctf-test-persist-" + ObjectId::random().to_hex())).string();
    LocalPersistentBackend store(root, 1024 * 1024);
    CF_CHECK(store.available());
    ObjectId id = ObjectId::deterministic("ns", "p");
    auto data = generate_content(10, 11, 100000);
    store.put(id, Generation::first(), data.data(), data.size());
    CF_CHECK(store.contains(id, Generation::first()));
    auto got = store.get(id, Generation::first());
    CF_CHECK(got == data);
    IntegrityId idg = compute_integrity(data.data(), data.size());
    CF_CHECK_NOTHROW(store.verify(id, Generation::first(), idg.crc32c, idg.sha));
    fs::remove_all(root);
}

CF_TEST(persistent_metadata_embedded) {
    std::string root = (fs::temp_directory_path() / ("ctf-test-pmeta-" + ObjectId::random().to_hex())).string();
    LocalPersistentBackend store(root, 1024 * 1024);
    ContextMetadata md;
    md.namespace_id = "ns";
    md.model_id = "m";
    md.model_revision = "r9";
    ObjectId id = ObjectId::deterministic("ns", "pmeta");
    auto data = generate_content(3, 4, 4096);
    store.put_with_metadata(id, Generation::first(), md, data.data(), data.size());
    auto got = store.get(id, Generation::first());
    CF_CHECK(got == data);
    fs::remove_all(root);
}

CF_TEST(persistent_capacity_enforced) {
    std::string root = (fs::temp_directory_path() / ("ctf-test-pcap-" + ObjectId::random().to_hex())).string();
    LocalPersistentBackend store(root, 1000);
    ObjectId id = ObjectId::deterministic("ns", "big");
    auto data = generate_content(1, 1, 2000);
    CF_CHECK_THROWS(store.put(id, Generation::first(), data.data(), data.size()), ErrKind::Capacity);
    fs::remove_all(root);
}

CF_TEST(persistent_replacement_and_external_delete_accounting) {
    const std::string root = temp_store("persistent-accounting");
    const ObjectId id = ObjectId::deterministic("ns", "accounted");
    const Generation gen = Generation::first();
    const auto first = generate_content(4, 5, 128);
    const auto replacement = generate_content(6, 7, 256);
    LocalPersistentBackend store(root, 1024 * 1024);
    store.put(id, gen, first.data(), first.size());
    const uint64_t first_accounted = store.used_bytes();
    CF_CHECK(first_accounted > first.size());
    store.put(id, gen, replacement.data(), replacement.size());
    CF_CHECK(store.get(id, gen) == replacement);
    CF_CHECK(store.used_bytes() == first_accounted + replacement.size() - first.size());

    for (const auto& entry : fs::directory_iterator(fs::path(root) / "objects")) {
        if (entry.path().extension() == ".cfobj") fs::remove(entry.path());
    }
    store.remove(id, gen);
    CF_CHECK(store.used_bytes() == 0);
    CF_CHECK(!store.contains(id, gen));
    fs::remove_all(root);
}

CF_TEST(object_store_tracks_coherent_domains_and_releases_accelerator) {
    const std::string root = temp_store("object-store-domains");
    HostMemoryBackend host(1024 * 1024);
    LocalPersistentBackend persistent(root, 1024 * 1024);
    TestAccelerator accelerator;
    NodeObjectStore store;
    store.set_host(&host);
    store.set_persistent(&persistent);
    store.set_accelerator(&accelerator);

    const ObjectId id = ObjectId::deterministic("ns", "multi-domain");
    const Generation gen = Generation::first();
    const auto bytes = generate_content(10, 11, 256);
    const IntegrityId integrity = compute_integrity(bytes.data(), bytes.size());
    ContextMetadata metadata;
    metadata.byte_size = bytes.size();

    store.store(id, gen, CopyRole::Authoritative, StorageDomain::HostMemory,
                &metadata, bytes.data(), bytes.size());
    store.store(id, gen, CopyRole::Persistent, StorageDomain::LocalPersistent,
                &metadata, bytes.data(), bytes.size());
    uint8_t* first_device = nullptr;
    accelerator.allocate(first_device, bytes.size());
    accelerator.copy_h2d(first_device, bytes.data(), bytes.size());
    store.retain_accelerator(id, gen, CopyRole::Replica, first_device, bytes.size(), integrity);
    CF_CHECK(store.host_used() == bytes.size());
    CF_CHECK(store.persistent_used() > bytes.size());
    CF_CHECK(store.accelerator_used() == bytes.size());

    uint8_t* replacement_device = nullptr;
    accelerator.allocate(replacement_device, bytes.size());
    accelerator.copy_h2d(replacement_device, bytes.data(), bytes.size());
    store.retain_accelerator(id, gen, CopyRole::Replica, replacement_device, bytes.size(), integrity);
    CF_CHECK(store.accelerator_used() == bytes.size());
    CF_CHECK(store.read(id, gen) == bytes);

    const auto divergent = generate_content(12, 14, bytes.size());
    CF_REQUIRE(divergent != bytes);
    CF_CHECK_THROWS(store.store(id, gen, CopyRole::Persistent, StorageDomain::LocalPersistent,
                                &metadata, divergent.data(), divergent.size()),
                    ErrKind::Integrity);
    store.remove(id, gen);
    CF_CHECK(store.host_used() == 0);
    CF_CHECK(store.persistent_used() == 0);
    CF_CHECK(store.accelerator_used() == 0);
    CF_CHECK(!store.has_copy(id, gen));
    fs::remove_all(root);
}

CF_TEST(node_runtime_rejects_unserviceable_configs_and_restarts_cleanly) {
    Telemetry telemetry;
    NodeConfig invalid;
    invalid.node_id = NodeId::deterministic("invalid-runtime");
    invalid.coord_port = 1;
    invalid.host_capacity = 1024;
    invalid.transfer_workers = 0;
    NodeRuntime invalid_runtime(invalid, telemetry);
    CF_CHECK_THROWS(invalid_runtime.start(), ErrKind::InvalidArgument);

    NodeConfig config;
    config.node_id = NodeId::deterministic("restart-runtime");
    config.coord_port = 1;
    config.host_capacity = 1024 * 1024;
    config.heartbeat_interval_ms = 60000;
    config.op_timeout_ms = 5000;
    config.transfer_workers = 1;
    config.job_queue_capacity = 1;
    NodeRuntime runtime(config, telemetry);
    for (int iteration = 0; iteration < 5; ++iteration) {
        const auto started = std::chrono::steady_clock::now();
        CF_REQUIRE(runtime.start());
        runtime.shutdown();
        const auto elapsed = std::chrono::steady_clock::now() - started;
        CF_CHECK(elapsed < std::chrono::seconds(2));
        CF_CHECK(runtime.data_port() == 0);
    }
}

CF_TEST(node_runtime_shutdown_interrupts_a_pending_coordinator_connect) {
    Telemetry telemetry;
    telemetry.set_stderr_enabled(false);
    NodeConfig config;
    config.node_id = NodeId::deterministic("pending-connect-runtime");
    config.coord_port = 1;  // nothing listens here
    config.host_capacity = 1024 * 1024;
    config.transfer_workers = 1;
    NodeRuntime runtime(config, telemetry);
    CF_REQUIRE(runtime.start());
    // Let the control thread get into its registration attempt. Where a
    // refused connect is only reported at the connect timeout, that thread is
    // now blocked for seconds while holding the control-socket mutex.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    const auto begin = std::chrono::steady_clock::now();
    runtime.shutdown();
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    CF_CHECK(elapsed < std::chrono::seconds(1));
    CF_CHECK(runtime.data_port() == 0);
}

CF_TEST(node_runtime_shutdown_wakes_workers_that_are_entering_their_wait) {
    // A peer that accepts and immediately hangs up makes every registration
    // attempt fail fast, so each start/shutdown cycle is cheap. What remains
    // under test is request_shutdown() racing a worker thread that has just
    // evaluated its queue predicate: a notification sent without passing
    // through the queue mutex is lost, the worker blocks forever, and
    // shutdown() never returns from joining it.
    Socket refusing_coordinator;
    uint16_t port = 0;
    CF_REQUIRE(refusing_coordinator.listen("127.0.0.1", 0, port));
    std::atomic<bool> accepting{true};
    std::thread acceptor([&] {
        while (accepting.load()) {
            Socket peer = refusing_coordinator.accept(50);
            peer.close();
        }
    });

    // The data-plane accept loop polls its listener in 200 ms slices that a
    // listener shutdown does not interrupt on every platform. One connection
    // per cycle, made while shutdown() runs, releases that poll so the cycles
    // stay short. It never touches the condition variables under test: a
    // connection accepted after shutdown began is dropped without a notify.
    std::atomic<uint16_t> poke_port{0};
    std::thread poker([&] {
        while (accepting.load()) {
            const uint16_t target = poke_port.exchange(0);
            if (target == 0) {
                std::this_thread::yield();
                continue;
            }
            Socket poke;
            (void)poke.connect("127.0.0.1", target, 20);
        }
    });

    Telemetry telemetry;
    telemetry.set_stderr_enabled(false);
    NodeConfig config;
    config.node_id = NodeId::deterministic("shutdown-wakeup-runtime");
    config.coord_port = port;
    config.host_capacity = 1024 * 1024;
    config.transfer_workers = 1;
    NodeRuntime runtime(config, telemetry);
    constexpr int kCycles = 1000;
    int completed = 0;
    for (int cycle = 0; cycle < kCycles; ++cycle) {
        if (!runtime.start()) break;
        // Sweep a short gap between start() and shutdown() so that some
        // cycles land on the instant the freshly started worker enters its
        // wait.
        const auto gap_end =
            std::chrono::steady_clock::now() + std::chrono::microseconds((cycle % 60) * 5);
        while (std::chrono::steady_clock::now() < gap_end) {
        }
        poke_port.store(runtime.data_port());
        runtime.shutdown();
        ++completed;
    }
    accepting.store(false);
    acceptor.join();
    poker.join();
    CF_CHECK(completed == kCycles);
    CF_CHECK(runtime.data_port() == 0);
}

CF_TEST(persistent_corruption_detected) {
    std::string root = (fs::temp_directory_path() / ("ctf-test-pcor-" + ObjectId::random().to_hex())).string();
    {
        LocalPersistentBackend store(root, 1024 * 1024);
        ObjectId id = ObjectId::deterministic("ns", "corrupt");
        auto data = generate_content(2, 2, 8192);
        store.put(id, Generation::first(), data.data(), data.size());
    }
    std::string file;
    for (const auto& e : fs::directory_iterator(fs::path(root) / "objects")) {
        file = e.path().string();
    }
    CF_REQUIRE(!file.empty());
    {
        std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(-100, std::ios::end);
        char c = 'X';
        f.write(&c, 1);
    }
    {
        LocalPersistentBackend store(root, 1024 * 1024);
        ObjectId id = ObjectId::deterministic("ns", "corrupt");
        CF_CHECK_THROWS(store.get(id, Generation::first()), ErrKind::Corrupt);
    }
    fs::remove_all(root);
}

CF_TEST(persistent_truncation_detected) {
    std::string root = (fs::temp_directory_path() / ("ctf-test-ptrunc-" + ObjectId::random().to_hex())).string();
    {
        LocalPersistentBackend store(root, 1024 * 1024);
        ObjectId id = ObjectId::deterministic("ns", "trunc");
        auto data = generate_content(4, 4, 16384);
        store.put(id, Generation::first(), data.data(), data.size());
    }
    std::string file;
    for (const auto& e : fs::directory_iterator(fs::path(root) / "objects")) {
        file = e.path().string();
    }
    std::error_code ec;
    fs::resize_file(file, 8000, ec);
    CF_REQUIRE(!ec);
    {
        LocalPersistentBackend store(root, 1024 * 1024);
        ObjectId id = ObjectId::deterministic("ns", "trunc");
        CF_CHECK_THROWS(store.get(id, Generation::first()), ErrKind::Corrupt);
    }
    fs::remove_all(root);
}

CF_TEST(persistent_recover_quarantine_tmp) {
    std::string root;
    try {
    root = (fs::temp_directory_path() / ("ctf-test-prec-" + ObjectId::random().to_hex())).string();
    {
        LocalPersistentBackend store(root, 1024 * 1024);
        ObjectId id1 = ObjectId::deterministic("ns", "good");
        ObjectId id2 = ObjectId::deterministic("ns", "bad");
        auto d1 = generate_content(1, 1, 2048);
        auto d2 = generate_content(2, 2, 2048);
        store.put(id1, Generation::first(), d1.data(), d1.size());
        store.put(id2, Generation::first(), d2.data(), d2.size());
        std::ofstream tmp((fs::path(root) / "objects" / "stale.tmp"));
        tmp << "junk";
        tmp.close();
    }
    // Corrupt exactly one of the two object files.
    const std::string bad_prefix = ObjectId::deterministic("ns", "bad").to_hex();
    bool corrupted = false;
    for (const auto& e : fs::directory_iterator(fs::path(root) / "objects")) {
        if (e.path().extension() == ".tmp") continue;
        if (corrupted || e.path().filename().string().rfind(bad_prefix, 0) != 0) continue;
        std::ifstream in(e.path(), std::ios::binary);
        std::vector<char> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        if (bytes.size() > 100) {
            bytes[bytes.size() / 2] = static_cast<char>(bytes[bytes.size() / 2] ^ 0xFF);
            std::ofstream out(e.path(), std::ios::binary | std::ios::trunc);
            out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
            corrupted = true;
        }
    }
    CF_REQUIRE(corrupted);
    {
        LocalPersistentBackend store(root, 1024 * 1024);
        auto recovered = store.recover();
        // Exactly one object survives intact; corruption is quarantined.
        CF_CHECK(recovered.size() == 1);
        CF_CHECK(store.contains(ObjectId::deterministic("ns", "good"), Generation::first()));
        CF_CHECK(!store.contains(ObjectId::deterministic("ns", "bad"), Generation::first()));
        bool has_tmp = false;
        for (const auto& e : fs::directory_iterator(fs::path(root) / "objects")) {
            if (e.path().extension() == ".tmp") has_tmp = true;
        }
        CF_CHECK(!has_tmp);
    }
    fs::remove_all(root);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[t] EXCEPTION: %s\n", e.what());
        std::fflush(stderr);
        std::error_code ec;
        fs::remove_all(root, ec);
        throw;
    }
}
