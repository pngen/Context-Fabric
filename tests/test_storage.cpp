#include "test_harness.h"

#include <filesystem>
#include <fstream>
#include <vector>

#include "context_fabric/storage/flashtier_adapter.h"
#include "context_fabric/storage/host_memory_backend.h"
#include "context_fabric/storage/persistent_backend.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/core/prng.h"

using namespace ctf;

namespace fs = std::filesystem;

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
    bool corrupted = false;
    for (const auto& e : fs::directory_iterator(fs::path(root) / "objects")) {
        if (e.path().extension() == ".tmp") continue;
        if (corrupted) continue;
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