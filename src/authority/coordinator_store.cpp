#include "context_fabric/authority/coordinator_store.h"

#include <cstdio>
#include <filesystem>
#include <fstream>

#include "context_fabric/core/error.h"

namespace ctf {

namespace fs = std::filesystem;

CoordinatorStore::CoordinatorStore(std::string path) : path_(std::move(path)) {}

bool CoordinatorStore::exists() const {
    std::error_code ec;
    return fs::exists(path_, ec);
}

CoordinatorSnapshot CoordinatorStore::load() {
    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) throw_error(ErrKind::Persistence, "cannot open coordinator store");
    std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    return deserialize_coordinator_snapshot(bytes.data(), bytes.size());
}

void CoordinatorStore::save(const CoordinatorSnapshot& s) {
    std::vector<uint8_t> bytes;
    serialize_coordinator_snapshot(s, bytes);
    std::string tmp = path_ + ".tmp";
    try {
        {
            std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
            if (!out.is_open()) throw_error(ErrKind::Persistence, "cannot open coordinator staging file");
            out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
            out.flush();
            if (!out.good()) throw_error(ErrKind::Persistence, "coordinator snapshot write failed");
        }
        fs::rename(tmp, path_);
    } catch (const fs::filesystem_error& e) {
        throw_error(ErrKind::Persistence, std::string("coordinator commit failed: ") + e.what());
    }
}

}  // namespace ctf