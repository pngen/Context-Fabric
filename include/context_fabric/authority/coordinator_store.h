#pragma once

#include <cstdint>
#include <string>

#include "context_fabric/authority/coordinator_state.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Durable coordinator metadata store.
//
// Persists the committed coordinator snapshot atomically (staging file +
// rename) so that a crash at any point leaves either the previous committed
// snapshot or the new one — never a torn intermediate state. This is what
// makes restart recovery deterministic: whatever is durable is the truth, and
// any in-flight operation that was not committed is simply absent.
// ---------------------------------------------------------------------------
class CoordinatorStore {
public:
    explicit CoordinatorStore(std::string path);

    // True if a snapshot already exists on disk.
    [[nodiscard]] bool exists() const;

    // Load the snapshot. Throws CfError(Corrupt) if the file is invalid.
    CoordinatorSnapshot load();

    // Atomically replace the on-disk snapshot with `s`.
    void save(const CoordinatorSnapshot& s);

    [[nodiscard]] const std::string& path() const noexcept { return path_; }

private:
    std::string path_;
};

}  // namespace ctf