#pragma once

#include <cstdint>
#include <string>
#include <mutex>
#include <memory>
#include <fstream>

#include "context_fabric/core/json.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Telemetry.
//
// All telemetry is LOCAL: emitted to a per-node JSON Lines file (and mirrored
// to stderr when enabled). No telemetry ever leaves the node. The schema is
// versioned (kTelemetrySchemaVersion) and documented in schemas/.
//
// Every event gets a monotonic sequence, wall-clock timestamp, node id and
// coordinator epoch automatically. The class is a single-writer log: callers
// may emit from multiple threads; emission is serialized by a mutex.
// ---------------------------------------------------------------------------
class Telemetry {
public:
    Telemetry() = default;
    ~Telemetry();

    Telemetry(const Telemetry&) = delete;
    Telemetry& operator=(const Telemetry&) = delete;

    // If `path` is empty, events go to stderr only.
    void open(const std::string& path);
    void close();
    [[nodiscard]] bool is_open() const noexcept { return opened_; }

    void set_node_id(uint64_t node_id) noexcept { node_id_ = node_id; }
    void set_coordinator_epoch(uint64_t epoch) noexcept { epoch_ = epoch; }
    void set_stderr_enabled(bool on) noexcept { stderr_enabled_ = on; }

    // Emit an event. `event` is the event type; doc() is a JSON object whose
    // fields are appended to the standard envelope.
    void emit(const std::string& event, const JsonObject& doc);

    // Convenience: emit with a single message field.
    void emit_msg(const std::string& event, const std::string& message);

private:
    std::mutex mutex_;
    std::string path_;
    std::ofstream file_;
    bool opened_ = false;
    bool stderr_enabled_ = true;
    uint64_t node_id_ = 0;
    uint64_t epoch_ = 0;
    uint64_t sequence_ = 0;
};

}  // namespace ctf