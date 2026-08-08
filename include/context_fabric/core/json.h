#pragma once

#include <cstdint>
#include <string>
#include <sstream>
#include <vector>

namespace ctf {

// ---------------------------------------------------------------------------
// Minimal deterministic JSON writer used for telemetry (JSON Lines) and
// machine-readable CLI output. Not a general-purpose JSON library.
// ---------------------------------------------------------------------------
class JsonObject {
public:
    JsonObject() = default;

    JsonObject& field(const char* name, const char* value);
    JsonObject& field(const char* name, const std::string& value);
    JsonObject& field(const char* name, bool value);
    JsonObject& field(const char* name, int64_t value);
    JsonObject& field(const char* name, uint64_t value);
    JsonObject& field(const char* name, double value, int precision = 6);
    JsonObject& field_u64(const char* name, uint64_t value) { return field(name, value); }

    // Writes a complete JSON object to `out` (compact, deterministic key order
    // matching insertion order).
    void write_to(std::string& out) const;

private:
    struct Entry {
        std::string key;
        std::string value_json;
    };
    std::vector<Entry> entries_;
};

// Returns `s` wrapped in double quotes with JSON escaping applied.
[[nodiscard]] std::string json_escape_string(const std::string& s);

}  // namespace ctf
