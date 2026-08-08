#pragma once

#include <cstdint>
#include <string>

namespace ctf {

// Project version; kept in one place so the CLI and telemetry agree.
inline constexpr uint32_t kVersionMajor = 1;
inline constexpr uint32_t kVersionMinor = 0;
inline constexpr uint32_t kVersionPatch = 0;
inline constexpr const char* kVersionString = "1.0.0";

// Network protocol version. Bumped whenever the wire format changes in a way
// that is not backward compatible. Peers negotiate this at connect time and
// reject mismatches.
inline constexpr uint32_t kProtocolVersion = 1;

// Persistent object record format version. Bumped when the on-disk format
// changes. Readers reject unknown versions.
inline constexpr uint32_t kPersistFormatVersion = 1;

// Coordinator durable metadata format version.
inline constexpr uint32_t kCoordinatorFormatVersion = 1;

// Telemetry JSON Lines schema version.
inline constexpr uint32_t kTelemetrySchemaVersion = 1;

// Frame magic: "CFRA" little-endian.
inline constexpr uint32_t kFrameMagic = 0x41524643u;

}  // namespace ctf