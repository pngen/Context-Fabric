#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Lineage: how a reusable computational-state object came to exist. Used to
// make reusable-state evolution inspectable. Survives persistence/migration
// because it is stored with the object metadata.
// ---------------------------------------------------------------------------
struct LineageEntry {
    ObjectId object;
    Generation generation;
    ObjectId parent;              // 0 if none (root object)
    std::string operation;        // derivation operation id
    std::string producer;         // producer node label/id
    uint64_t creation_sequence;   // global creation sequence
    uint64_t created_unix_ms;     // wall clock at creation
};

// Serialization helpers (deterministic binary).
void serialize_lineage(const std::vector<LineageEntry>& lineage, std::vector<uint8_t>& out);
std::vector<LineageEntry> deserialize_lineage(const uint8_t* data, size_t len);

}  // namespace ctf