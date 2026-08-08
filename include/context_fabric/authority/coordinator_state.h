#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"
#include "context_fabric/core/node.h"
#include "context_fabric/core/metadata.h"
#include "context_fabric/core/types.h"
#include "context_fabric/core/lease.h"
#include "context_fabric/core/lineage.h"
#include "context_fabric/core/integrity.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Coordinator committed state. This is the authoritative, durable record of
// the fabric. Only committed state is serialized; in-flight operations are
// never persisted so restart deterministically aborts them.
// ---------------------------------------------------------------------------

// A registered node session.
struct CoordNode {
    uint64_t node_id = 0;
    std::string label;
    NodeAddress addr;
    NodeCapabilities caps;
    uint64_t session_id = 0;
    uint64_t session_epoch = 0;
    uint64_t last_heartbeat_ms = 0;
    bool online = false;
};

// A committed object record.
struct CoordObject {
    ObjectId id;
    ContextMetadata metadata;
    Generation generation;
    uint64_t lease_generation = 0;
    LifecycleState lifecycle = LifecycleState::Creating;

    uint64_t authoritative_node = 0;
    StorageDomain authoritative_domain = StorageDomain::Unknown;
    IntegrityId integrity;  // expected checksums of authoritative generation

    std::vector<uint64_t> replicas;        // nodes holding validated read-only replicas
    std::vector<uint64_t> persistent_nodes;  // nodes holding durable backing copies

    uint64_t owner_node = 0;
    uint64_t created_unix_ms = 0;
    uint64_t expires_unix_ms = 0;  // 0 = no expiry
    std::vector<LineageEntry> lineage;

    // In-flight metadata (not persisted).
    uint64_t active_op_token = 0;      // 0 = no in-flight data operation
    std::string active_op_kind;        // "replicate" | "migrate" | "persist" | "restore" | "create"
    uint64_t active_op_target = 0;
    StorageDomain active_op_domain = StorageDomain::Unknown;
    bool active_op_keep_source = false;
    // Node that has reported success for the current transfer (1 = source's
    // report, 2 = destination's report). Used to commit only after both sides
    // confirm.
    uint8_t active_op_reports = 0;

    [[nodiscard]] bool in_flight() const noexcept { return active_op_token != 0; }
};

// ---------------------------------------------------------------------------
// Coordinator snapshot serialization (deterministic binary; versioned).
// ---------------------------------------------------------------------------
struct CoordinatorSnapshot {
    uint32_t epoch = 0;
    uint64_t seq = 0;
    std::vector<CoordNode> nodes;
    std::vector<CoordObject> objects;
};

void serialize_coordinator_snapshot(const CoordinatorSnapshot& s, std::vector<uint8_t>& out);
CoordinatorSnapshot deserialize_coordinator_snapshot(const uint8_t* data, size_t len);
void serialize_coord_object(const CoordObject& o, std::vector<uint8_t>& out);
CoordObject deserialize_coord_object(const uint8_t* data, size_t len);

}  // namespace ctf