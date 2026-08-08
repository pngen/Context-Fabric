#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

#include "context_fabric/core/types.h"

namespace ctf {

// ---------------------------------------------------------------------------
// ContextMetadata: immutable compatibility + policy description of a
// computational-state object. Compatibility descriptors are framework-neutral:
// nothing in the core depends on a specific model runtime.
// ---------------------------------------------------------------------------
struct ContextMetadata {
    SemanticClass semantic_class = SemanticClass::Generic;

    // Namespace isolates privacy and application domains. Private and
    // NamespaceShared objects are only reusable within this namespace.
    std::string namespace_id;

    // Compatibility descriptors. Two objects may share/reuse state only when
    // every compatibility descriptor matches exactly.
    std::string producer;            // producer/runtime identifier
    std::string model_id;            // model identifier
    std::string model_revision;      // model revision/hash
    std::string tokenizer_id;        // tokenizer identity where relevant
    std::string attention_layout;    // attention/layout identity where relevant
    std::string tensor_shape;        // tensor shape where relevant
    std::string tensor_dtype;        // tensor dtype where relevant
    std::string quantization;        // quantization/compression representation
    std::string architecture_namespace;  // app/architecture namespace

    // User-defined compatibility tags; all must match for reuse.
    std::vector<std::string> tags;

    // Logical size (units are class-specific) and physical byte size.
    uint64_t logical_size = 0;
    uint64_t byte_size = 0;

    SharingPolicy sharing = SharingPolicy::Private;
    PersistencePolicy persistence = PersistencePolicy::Volatile;

    bool mutable_flag = false;   // if false, content is immutable per generation

    // Expiration lifetime in seconds from creation; nullopt = no expiry.
    std::optional<uint64_t> lifetime_seconds;

    // Overload hints / planning inputs (advisory, not authoritative).
    double recompute_seconds = 0.0;        // estimated recomputation time
    double recompute_resource_cost = 0.0;  // estimated recompute resource cost
    double reuse_probability = 0.0;        // expected future-use probability
    bool pinned = false;                   // pinned/required locality hint

    // Precomputed compatibility fingerprint (see compatibility.h).
    uint64_t compatibility_fingerprint = 0;

    [[nodiscard]] bool compatible_with(const ContextMetadata& other) const noexcept;
    [[nodiscard]] uint64_t compute_compatibility_fingerprint() const;

    // Serialization (deterministic binary form used on the wire and on disk).
    void serialize(std::vector<uint8_t>& out) const;
    // Returns consumed byte count; throws CfError(Protocol) on malformed input.
    size_t deserialize(const uint8_t* data, size_t len);
};

}  // namespace ctf
