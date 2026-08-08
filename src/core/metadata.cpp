#include "context_fabric/core/metadata.h"

#include <algorithm>
#include <cmath>
#include <utility>

#include "context_fabric/core/object_id.h"
#include "context_fabric/core/serializer.h"
#include "context_fabric/core/error.h"

namespace ctf {

uint64_t ContextMetadata::compute_compatibility_fingerprint() const {
    Writer w;
    w.u32(static_cast<uint32_t>(semantic_class));
    w.str(namespace_id);
    w.str(producer);
    w.str(model_id);
    w.str(model_revision);
    w.str(tokenizer_id);
    w.str(attention_layout);
    w.str(tensor_shape);
    w.str(tensor_dtype);
    w.str(quantization);
    w.str(architecture_namespace);
    w.u32(static_cast<uint32_t>(sharing));
    w.str_vector(tags);
    return stable_hash64(w.data().data(), w.data().size());
}

bool ContextMetadata::compatible_with(const ContextMetadata& other) const noexcept {
    // Strict equality on every compatibility descriptor. Two objects that
    // differ in required compatibility metadata must never be silently reused.
    // The cached fingerprint is not an authority boundary: callers can supply
    // metadata, so matching cached values alone cannot establish compatibility.
    return semantic_class == other.semantic_class &&
           namespace_id == other.namespace_id &&
           producer == other.producer &&
           model_id == other.model_id &&
           model_revision == other.model_revision &&
           tokenizer_id == other.tokenizer_id &&
           attention_layout == other.attention_layout &&
           tensor_shape == other.tensor_shape &&
           tensor_dtype == other.tensor_dtype &&
           quantization == other.quantization &&
           architecture_namespace == other.architecture_namespace &&
           sharing == other.sharing &&
           tags == other.tags;
}

void ContextMetadata::serialize(std::vector<uint8_t>& out) const {
    Writer w;
    w.u32(static_cast<uint32_t>(semantic_class));
    w.str(namespace_id);
    w.str(producer);
    w.str(model_id);
    w.str(model_revision);
    w.str(tokenizer_id);
    w.str(attention_layout);
    w.str(tensor_shape);
    w.str(tensor_dtype);
    w.str(quantization);
    w.str(architecture_namespace);
    w.str_vector(tags);
    w.u64(logical_size);
    w.u64(byte_size);
    w.u32(static_cast<uint32_t>(sharing));
    w.u32(static_cast<uint32_t>(persistence));
    w.u8(mutable_flag ? 1 : 0);
    w.u8(lifetime_seconds.has_value() ? 1 : 0);
    if (lifetime_seconds.has_value()) w.u64(*lifetime_seconds);
    w.f64(recompute_seconds);
    w.f64(recompute_resource_cost);
    w.f64(reuse_probability);
    w.u8(pinned ? 1 : 0);
    w.u64(compatibility_fingerprint);
    out = w.data();
}

size_t ContextMetadata::deserialize(const uint8_t* data, size_t len) {
    Reader r(data, len);
    ContextMetadata parsed;

    const uint32_t semantic = r.u32();
    if (semantic > static_cast<uint32_t>(SemanticClass::ApplicationDefined)) {
        throw_error(ErrKind::Protocol, "invalid semantic class");
    }
    parsed.semantic_class = static_cast<SemanticClass>(semantic);
    parsed.namespace_id = r.str();
    parsed.producer = r.str();
    parsed.model_id = r.str();
    parsed.model_revision = r.str();
    parsed.tokenizer_id = r.str();
    parsed.attention_layout = r.str();
    parsed.tensor_shape = r.str();
    parsed.tensor_dtype = r.str();
    parsed.quantization = r.str();
    parsed.architecture_namespace = r.str();
    parsed.tags = r.str_vector_strict();
    parsed.logical_size = r.u64();
    parsed.byte_size = r.u64();

    const uint32_t sharing_value = r.u32();
    if (sharing_value > static_cast<uint32_t>(SharingPolicy::GloballyShareable)) {
        throw_error(ErrKind::Protocol, "invalid sharing policy");
    }
    parsed.sharing = static_cast<SharingPolicy>(sharing_value);

    const uint32_t persistence_value = r.u32();
    if (persistence_value > static_cast<uint32_t>(PersistencePolicy::Persistent)) {
        throw_error(ErrKind::Protocol, "invalid persistence policy");
    }
    parsed.persistence = static_cast<PersistencePolicy>(persistence_value);

    const uint8_t mutable_value = r.u8();
    if (mutable_value > 1) throw_error(ErrKind::Protocol, "invalid mutable flag");
    parsed.mutable_flag = mutable_value != 0;

    const uint8_t has_lifetime = r.u8();
    if (has_lifetime > 1) throw_error(ErrKind::Protocol, "invalid lifetime presence flag");
    if (has_lifetime != 0) parsed.lifetime_seconds = r.u64();

    parsed.recompute_seconds = r.f64();
    parsed.recompute_resource_cost = r.f64();
    parsed.reuse_probability = r.f64();
    if (!std::isfinite(parsed.recompute_seconds) || parsed.recompute_seconds < 0.0 ||
        !std::isfinite(parsed.recompute_resource_cost) || parsed.recompute_resource_cost < 0.0 ||
        !std::isfinite(parsed.reuse_probability) || parsed.reuse_probability < 0.0 ||
        parsed.reuse_probability > 1.0) {
        throw_error(ErrKind::Protocol, "invalid metadata planning value");
    }

    const uint8_t pinned_value = r.u8();
    if (pinned_value > 1) throw_error(ErrKind::Protocol, "invalid pinned flag");
    parsed.pinned = pinned_value != 0;
    parsed.compatibility_fingerprint = r.u64();

    if (!r.at_end()) throw_error(ErrKind::Protocol, "trailing bytes after metadata");
    if (parsed.compatibility_fingerprint != 0 &&
        parsed.compatibility_fingerprint != parsed.compute_compatibility_fingerprint()) {
        throw_error(ErrKind::Protocol, "metadata compatibility fingerprint mismatch");
    }

    *this = std::move(parsed);
    return len;
}

}  // namespace ctf
