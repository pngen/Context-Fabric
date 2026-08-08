#include "context_fabric/core/metadata.h"

#include <algorithm>

#include "context_fabric/core/object_id.h"
#include "context_fabric/core/serializer.h"
#include "context_fabric/core/error.h"

namespace ctf {

uint64_t ContextMetadata::compute_compatibility_fingerprint() const noexcept {
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
    if (compatibility_fingerprint != 0 && other.compatibility_fingerprint != 0) {
        return compatibility_fingerprint == other.compatibility_fingerprint;
    }
    // Strict equality on every compatibility descriptor. Two objects that
    // differ in required compatibility metadata must never be silently reused.
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
    semantic_class = static_cast<SemanticClass>(r.u32());
    namespace_id = r.str();
    producer = r.str();
    model_id = r.str();
    model_revision = r.str();
    tokenizer_id = r.str();
    attention_layout = r.str();
    tensor_shape = r.str();
    tensor_dtype = r.str();
    quantization = r.str();
    architecture_namespace = r.str();
    tags = r.str_vector_strict();
    logical_size = r.u64();
    byte_size = r.u64();
    sharing = static_cast<SharingPolicy>(r.u32());
    persistence = static_cast<PersistencePolicy>(r.u32());
    mutable_flag = r.u8() != 0;
    if (r.u8() != 0) lifetime_seconds = r.u64();
    recompute_seconds = r.f64();
    recompute_resource_cost = r.f64();
    reuse_probability = r.f64();
    pinned = r.u8() != 0;
    compatibility_fingerprint = r.u64();
    return len - r.remaining();
}

}  // namespace ctf