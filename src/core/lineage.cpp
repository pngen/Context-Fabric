#include "context_fabric/core/lineage.h"

#include "context_fabric/core/serializer.h"
#include "context_fabric/core/error.h"

namespace ctf {

namespace {
// Reads hi then lo in a fixed order (function argument evaluation order is
// unspecified, so reading inline through two r.u64() calls is not portable).
ObjectId read_object_id(Reader& r) {
    uint64_t hi = r.u64();
    uint64_t lo = r.u64();
    return ObjectId::from_parts(hi, lo);
}
}  // namespace

void serialize_lineage(const std::vector<LineageEntry>& lineage, std::vector<uint8_t>& out) {
    Writer w;
    w.u32(static_cast<uint32_t>(lineage.size()));
    for (const auto& e : lineage) {
        w.u64(e.object.hi());
        w.u64(e.object.lo());
        w.u64(e.generation.value());
        w.u64(e.parent.hi());
        w.u64(e.parent.lo());
        w.str(e.operation);
        w.str(e.producer);
        w.u64(e.creation_sequence);
        w.u64(e.created_unix_ms);
    }
    out = w.data();
}

std::vector<LineageEntry> deserialize_lineage(const uint8_t* data, size_t len) {
    Reader r(data, len);
    uint32_t count = r.u32();
    if (count > 100000) throw_error(ErrKind::Protocol, "lineage count unreasonable");
    std::vector<LineageEntry> v;
    v.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        LineageEntry e;
        e.object = read_object_id(r);
        e.generation = Generation::from_value(r.u64());
        e.parent = read_object_id(r);
        e.operation = r.str();
        e.producer = r.str();
        e.creation_sequence = r.u64();
        e.created_unix_ms = r.u64();
        v.push_back(std::move(e));
    }
    return v;
}

}  // namespace ctf