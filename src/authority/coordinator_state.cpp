#include "context_fabric/authority/coordinator_state.h"

#include "context_fabric/core/serializer.h"
#include "context_fabric/core/error.h"
#include "context_fabric/core/version.h"

namespace ctf {

namespace {
constexpr uint32_t kCoordMagic = 0x4F464343u;  // "CCFO"

ObjectId read_object_id(Reader& r) {
    uint64_t hi = r.u64();
    uint64_t lo = r.u64();
    return ObjectId::from_parts(hi, lo);
}
}  // namespace

void serialize_coord_object(const CoordObject& o, std::vector<uint8_t>& out) {
    Writer w;
    w.u64(o.id.hi());
    w.u64(o.id.lo());
    {
        std::vector<uint8_t> md;
        o.metadata.serialize(md);
        w.bytes(md.data(), md.size());
    }
    w.u64(o.generation.value());
    w.u64(o.lease_generation);
    w.u32(static_cast<uint32_t>(o.lifecycle));
    w.u64(o.authoritative_node);
    w.u32(static_cast<uint32_t>(o.authoritative_domain));
    w.u64(o.integrity.crc32c);
    w.u64(o.integrity.sha);
    w.u64(o.integrity.bytes);
    w.u32(static_cast<uint32_t>(o.replicas.size()));
    for (uint64_t n : o.replicas) w.u64(n);
    w.u32(static_cast<uint32_t>(o.persistent_nodes.size()));
    for (uint64_t n : o.persistent_nodes) w.u64(n);
    w.u64(o.owner_node);
    w.u64(o.created_unix_ms);
    w.u64(o.expires_unix_ms);
    std::vector<uint8_t> lin;
    serialize_lineage(o.lineage, lin);
    w.bytes(lin.data(), lin.size());
    out = w.data();
}

CoordObject deserialize_coord_object(const uint8_t* data, size_t len) {
    Reader r(data, len);
    CoordObject o;
    o.id = read_object_id(r);
    std::vector<uint8_t> md = r.bytes(r.u32());
    o.metadata.deserialize(md.data(), md.size());
    o.generation = Generation::from_value(r.u64());
    o.lease_generation = r.u64();
    o.lifecycle = static_cast<LifecycleState>(r.u32());
    o.authoritative_node = r.u64();
    o.authoritative_domain = static_cast<StorageDomain>(r.u32());
    o.integrity.crc32c = r.u64();
    o.integrity.sha = r.u64();
    o.integrity.bytes = static_cast<uint32_t>(r.u64());
    uint32_t nr = r.u32();
    if (nr > 100000) throw_error(ErrKind::Corrupt, "replica list unreasonable");
    for (uint32_t i = 0; i < nr; ++i) o.replicas.push_back(r.u64());
    uint32_t np = r.u32();
    if (np > 100000) throw_error(ErrKind::Corrupt, "persistent list unreasonable");
    for (uint32_t i = 0; i < np; ++i) o.persistent_nodes.push_back(r.u64());
    o.owner_node = r.u64();
    o.created_unix_ms = r.u64();
    o.expires_unix_ms = r.u64();
    std::vector<uint8_t> lin = r.bytes(r.u32());
    o.lineage = deserialize_lineage(lin.data(), lin.size());
    return o;
}

void serialize_coordinator_snapshot(const CoordinatorSnapshot& s, std::vector<uint8_t>& out) {
    Writer w;
    w.u32(kCoordMagic);
    w.u32(kCoordinatorFormatVersion);
    w.u32(s.epoch);
    w.u64(s.seq);
    w.u32(static_cast<uint32_t>(s.nodes.size()));
    for (const auto& n : s.nodes) {
        w.u64(n.node_id);
        w.str(n.label);
        w.str(n.addr.host);
        w.u16(n.addr.port);
        w.u32(capabilities_to_bits(n.caps));
        w.u64(n.caps.max_host_bytes);
        w.u64(n.caps.max_persistent_bytes);
        w.u64(n.caps.max_accelerator_bytes);
        w.u64(n.session_id);
        w.u64(n.session_epoch);
        w.u64(n.last_heartbeat_ms);
        w.u8(n.online ? 1 : 0);
    }
    w.u32(static_cast<uint32_t>(s.objects.size()));
    for (const auto& o : s.objects) {
        std::vector<uint8_t> buf;
        serialize_coord_object(o, buf);
        w.bytes(buf.data(), buf.size());
    }
    out = w.data();
}

CoordinatorSnapshot deserialize_coordinator_snapshot(const uint8_t* data, size_t len) {
    Reader r(data, len);
    CoordinatorSnapshot s;
    uint32_t magic = r.u32();
    if (magic != kCoordMagic) throw_error(ErrKind::Corrupt, "coordinator snapshot magic mismatch");
    uint32_t ver = r.u32();
    if (ver != kCoordinatorFormatVersion) throw_error(ErrKind::Corrupt, "unsupported coordinator format version");
    s.epoch = r.u32();
    s.seq = r.u64();
    uint32_t nn = r.u32();
    if (nn > 100000) throw_error(ErrKind::Corrupt, "node count unreasonable");
    for (uint32_t i = 0; i < nn; ++i) {
        CoordNode n;
        n.node_id = r.u64();
        n.label = r.str();
        n.addr.host = r.str();
        n.addr.port = r.u16();
        NodeCapabilities caps;
        uint32_t bits = r.u32();
        caps = capabilities_from_bits(bits);
        caps.max_host_bytes = r.u64();
        caps.max_persistent_bytes = r.u64();
        caps.max_accelerator_bytes = r.u64();
        n.caps = caps;
        n.session_id = r.u64();
        n.session_epoch = r.u64();
        n.last_heartbeat_ms = r.u64();
        n.online = r.u8() != 0;
        s.nodes.push_back(std::move(n));
    }
    uint32_t no = r.u32();
    if (no > 1000000) throw_error(ErrKind::Corrupt, "object count unreasonable");
    for (uint32_t i = 0; i < no; ++i) {
        std::vector<uint8_t> buf = r.bytes(r.u32());
        s.objects.push_back(deserialize_coord_object(buf.data(), buf.size()));
    }
    return s;
}

}  // namespace ctf