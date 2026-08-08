#include "context_fabric/authority/coordinator_state.h"

#include <unordered_set>

#include "context_fabric/core/integrity.h"
#include "context_fabric/core/serializer.h"
#include "context_fabric/core/error.h"
#include "context_fabric/core/version.h"

namespace ctf {

namespace {
constexpr uint32_t kCoordMagic = 0x4F464343u;  // Wire bytes "OFCC".
constexpr uint32_t kLegacyCoordinatorFormatVersion = 1;

ObjectId read_object_id(Reader& r) {
    uint64_t hi = r.u64();
    uint64_t lo = r.u64();
    return ObjectId::from_parts(hi, lo);
}
}  // namespace

void serialize_coord_object(const CoordObject& o, std::vector<uint8_t>& out) {
    if (o.replicas.size() > 100000 || o.persistent_nodes.size() > 100000) {
        throw_error(ErrKind::Capacity, "coordinator object holder count unreasonable");
    }
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
    w.u32(checked_u32_size(o.replicas.size(), "coordinator replica count"));
    for (uint64_t n : o.replicas) w.u64(n);
    w.u32(checked_u32_size(o.persistent_nodes.size(), "coordinator persistent count"));
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
    try {
        Reader r(data, len);
        CoordObject o;
        o.id = read_object_id(r);
        if (!o.id.valid()) throw_error(ErrKind::Corrupt, "coordinator object id is zero");
        std::vector<uint8_t> md = r.bytes(r.u32());
        if (o.metadata.deserialize(md.data(), md.size()) != md.size()) {
            throw_error(ErrKind::Corrupt, "coordinator object metadata has trailing bytes");
        }
        o.generation = Generation::from_value(r.u64());
        o.lease_generation = r.u64();
        const uint32_t lifecycle = r.u32();
        if (lifecycle > static_cast<uint32_t>(LifecycleState::Released)) {
            throw_error(ErrKind::Corrupt, "coordinator object lifecycle is invalid", lifecycle);
        }
        o.lifecycle = static_cast<LifecycleState>(lifecycle);
        o.authoritative_node = r.u64();
        const uint32_t domain = r.u32();
        if (domain > static_cast<uint32_t>(StorageDomain::RemotePersistent)) {
            throw_error(ErrKind::Corrupt, "coordinator object storage domain is invalid", domain);
        }
        o.authoritative_domain = static_cast<StorageDomain>(domain);
        o.integrity.crc32c = r.u64();
        o.integrity.sha = r.u64();
        o.integrity.bytes = r.u64();
        uint32_t nr = r.u32();
        if (nr > 100000 || static_cast<size_t>(nr) > r.remaining() / sizeof(uint64_t)) {
            throw_error(ErrKind::Corrupt, "replica list unreasonable");
        }
        o.replicas.reserve(nr);
        for (uint32_t i = 0; i < nr; ++i) o.replicas.push_back(r.u64());
        uint32_t np = r.u32();
        if (np > 100000 || static_cast<size_t>(np) > r.remaining() / sizeof(uint64_t)) {
            throw_error(ErrKind::Corrupt, "persistent list unreasonable");
        }
        o.persistent_nodes.reserve(np);
        for (uint32_t i = 0; i < np; ++i) o.persistent_nodes.push_back(r.u64());
        o.owner_node = r.u64();
        o.created_unix_ms = r.u64();
        o.expires_unix_ms = r.u64();
        std::vector<uint8_t> lin = r.bytes(r.u32());
        o.lineage = deserialize_lineage(lin.data(), lin.size());
        if (!r.at_end()) throw_error(ErrKind::Corrupt, "coordinator object has trailing bytes");
        return o;
    } catch (const CfError& e) {
        if (e.kind() == ErrKind::Corrupt) throw;
        throw_error(ErrKind::Corrupt, std::string("invalid coordinator object: ") + e.what(), e.code());
    }
}

void serialize_coordinator_snapshot(const CoordinatorSnapshot& s, std::vector<uint8_t>& out) {
    if (s.nodes.size() > 100000 || s.objects.size() > 1000000) {
        throw_error(ErrKind::Capacity, "coordinator snapshot entry count unreasonable");
    }
    Writer w;
    w.u32(kCoordMagic);
    w.u32(kCoordinatorFormatVersion);
    w.u32(s.epoch);
    w.u64(s.seq);
    w.u32(checked_u32_size(s.nodes.size(), "coordinator node count"));
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
    w.u32(checked_u32_size(s.objects.size(), "coordinator object count"));
    for (const auto& o : s.objects) {
        std::vector<uint8_t> buf;
        serialize_coord_object(o, buf);
        w.bytes(buf.data(), buf.size());
    }
    const uint32_t checksum = Crc32C::compute(w.data().data(), w.data().size());
    w.u32(checksum);
    out = w.data();
}

CoordinatorSnapshot deserialize_coordinator_snapshot(const uint8_t* data, size_t len) {
    try {
        if (data == nullptr || len < 8) throw_error(ErrKind::Corrupt, "coordinator snapshot is truncated");

        Reader prefix(data, len);
        const uint32_t magic = prefix.u32();
        if (magic != kCoordMagic) throw_error(ErrKind::Corrupt, "coordinator snapshot magic mismatch");
        const uint32_t ver = prefix.u32();
        if (ver != kLegacyCoordinatorFormatVersion && ver != kCoordinatorFormatVersion) {
            throw_error(ErrKind::Corrupt, "unsupported coordinator format version", ver);
        }

        size_t body_len = len;
        if (ver == kCoordinatorFormatVersion) {
            if (len < 12) throw_error(ErrKind::Corrupt, "checksummed coordinator snapshot is truncated");
            body_len -= sizeof(uint32_t);
            const uint32_t stored = (static_cast<uint32_t>(data[body_len]) << 24) |
                                    (static_cast<uint32_t>(data[body_len + 1]) << 16) |
                                    (static_cast<uint32_t>(data[body_len + 2]) << 8) |
                                    static_cast<uint32_t>(data[body_len + 3]);
            const uint32_t computed = Crc32C::compute(data, body_len);
            if (stored != computed) throw_error(ErrKind::Corrupt, "coordinator snapshot checksum mismatch");
        }

        Reader r(data, body_len);
        (void)r.u32();
        (void)r.u32();
        CoordinatorSnapshot s;
        s.epoch = r.u32();
        s.seq = r.u64();
        uint32_t nn = r.u32();
        if (nn > 100000) throw_error(ErrKind::Corrupt, "node count unreasonable");
        s.nodes.reserve(nn);
        std::unordered_set<uint64_t> node_ids;
        for (uint32_t i = 0; i < nn; ++i) {
            CoordNode n;
            n.node_id = r.u64();
            if (n.node_id == 0 || !node_ids.insert(n.node_id).second) {
                throw_error(ErrKind::Corrupt, "coordinator snapshot contains an invalid or duplicate node id");
            }
            n.label = r.str();
            n.addr.host = r.str();
            n.addr.port = r.u16();
            const uint32_t bits = r.u32();
            constexpr uint32_t kKnownCaps = kCapHostMemory | kCapLocalPersistent | kCapAccelerator | kCapCuda;
            if ((bits & ~kKnownCaps) != 0) throw_error(ErrKind::Corrupt, "node capability bits are invalid", bits);
            NodeCapabilities caps = capabilities_from_bits(bits);
            caps.max_host_bytes = r.u64();
            caps.max_persistent_bytes = r.u64();
            caps.max_accelerator_bytes = r.u64();
            n.caps = caps;
            n.session_id = r.u64();
            n.session_epoch = r.u64();
            n.last_heartbeat_ms = r.u64();
            const uint8_t online = r.u8();
            if (online > 1) throw_error(ErrKind::Corrupt, "node online flag is invalid", online);
            n.online = online != 0;
            s.nodes.push_back(std::move(n));
        }
        uint32_t no = r.u32();
        if (no > 1000000) throw_error(ErrKind::Corrupt, "object count unreasonable");
        s.objects.reserve(no);
        std::unordered_set<ObjectId> object_ids;
        for (uint32_t i = 0; i < no; ++i) {
            std::vector<uint8_t> buf = r.bytes(r.u32());
            CoordObject object = deserialize_coord_object(buf.data(), buf.size());
            if (!object_ids.insert(object.id).second) {
                throw_error(ErrKind::Corrupt, "coordinator snapshot contains a duplicate object id");
            }
            s.objects.push_back(std::move(object));
        }
        if (!r.at_end()) throw_error(ErrKind::Corrupt, "coordinator snapshot has trailing bytes");
        return s;
    } catch (const CfError& e) {
        if (e.kind() == ErrKind::Corrupt) throw;
        throw_error(ErrKind::Corrupt, std::string("invalid coordinator snapshot: ") + e.what(), e.code());
    }
}

}  // namespace ctf
