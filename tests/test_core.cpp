#include "test_harness.h"

#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"
#include "context_fabric/core/metadata.h"
#include "context_fabric/core/serializer.h"
#include "context_fabric/core/state_machine.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/core/json.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/core/clock.h"
#include "context_fabric/core/lineage.h"
#include "context_fabric/core/node.h"

using namespace ctf;

CF_TEST(object_id_deterministic_and_stable) {
    ObjectId a = ObjectId::deterministic("ns", "obj1");
    ObjectId b = ObjectId::deterministic("ns", "obj1");
    ObjectId c = ObjectId::deterministic("ns", "obj2");
    CF_CHECK(a == b);
    CF_CHECK(a != c);
    CF_CHECK(a.valid());
    // Round-trip through hex.
    ObjectId d = ObjectId::from_hex(a.to_hex());
    CF_CHECK(d == a);
    // Determinism across re-creation.
    ObjectId e = ObjectId::deterministic("ns", "obj1");
    CF_CHECK(e == a);
    // Hashable + comparable.
    CF_CHECK(a < c || c < a);
}

CF_TEST(object_id_deterministic_components_are_unambiguous) {
    CF_CHECK(ObjectId::deterministic("a@b", "c") != ObjectId::deterministic("a", "b@c"));
    CF_CHECK(ObjectId::deterministic("", "a@b") != ObjectId::deterministic("@a", "b"));
}

CF_TEST(object_id_random_unique) {
    ObjectId a = ObjectId::random();
    ObjectId b = ObjectId::random();
    CF_CHECK(a != b);
    CF_CHECK(a.valid());
    CF_CHECK(ObjectId::from_hex(a.to_hex()) == a);
}

CF_TEST(object_id_invalid_hex_rejected) {
    CF_CHECK_THROWS(ObjectId::from_hex("zzzz"), ErrKind::InvalidArgument);
    CF_CHECK_THROWS(ObjectId::from_hex("abc"), ErrKind::InvalidArgument);
}

CF_TEST(generation_advances) {
    Generation g = Generation::first();
    CF_CHECK(g.value() == 1);
    Generation g2 = g.next();
    CF_CHECK(g2.value() == 2);
    CF_CHECK(g2 != g);
    CF_CHECK(g2 > g);
    Generation h = Generation::from_hex("0000000000000002");
    CF_CHECK(h == g2);
    CF_CHECK(Generation::from_hex(g2.to_hex()) == g2);
}

CF_TEST(generation_rejects_invalid_advance_and_overflow) {
    CF_CHECK_THROWS(Generation().next(), ErrKind::State);
    CF_CHECK_THROWS(Generation::from_hex("ffffffffffffffff").next(), ErrKind::Capacity);
}

CF_TEST(metadata_compatibility_strict) {
    ContextMetadata m1;
    m1.namespace_id = "model-a";
    m1.model_id = "llm";
    m1.model_revision = "rev1";
    m1.semantic_class = SemanticClass::KvCache;
    m1.tensor_shape = "1,2048,128";
    m1.tensor_dtype = "bfloat16";
    m1.compatibility_fingerprint = m1.compute_compatibility_fingerprint();

    ContextMetadata m2 = m1;
    CF_CHECK(m1.compatible_with(m2));

    ContextMetadata m3 = m1;
    m3.model_revision = "rev2";
    m3.compatibility_fingerprint = m3.compute_compatibility_fingerprint();
    CF_CHECK(!m1.compatible_with(m3));

    ContextMetadata m4 = m1;
    m4.tensor_dtype = "float16";
    m4.compatibility_fingerprint = m4.compute_compatibility_fingerprint();
    CF_CHECK(!m1.compatible_with(m4));

    // Different namespace must not be silently compatible.
    ContextMetadata m5 = m1;
    m5.namespace_id = "model-b";
    m5.compatibility_fingerprint = m5.compute_compatibility_fingerprint();
    CF_CHECK(!m1.compatible_with(m5));

    // User tags must match.
    ContextMetadata m6 = m1;
    m6.tags = {"x"};
    m6.compatibility_fingerprint = m6.compute_compatibility_fingerprint();
    CF_CHECK(!m1.compatible_with(m6));

    // A caller-supplied/stale matching fingerprint is never sufficient to
    // bypass descriptor comparison.
    ContextMetadata forged = m1;
    forged.model_revision = "forged";
    CF_CHECK(forged.compatibility_fingerprint == m1.compatibility_fingerprint);
    CF_CHECK(!m1.compatible_with(forged));
}

CF_TEST(metadata_serialization_roundtrip) {
    ContextMetadata md;
    md.semantic_class = SemanticClass::AgentState;
    md.namespace_id = "agent-ns";
    md.producer = "runtime-x";
    md.model_id = "m";
    md.model_revision = "r";
    md.tokenizer_id = "tok";
    md.attention_layout = "gqa";
    md.tensor_shape = "2,3";
    md.tensor_dtype = "f32";
    md.quantization = "fp16";
    md.architecture_namespace = "arch";
    md.tags = {"a", "b", "c"};
    md.logical_size = 100;
    md.byte_size = 4096;
    md.sharing = SharingPolicy::NamespaceShared;
    md.persistence = PersistencePolicy::Persistent;
    md.mutable_flag = true;
    md.lifetime_seconds = 3600;
    md.recompute_seconds = 1.5;
    md.recompute_resource_cost = 2.0;
    md.reuse_probability = 0.8;
    md.pinned = true;
    md.compatibility_fingerprint = md.compute_compatibility_fingerprint();

    std::vector<uint8_t> buf;
    md.serialize(buf);
    ContextMetadata out;
    out.deserialize(buf.data(), buf.size());
    CF_CHECK(out.semantic_class == md.semantic_class);
    CF_CHECK(out.namespace_id == md.namespace_id);
    CF_CHECK(out.producer == md.producer);
    CF_CHECK(out.model_id == md.model_id);
    CF_CHECK(out.tags == md.tags);
    CF_CHECK(out.persistence == md.persistence);
    CF_CHECK(out.mutable_flag == md.mutable_flag);
    CF_CHECK(out.lifetime_seconds == md.lifetime_seconds);
    CF_CHECK(out.compatibility_fingerprint == md.compatibility_fingerprint);
    CF_CHECK(out.compatible_with(md));
}

CF_TEST(metadata_deserialization_fails_closed) {
    ContextMetadata md;
    md.namespace_id = "ns";
    md.model_id = "model";
    md.compatibility_fingerprint = md.compute_compatibility_fingerprint();
    std::vector<uint8_t> bytes;
    md.serialize(bytes);

    auto forged_fingerprint = bytes;
    forged_fingerprint.back() ^= 1;
    ContextMetadata out;
    CF_CHECK_THROWS(out.deserialize(forged_fingerprint.data(), forged_fingerprint.size()), ErrKind::Protocol);

    auto invalid_enum = bytes;
    invalid_enum[0] = 0xff;
    invalid_enum[1] = 0xff;
    invalid_enum[2] = 0xff;
    invalid_enum[3] = 0xff;
    CF_CHECK_THROWS(out.deserialize(invalid_enum.data(), invalid_enum.size()), ErrKind::Protocol);

    auto trailing = bytes;
    trailing.push_back(0);
    CF_CHECK_THROWS(out.deserialize(trailing.data(), trailing.size()), ErrKind::Protocol);
}

CF_TEST(serializer_bounds_checks) {
    Writer w;
    w.u32(1);
    w.u64(2);
    w.str("hello");
    Reader r(w.data().data(), w.data().size());
    CF_CHECK(r.u32() == 1);
    CF_CHECK(r.u64() == 2);
    CF_CHECK(r.str() == "hello");
    CF_CHECK(r.at_end());
    CF_CHECK_THROWS(r.u32(), ErrKind::Protocol);

    // Malformed length field must be rejected (overrun).
    std::vector<uint8_t> bad = {0, 0, 0, 255};  // claims 255-byte string
    Reader r2(bad.data(), bad.size());
    CF_CHECK_THROWS(r2.str(), ErrKind::Protocol);
}

CF_TEST(serializer_null_zero_is_safe_and_nonzero_is_rejected) {
    Reader empty(nullptr, 0);
    CF_CHECK(empty.at_end());
    CF_CHECK_NOTHROW(empty.bytes(nullptr, 0));
    CF_CHECK_THROWS(Reader(nullptr, 1), ErrKind::Protocol);

    Writer writer;
    CF_CHECK_NOTHROW(writer.append(nullptr, 0));
    CF_CHECK_NOTHROW(writer.bytes(nullptr, 0));
    CF_CHECK_THROWS(writer.append(nullptr, 1), ErrKind::InvalidArgument);
}

CF_TEST(json_writer_preserves_valid_utf8_and_escapes_invalid_bytes) {
    const std::string valid = "caf\xC3\xA9";
    CF_CHECK(json_escape_string(valid) == std::string("\"") + valid + "\"");

    const std::string invalid(1, static_cast<char>(0xFF));
    CF_CHECK(json_escape_string(invalid) == "\"\\u00ff\"");

    JsonObject object;
    object.field("bad\xFF", invalid).field("finite", 1.25).field("nan", std::numeric_limits<double>::quiet_NaN());
    std::string encoded;
    object.write_to(encoded);
    CF_CHECK(encoded == "{\"bad\\u00ff\":\"\\u00ff\",\"finite\":1.250000,\"nan\":null}");
}

CF_TEST(state_machine_legal_and_illegal) {
    // Legal
    CF_CHECK(LifecycleAuthority::is_legal(LifecycleState::Creating, LifecycleState::Available));
    CF_CHECK(LifecycleAuthority::is_legal(LifecycleState::Available, LifecycleState::LeasedWrite));
    CF_CHECK(LifecycleAuthority::is_legal(LifecycleState::LeasedWrite, LifecycleState::Available));
    CF_CHECK(LifecycleAuthority::is_legal(LifecycleState::Available, LifecycleState::Migrating));
    CF_CHECK(LifecycleAuthority::is_legal(LifecycleState::Migrating, LifecycleState::Available));
    CF_CHECK(LifecycleAuthority::is_legal(LifecycleState::Creating, LifecycleState::Released));
    CF_CHECK(LifecycleAuthority::is_legal(LifecycleState::Corrupt, LifecycleState::Released));
    CF_CHECK(LifecycleAuthority::is_legal(LifecycleState::Expired, LifecycleState::Released));
    CF_CHECK(LifecycleAuthority::is_legal(LifecycleState::Recovering, LifecycleState::Available));
    CF_CHECK(LifecycleAuthority::is_legal(LifecycleState::Restoring, LifecycleState::Available));

    // Illegal
    CF_CHECK(!LifecycleAuthority::is_legal(LifecycleState::Available, LifecycleState::Creating));
    CF_CHECK(!LifecycleAuthority::is_legal(LifecycleState::Released, LifecycleState::Available));
    CF_CHECK(!LifecycleAuthority::is_legal(LifecycleState::Migrating, LifecycleState::LeasedWrite));
    CF_CHECK(!LifecycleAuthority::is_legal(LifecycleState::Corrupt, LifecycleState::Available));
    CF_CHECK(!LifecycleAuthority::is_legal(LifecycleState::Creating, LifecycleState::LeasedWrite));
    CF_CHECK(!LifecycleAuthority::is_legal(LifecycleState::LeasedRead, LifecycleState::Migrating));

    // transition() enforces with typed error
    CF_CHECK_THROWS(LifecycleAuthority::transition(LifecycleState::Available, LifecycleState::Creating),
                    ErrKind::State);
    CF_CHECK_NOTHROW(LifecycleAuthority::transition(LifecycleState::Creating, LifecycleState::Available));
}

CF_TEST(integrity_crc_and_sha) {
    std::vector<uint8_t> data = generate_content(1, 2, 100000);
    uint32_t crc = Crc32C::compute(data.data(), data.size());
    CF_CHECK(crc != 0);
    // Same input => same output; different input => different output.
    CF_CHECK(Crc32C::compute(data.data(), data.size()) == crc);
    std::vector<uint8_t> data2 = data;
    data2[500] ^= 0xFF;
    CF_CHECK(Crc32C::compute(data2.data(), data2.size()) != crc);

    auto sha1 = Sha256::compute(data.data(), data.size());
    auto sha2 = Sha256::compute(data.data(), data.size());
    CF_CHECK(sha1 == sha2);
    CF_CHECK(Sha256::compute(data2.data(), data2.size()) != sha1);

    IntegrityBuilder ib;
    ib.update(data.data(), data.size());
    IntegrityId idg = ib.finish();
    CF_CHECK(idg.crc32c == crc);
    CF_CHECK(idg.bytes == data.size());

    // Known answer vector for SHA-256("abc").
    const char* abc = "abc";
    auto d = Sha256::compute(abc, 3);
    const uint8_t expected[32] = {0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40,
                                  0xde, 0x5d, 0xae, 0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17,
                                  0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad};
    CF_CHECK(std::memcmp(d.data(), expected, 32) == 0);

    // Standard CRC-32C/Castagnoli known-answer vector.
    const char* digits = "123456789";
    CF_CHECK(Crc32C::compute(digits, 9) == 0xE3069283u);
}

CF_TEST(prng_deterministic) {
    auto a = generate_content(99, 5, 4096);
    auto b = generate_content(99, 5, 4096);
    CF_CHECK(a == b);
    auto c = generate_content(99, 6, 4096);
    CF_CHECK(a != c);
    auto d = generate_content(100, 5, 4096);
    CF_CHECK(a != d);
    Prng p1(1234);
    Prng p2(1234);
    for (int i = 0; i < 1000; ++i) CF_CHECK(p1.next_u64() == p2.next_u64());

    const std::vector<uint8_t> known = generate_content(1, 2, 8);
    const uint8_t expected[8] = {0xc0, 0x8c, 0x66, 0xe5, 0xda, 0xab, 0xcd, 0xb0};
    CF_CHECK(std::memcmp(known.data(), expected, sizeof(expected)) == 0);
    CF_CHECK_THROWS(fill_deterministic(1, 0, nullptr, 1), ErrKind::InvalidArgument);
    CF_CHECK_NOTHROW(fill_deterministic(1, 0, nullptr, 0));
}

CF_TEST(byte_size_parsing) {
    uint64_t v = 0;
    CF_CHECK(parse_byte_size("1024", v) && v == 1024);
    CF_CHECK(parse_byte_size("1KiB", v) && v == 1024);
    CF_CHECK(parse_byte_size("1KB", v) && v == 1000);
    CF_CHECK(parse_byte_size("2MiB", v) && v == 2 * 1024 * 1024);
    CF_CHECK(parse_byte_size("1.5GiB", v) && v == 1610612736);
    CF_CHECK(parse_byte_size("512 MB", v) && v == 512ull * 1000 * 1000);
    CF_CHECK(!parse_byte_size("", v));
    CF_CHECK(!parse_byte_size("abc", v));
    CF_CHECK(!parse_byte_size("1XB", v));
    CF_CHECK(!parse_byte_size("1MiGISBH", v));
    CF_CHECK(!parse_byte_size("1.5", v));
    CF_CHECK(!parse_byte_size("1.", v));
    CF_CHECK(!parse_byte_size("1MiB-junk", v));
}

CF_TEST(node_address_formatting_is_not_truncated) {
    NodeAddress address;
    address.host = std::string(240, 'a') + ".example";
    address.port = 65535;
    CF_CHECK(address.to_string() == address.host + ":65535");

    address.host = "2001:db8::1";
    address.port = 80;
    CF_CHECK(address.to_string() == "[2001:db8::1]:80");
}

CF_TEST(lineage_roundtrip) {
    std::vector<LineageEntry> lin;
    LineageEntry e;
    e.object = ObjectId::deterministic("ns", "child");
    e.generation = Generation::first();
    e.parent = ObjectId::deterministic("ns", "parent");
    e.operation = "derive";
    e.producer = "node-a";
    e.creation_sequence = 7;
    e.created_unix_ms = 12345;
    lin.push_back(e);

    std::vector<uint8_t> buf;
    serialize_lineage(lin, buf);
    auto out = deserialize_lineage(buf.data(), buf.size());
    CF_CHECK(out.size() == 1);
    CF_CHECK(out[0].object == e.object);
    CF_CHECK(out[0].generation == e.generation);
    CF_CHECK(out[0].parent == e.parent);
    CF_CHECK(out[0].operation == e.operation);
    CF_CHECK(out[0].creation_sequence == 7);
}

CF_TEST(enum_names_roundtrip) {
    for (uint32_t i = 0; i <= 10; ++i) {
        auto c = static_cast<SemanticClass>(i);
        auto name = semantic_class_name(c);
        auto back = semantic_class_from_name(name);
        CF_CHECK(back.has_value());
        CF_CHECK(*back == c);
    }
    for (uint32_t i = 0; i <= 12; ++i) {
        auto s = static_cast<LifecycleState>(i);
        auto name = lifecycle_state_name(s);
        auto back = lifecycle_state_from_name(name);
        CF_CHECK(back.has_value());
        CF_CHECK(*back == s);
    }
}
