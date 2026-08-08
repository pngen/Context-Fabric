#include "test_harness.h"

#include <limits>
#include <vector>

#include "context_fabric/protocol/messages.h"
#include "context_fabric/core/serializer.h"

using namespace ctf;

CF_TEST(protocol_result_serialization) {
    RpcResult ok = RpcResult::success();
    std::vector<uint8_t> buf;
    Writer w;
    serialize_result(w, ok);
    Reader r(w.data().data(), w.data().size());
    RpcResult out = deserialize_result(r);
    CF_CHECK(out.ok);
    CF_CHECK(out.err == ErrKind::None);

    RpcResult fail = RpcResult::failure(ErrKind::Integrity, "bad checksum", 42);
    Writer w2;
    serialize_result(w2, fail);
    Reader r2(w2.data().data(), w2.data().size());
    RpcResult out2 = deserialize_result(r2);
    CF_CHECK(!out2.ok);
    CF_CHECK(out2.err == ErrKind::Integrity);
    CF_CHECK(out2.err_code == 42);
    CF_CHECK(out2.message == "bad checksum");
}

CF_TEST(protocol_message_roundtrips) {
    // NodeRegister
    {
        NodeRegisterReq req;
        req.node_id = 123;
        req.label = "node-a";
        req.addr.host = "127.0.0.1";
        req.addr.port = 9000;
        req.capabilities = kCapHostMemory | kCapLocalPersistent;
        req.max_host_bytes = 1024;
        Writer w;
        serialize_node_register(w, req);
        Reader r(w.data().data(), w.data().size());
        NodeRegisterReq out = deserialize_node_register(r);
        CF_CHECK(out.node_id == 123);
        CF_CHECK(out.label == "node-a");
        CF_CHECK(out.addr.port == 9000);
        CF_CHECK(out.capabilities == req.capabilities);
    }
    // ObjectCreate
    {
        ObjectCreateReq req;
        req.metadata.namespace_id = "ns";
        req.metadata.byte_size = 100;
        req.object_id = ObjectId::deterministic("ns", "obj");
        req.parent = ObjectId::deterministic("ns", "parent");
        req.owner_node = 7;
        req.derivation_op = "derive";
        req.metadata.compatibility_fingerprint = req.metadata.compute_compatibility_fingerprint();
        Writer w;
        serialize_object_create(w, req);
        Reader r(w.data().data(), w.data().size());
        ObjectCreateReq out = deserialize_object_create(r);
        CF_CHECK(out.metadata.namespace_id == "ns");
        CF_CHECK(out.object_id == req.object_id);
        CF_CHECK(out.parent == req.parent);
        CF_CHECK(out.owner_node == 7);
        CF_CHECK(out.derivation_op == "derive");
    }
    // Lease
    {
        LeaseAcquireResp resp;
        resp.result = RpcResult::success();
        resp.has_lease = 1;
        resp.lease.object = ObjectId::deterministic("ns", "obj");
        resp.lease.generation = Generation::first();
        resp.lease.holder = NodeId(5);
        resp.lease.mode = LeaseMode::Write;
        resp.lease.coordinator_epoch = 3;
        resp.lease.lease_generation = 2;
        resp.lease.lease_id = 99;
        resp.lease.acquired_unix_ms = 100;
        resp.lease.expires_unix_ms = 200;
        Writer w;
        serialize_lease_acquire_resp(w, resp);
        Reader r(w.data().data(), w.data().size());
        LeaseAcquireResp out = deserialize_lease_acquire_resp(r);
        CF_CHECK(out.has_lease == 1);
        CF_CHECK(out.lease.object == resp.lease.object);
        CF_CHECK(out.lease.generation == resp.lease.generation);
        CF_CHECK(out.lease.mode == LeaseMode::Write);
        CF_CHECK(out.lease.coordinator_epoch == 3);
        CF_CHECK(out.lease.lease_id == 99);
    }
    // InstructRecv with metadata
    {
        InstructRecvReq req;
        req.op_token = 42;
        req.object_id = ObjectId::deterministic("ns", "obj");
        req.generation = Generation::first();
        req.source_node = 9;
        req.byte_size = 1000;
        req.crc32c = 1;
        req.sha = 2;
        req.role = static_cast<uint32_t>(CopyRole::Replica);
        req.domain = static_cast<uint32_t>(StorageDomain::HostMemory);
        req.metadata.model_id = "m";
        req.metadata.model_revision = "r1";
        Writer w;
        serialize_instruct_recv(w, req);
        Reader r(w.data().data(), w.data().size());
        InstructRecvReq out = deserialize_instruct_recv(r);
        CF_CHECK(out.op_token == 42);
        CF_CHECK(out.source_node == 9);
        CF_CHECK(out.role == req.role);
        CF_CHECK(out.metadata.model_id == "m");
        CF_CHECK(out.metadata.model_revision == "r1");
    }
    // DataBlock
    {
        DataBlockMsg blk;
        blk.op_token = 3;
        blk.object_id = ObjectId::deterministic("ns", "obj");
        blk.generation = Generation::first();
        blk.seq = 0;
        blk.total_chunks = 1;
        blk.byte_size = 8;
        blk.crc32c = 7;
        blk.sha = 8;
        blk.data = {1, 2, 3, 4, 5, 6, 7, 8};
        Writer w;
        serialize_data_block(w, blk);
        Reader r(w.data().data(), w.data().size());
        DataBlockMsg out = deserialize_data_block(r);
        CF_CHECK(out.op_token == 3);
        CF_CHECK(out.seq == 0);
        CF_CHECK(out.byte_size == 8);
        CF_CHECK(out.data == blk.data);
        CF_CHECK(out.total_chunks == 1);
    }
}

CF_TEST(protocol_malformed_rejected) {
    // Truncated data block.
    DataBlockMsg blk;
    blk.op_token = 1;
    blk.object_id = ObjectId::random();
    blk.generation = Generation::first();
    blk.data = {1, 2, 3};
    Writer w;
    serialize_data_block(w, blk);
    std::vector<uint8_t> truncated(w.data().begin(), w.data().begin() + static_cast<std::ptrdiff_t>(w.size() - 2));
    Reader r(truncated.data(), truncated.size());
    CF_CHECK_THROWS(deserialize_data_block(r), ErrKind::Protocol);

    // Boolean fields and enum values must use their canonical encodings.
    Writer bad_result_bool;
    bad_result_bool.u8(2);
    bad_result_bool.u32(static_cast<uint32_t>(ErrKind::None));
    bad_result_bool.u64(0);
    bad_result_bool.str("");
    Reader result_bool_reader(bad_result_bool.data().data(), bad_result_bool.size());
    CF_CHECK_THROWS(deserialize_result(result_bool_reader), ErrKind::Protocol);

    Writer bad_error_kind;
    bad_error_kind.u8(0);
    bad_error_kind.u32((std::numeric_limits<uint32_t>::max)());
    bad_error_kind.u64(0);
    bad_error_kind.str("failure");
    Reader error_kind_reader(bad_error_kind.data().data(), bad_error_kind.size());
    CF_CHECK_THROWS(deserialize_result(error_kind_reader), ErrKind::Protocol);

    Writer bad_verify_bool;
    bad_verify_bool.u64(1);
    bad_verify_bool.u64(2);
    bad_verify_bool.u64(3);
    bad_verify_bool.u8(2);
    Reader verify_bool_reader(bad_verify_bool.data().data(), bad_verify_bool.size());
    CF_CHECK_THROWS(deserialize_verify(verify_bool_reader), ErrKind::Protocol);

    Writer bad_domain;
    bad_domain.u64(1);
    bad_domain.u64(2);
    bad_domain.u64(3);
    bad_domain.u32((std::numeric_limits<uint32_t>::max)());
    bad_domain.u8(0);
    Reader domain_reader(bad_domain.data().data(), bad_domain.size());
    CF_CHECK_THROWS(deserialize_migrate(domain_reader), ErrKind::Protocol);

    RpcResult inconsistent = RpcResult::success();
    inconsistent.err = ErrKind::Integrity;
    Writer inconsistent_writer;
    CF_CHECK_THROWS(serialize_result(inconsistent_writer, inconsistent), ErrKind::InvalidArgument);

    DecisionQueryResp nonfinite;
    nonfinite.result = RpcResult::success();
    nonfinite.decision.score = (std::numeric_limits<double>::quiet_NaN)();
    Writer nonfinite_writer;
    CF_CHECK_THROWS(serialize_decision_query_resp(nonfinite_writer, nonfinite), ErrKind::InvalidArgument);
}
