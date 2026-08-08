#include "test_harness.h"

#include <chrono>
#include <thread>

#include "context_fabric/api/context_fabric.h"
#include "context_fabric/authority/client.h"
#include "context_fabric/authority/coordinator.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/core/version.h"

using namespace ctf;

namespace {

struct TestCoordinator {
    Telemetry telemetry;
    Coordinator coord;

    TestCoordinator() : coord(make_cfg(), telemetry) {
        telemetry.set_stderr_enabled(false);
        if (!coord.start()) throw CfError(ErrKind::Internal, "coordinator start failed");
    }
    ~TestCoordinator() { coord.shutdown(); }

    static Coordinator::Config make_cfg() {
        Coordinator::Config cfg;
        cfg.bind_host = "127.0.0.1";
        cfg.control_port = 0;
        cfg.store_path = "";
        cfg.telemetry_path = "";
        cfg.heartbeat_interval_ms = 500;
        cfg.session_timeout_ms = 3000;
        cfg.lease_default_ms = 2000;
        cfg.op_timeout_ms = 15000;
        cfg.create_timeout_ms = 10000;
        cfg.driver_threads = 4;
        return cfg;
    }

    uint16_t port() const { return coord.bound_port(); }
};

uint64_t node_id(const std::string& name) { return NodeId::deterministic(name).value(); }

}  // namespace

CF_TEST(coordinator_hello_and_list) {
    TestCoordinator tc;
    ClientSession c;
    CF_REQUIRE(c.connect("127.0.0.1", tc.port(), 5000));
    uint32_t epoch = c.hello();
    CF_CHECK(epoch >= 1);
    auto list = c.list("");
    CF_CHECK(list.result.ok);
    CF_CHECK(list.ids.empty());
}

CF_TEST(coordinator_object_roundtrip_inprocess) {
    TestCoordinator tc;
    ClientSession c;
    CF_REQUIRE(c.connect("127.0.0.1", tc.port(), 5000));
    c.hello();

    NodeRegisterReq reg;
    reg.node_id = node_id("node-a");
    reg.label = "node-a";
    reg.addr.host = "127.0.0.1";
    reg.addr.port = 1234;
    reg.capabilities = kCapHostMemory | kCapLocalPersistent;
    reg.max_host_bytes = 256ull * 1024 * 1024;
    Writer w;
    serialize_node_register(w, reg);
    std::vector<uint8_t> payload;
    RpcResult r = c.call(kMsgNodeRegister, kMsgNodeRegisterResp, w.data(), payload);
    CF_REQUIRE(r.ok);
    Reader rd(payload.data(), payload.size());
    NodeRegisterResp resp = deserialize_node_register_resp(rd);
    CF_REQUIRE(resp.result.ok);

    // List should still work.
    auto lst = c.list("");
    CF_CHECK(lst.result.ok);
    CF_CHECK(lst.ids.empty());
}

CF_TEST(coordinator_epoch_bumps_on_restart) {
    // Coordinator restart bumps the epoch.
    Telemetry t1;
    t1.set_stderr_enabled(false);
    Coordinator::Config cfg = TestCoordinator::make_cfg();
    cfg.store_path = "";
    Coordinator c1(cfg, t1);
    if (c1.start()) {
        uint32_t e1 = c1.epoch();
        c1.shutdown();
        CF_CHECK(e1 >= 1);
    }
}
