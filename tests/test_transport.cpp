#include "test_harness.h"

#include <atomic>
#include <thread>
#include <chrono>
#include <vector>

#include "context_fabric/transport/frame.h"
#include "context_fabric/transport/socket.h"
#include "context_fabric/transport/tcp_transport.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/core/serializer.h"
#include "context_fabric/core/version.h"
#include "context_fabric/protocol/messages.h"

using namespace ctf;

CF_TEST(frame_header_roundtrip) {
    FrameHeader h;
    h.magic = kFrameMagic;
    h.version = kProtocolVersion;
    h.flags = kFrameFlagChecksum;
    h.type = 77;
    h.request_id = 0xDEADBEEF12345678ull;
    h.node_id = 0x1122334455667788ull;
    h.payload_len = 100;
    h.payload_crc = 0x12345678;
    h.header_crc = 0x9ABCDEF0;
    uint8_t buf[kFrameHeaderSize];
    encode_frame_header(h, buf);
    FrameHeader out = decode_frame_header(buf, kFrameHeaderSize);
    CF_CHECK(out.magic == h.magic);
    CF_CHECK(out.version == h.version);
    CF_CHECK(out.type == h.type);
    CF_CHECK(out.request_id == h.request_id);
    CF_CHECK(out.node_id == h.node_id);
    CF_CHECK(out.payload_len == h.payload_len);
}

CF_TEST(frame_header_bad_size_rejected) {
    uint8_t buf[10] = {0};
    CF_CHECK_THROWS(decode_frame_header(buf, 10), ErrKind::Protocol);
}

CF_TEST(frame_malformed_rejected_fuzz) {
    // Deterministic generated invalid frames must never crash the decoder and
    // must be rejected by validation.
    Prng rng(0xC0FFEE);
    uint32_t rejected = 0;
    for (int iter = 0; iter < 500; ++iter) {
        std::vector<uint8_t> buf(kFrameHeaderSize);
        for (auto& b : buf) b = rng.next_u8();
        bool threw = false;
        try {
            FrameHeader h = decode_frame_header(buf.data(), kFrameHeaderSize);
            validate_frame_header(h);
        } catch (const CfError&) {
            threw = true;
        }
        if (threw) ++rejected;
    }
    // The overwhelming majority of random frames fail magic/version/cheksum.
    CF_CHECK(rejected > 480);
}

CF_TEST(frame_header_bad_version_magic_rejected) {
    FrameHeader h;
    h.magic = 0xDEADBEEF;
    h.version = kProtocolVersion;
    h.flags = kFrameFlagChecksum;
    CF_CHECK_THROWS(validate_frame_header(h), ErrKind::Protocol);

    h.magic = kFrameMagic;
    h.version = kProtocolVersion + 1;
    CF_CHECK_THROWS(validate_frame_header(h), ErrKind::Protocol);

    h.version = kProtocolVersion;
    h.flags = 0;  // missing checksum flag
    CF_CHECK_THROWS(validate_frame_header(h), ErrKind::Protocol);

    h.flags = kFrameFlagChecksum;
    h.payload_len = kFrameMaxPayload + 1;
    CF_CHECK_THROWS(validate_frame_header(h), ErrKind::Protocol);
}

CF_TEST(tcp_echo_roundtrip) {
    // A real TCP echo over loopback: the socket layer must reassemble frames
    // correctly across partial writes/reads.
    Socket listener;
    uint16_t port = 0;
    CF_REQUIRE(listener.listen("127.0.0.1", 0, port));

    std::thread server([&] {
        Socket s = listener.accept(5000);
        CF_CHECK(s.valid());
        if (!s.valid()) return;
        // Echo 1 MiB in 64 KiB chunks to exercise partial I/O.
        std::vector<uint8_t> buf(64 * 1024);
        size_t total = 0;
        while (total < 1024 * 1024) {
            size_t n = s.recv_some(buf.data(), buf.size(), 5000);
            if (n == 0) break;
            s.send_all(buf.data(), n, 5000);
            total += n;
        }
        s.shutdown_send();
        s.close();
        listener.close();
    });

    Socket client;
    CF_REQUIRE(client.connect("127.0.0.1", port, 5000));
    std::vector<uint8_t> data = generate_content(7, 8, 1024 * 1024);
    client.send_all(data.data(), data.size(), 10000);
    client.shutdown_send();

    std::vector<uint8_t> received;
    received.reserve(data.size());
    std::vector<uint8_t> buf(64 * 1024);
    while (true) {
        size_t n = client.recv_some(buf.data(), buf.size(), 5000);
        if (n == 0) break;
        received.insert(received.end(), buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(n));
    }
    client.close();
    server.join();
    CF_CHECK(received == data);
}

CF_TEST(tcp_timeout_handled) {
    Socket listener;
    uint16_t port = 0;
    CF_REQUIRE(listener.listen("127.0.0.1", 0, port));
    std::thread server([&] {
        Socket s = listener.accept(5000);
        if (s.valid()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            s.close();
        }
        listener.close();
    });
    Socket client;
    CF_REQUIRE(client.connect("127.0.0.1", port, 5000));
    // Reading from a silent peer must time out (not hang forever).
    uint8_t buf[16];
    CF_CHECK_THROWS(client.recv_some(buf, sizeof(buf), 100), ErrKind::Timeout);
    client.close();
    server.join();
}

CF_TEST(tcp_resolution_respects_supported_deadlines) {
    Socket listener;
    uint16_t port = 0;
    CF_REQUIRE(listener.listen("127.0.0.1", 0, port));
    std::thread server([&] {
        Socket accepted = listener.accept(3000);
        if (accepted.valid()) accepted.close();
        listener.close();
    });

    Socket hostname_client;
    auto started = std::chrono::steady_clock::now();
    bool connected = hostname_client.connect("localhost", port, 2000);
    auto hostname_elapsed = std::chrono::steady_clock::now() - started;
    if (connected) hostname_client.close();
    server.join();
    CF_CHECK(connected);
    CF_CHECK(hostname_elapsed < std::chrono::milliseconds(2500));

    Socket zero_budget;
    started = std::chrono::steady_clock::now();
    CF_CHECK(!zero_budget.connect("127.0.0.1", port, 0));
    CF_CHECK(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(100));

#if defined(_WIN32)
    // Windows GetAddrInfoExW accepts the caller's resolver deadline. `.invalid`
    // is reserved for negative tests and cannot resolve successfully.
    Socket missing_host;
    started = std::chrono::steady_clock::now();
    CF_CHECK(!missing_host.connect("context-fabric-resolution-test.invalid", 1, 250));
    CF_CHECK(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(1500));
#endif
}

CF_TEST(send_frame_recv_frame_roundtrip) {
    Socket listener;
    uint16_t port = 0;
    CF_REQUIRE(listener.listen("127.0.0.1", 0, port));

    std::atomic<bool> server_ok{false};
    std::thread server([&] {
        TransportConnection conn("peer", listener.accept(5000), 5000, 5000);
        if (!conn.is_open()) return;
        std::vector<uint8_t> payload;
        FrameHeader hdr = conn.recv(payload);
        if (hdr.type == kMsgHello && hdr.request_id == 5) {
            server_ok.store(true);
        }
        conn.close();
        listener.close();
    });

    TransportConnection client("c", Socket(), 5000, 5000);
    CF_REQUIRE(client.connect("127.0.0.1", port, 5000));
    std::vector<uint8_t> payload;
    Writer w;
    w.u32(kProtocolVersion);
    w.str("client");
    client.send(kMsgHello, 5, 0, w.data());
    // No response expected; just wait for the server to observe.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    client.close();
    server.join();
    CF_CHECK(server_ok.load());
}

CF_TEST(send_frame_oversized_rejected) {
    Socket listener;
    uint16_t port = 0;
    CF_REQUIRE(listener.listen("127.0.0.1", 0, port));
    std::thread server([&] {
        Socket s = listener.accept(5000);
        if (s.valid()) s.close();
        listener.close();
    });
    TransportConnection client("c", Socket(), 5000, 5000);
    CF_REQUIRE(client.connect("127.0.0.1", port, 5000));
    std::vector<uint8_t> huge(kFrameMaxPayload + 1);
    CF_CHECK_THROWS(client.send(kMsgDataBlock, 1, 0, huge), ErrKind::Capacity);
    client.close();
    server.join();
}
