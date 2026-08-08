#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/transport/frame.h"
#include "context_fabric/transport/socket.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Framed connection to a peer over TCP. The transport is deliberately a thin
// project-owned abstraction so future RDMA/shared-memory/IPC/QUIC transports
// can be added without changing core state semantics.
// ---------------------------------------------------------------------------
class TransportConnection {
public:
    TransportConnection() = default;
    TransportConnection(std::string peer_name, Socket socket, uint64_t read_timeout_ms,
                        uint64_t write_timeout_ms);

    TransportConnection(const TransportConnection&) = delete;
    TransportConnection& operator=(const TransportConnection&) = delete;
    TransportConnection(TransportConnection&&) noexcept = default;
    TransportConnection& operator=(TransportConnection&&) noexcept = default;

    bool connect(const std::string& host, uint16_t port, uint64_t timeout_ms);

    void send(MessageType type, uint64_t request_id, uint64_t node_id,
              const std::vector<uint8_t>& payload);
    FrameHeader recv(std::vector<uint8_t>& payload);

    void close();
    [[nodiscard]] bool is_open() const noexcept;

    [[nodiscard]] const std::string& peer() const noexcept { return peer_; }

private:
    std::string peer_;
    Socket sock_;
    uint64_t read_timeout_ms_ = 10000;
    uint64_t write_timeout_ms_ = 10000;
};

// ---------------------------------------------------------------------------
// Listener for inbound data-plane connections.
// ---------------------------------------------------------------------------
class TransportListener {
public:
    TransportListener() = default;

    bool listen(const std::string& host, uint16_t port, uint16_t& bound_port);

    // Returns a connection or an invalid connection (empty peer) on timeout.
    TransportConnection accept();

    void close();

private:
    Socket sock_;
    uint64_t acceptor_timeout_ms_ = 200;
    uint64_t read_timeout_ms_ = 30000;
    uint64_t write_timeout_ms_ = 30000;
};

}  // namespace ctf