#include "context_fabric/transport/tcp_transport.h"

#include "context_fabric/core/clock.h"

namespace ctf {

// ---------------------------------------------------------------------------
// TransportConnection: a framed TCP connection to a peer.
// ---------------------------------------------------------------------------
TransportConnection::TransportConnection(std::string peer_name, Socket socket,
                                         uint64_t read_timeout_ms, uint64_t write_timeout_ms)
    : peer_(std::move(peer_name)),
      sock_(std::move(socket)),
      read_timeout_ms_(read_timeout_ms),
      write_timeout_ms_(write_timeout_ms) {}

bool TransportConnection::connect(const std::string& host, uint16_t port, uint64_t timeout_ms) {
    return sock_.connect(host, port, timeout_ms);
}

void TransportConnection::send(MessageType type, uint64_t request_id, uint64_t node_id,
                               const std::vector<uint8_t>& payload) {
    send_frame(sock_, type, request_id, node_id, payload, write_timeout_ms_);
}

FrameHeader TransportConnection::recv(std::vector<uint8_t>& payload) {
    return recv_frame(sock_, payload, read_timeout_ms_);
}

void TransportConnection::close() {
    sock_.shutdown_send();
    sock_.close();
}

[[nodiscard]] bool TransportConnection::is_open() const noexcept { return sock_.valid(); }

// ---------------------------------------------------------------------------
// TransportListener: accepts inbound data-plane connections.
// ---------------------------------------------------------------------------
bool TransportListener::listen(const std::string& host, uint16_t port, uint16_t& bound_port) {
    return sock_.listen(host, port, bound_port);
}

TransportConnection TransportListener::accept() {
    Socket s = sock_.accept(acceptor_timeout_ms_);
    if (!s.valid()) {
        return TransportConnection(std::string(), Socket(), read_timeout_ms_, write_timeout_ms_);
    }
    return TransportConnection(std::string("peer"), std::move(s), read_timeout_ms_, write_timeout_ms_);
}

void TransportListener::close() { sock_.close(); }

}  // namespace ctf