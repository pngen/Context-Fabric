#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace ctf {

// ---------------------------------------------------------------------------
// Cross-platform TCP socket abstraction over Winsock2 (Windows) or BSD/POSIX
// sockets (Linux). Presentable behavior:
//   - blocking connect with a timeout
//   - blocking send_all / recv_some with per-operation timeouts
//   - framed-message friendly: the caller never assumes one send() == one msg
//   - clean shutdown and close
// Socket handles never leak through the public API.
// ---------------------------------------------------------------------------
class Socket {
public:
    Socket() noexcept = default;
    ~Socket();

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    [[nodiscard]] bool valid() const noexcept { return fd_ != kInvalid; }

    // Connect to host:port. Numeric IPv4 resolution and every socket attempt
    // share `timeout_ms`; Windows hostname resolution uses the same deadline.
    // POSIX hostname resolution inherits the system resolver's own deadline
    // because getaddrinfo has no portable cancellation API. Returns false on
    // failure (peer refused, timeout, DNS failure). When `keep_running` is
    // supplied, a pending attempt is abandoned promptly once it reads false,
    // so an owner that is shutting down never waits out the full timeout.
    bool connect(const std::string& host, uint16_t port, uint64_t timeout_ms,
                 const std::atomic<bool>* keep_running = nullptr);

    // Bind+listen on host:port (port 0 = ephemeral). On success sets
    // `bound_port` to the actual bound port.
    bool listen(const std::string& host, uint16_t port, uint16_t& bound_port);

    // Accept a pending connection (blocking, bounded by timeout_ms).
    Socket accept(uint64_t timeout_ms);

    // Send exactly `len` bytes; returns number of bytes sent (== len on full
    // success) or throws CfError(Transport/Timeout) on partial failure.
    void send_all(const void* data, size_t len, uint64_t timeout_ms);

    // Receive up to `len` bytes; returns bytes received (0 = orderly EOF).
    // Throws CfError(Timeout) if the peer sends nothing within timeout_ms.
    size_t recv_some(void* data, size_t len, uint64_t timeout_ms);

    // Graceful shutdown of the send direction.
    void shutdown_send();

    // Shut down both directions. On Windows this reliably wakes a thread
    // blocked in send()/recv() on this socket (closing the handle from another
    // thread does not). Call before joining threads that use the socket.
    void shutdown_both();

    void close() noexcept;

    [[nodiscard]] std::string peer_address() const;

private:
#if defined(_WIN32)
    static constexpr uintptr_t kInvalid = ~uintptr_t(0);
    uintptr_t fd_ = kInvalid;
#else
    static constexpr int kInvalid = -1;
    int fd_ = kInvalid;
#endif
};

}  // namespace ctf
