#include "context_fabric/transport/socket.h"

#include <cstring>
#include <string>

#include "context_fabric/core/error.h"

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#endif

namespace ctf {

#if defined(_WIN32)
namespace {
struct WsaInitializer {
    WsaInitializer() {
        WSADATA data;
        if (WSAStartup(MAKEWORD(2, 2), &data) != 0) {
            fprintf(stderr, "WSAStartup failed\n");
        }
    }
    ~WsaInitializer() { WSACleanup(); }
};
WsaInitializer g_wsa;
}  // namespace
#endif

#if defined(_WIN32)
using socket_int_t = SOCKET;
#else
using socket_int_t = int;
#endif

namespace {

socket_int_t to_fd(uintptr_t fd) {
#if defined(_WIN32)
    return static_cast<SOCKET>(fd);
#else
    return static_cast<int>(fd);
#endif
}

uintptr_t to_handle(socket_int_t fd) {
#if defined(_WIN32)
    return static_cast<uintptr_t>(fd);
#else
    return static_cast<uintptr_t>(fd);
#endif
}

void close_fd(uintptr_t fd) {
#if defined(_WIN32)
    closesocket(static_cast<SOCKET>(fd));
#else
    ::close(static_cast<int>(fd));
#endif
}

bool set_recv_timeout(uintptr_t fd, uint64_t ms) {
#if defined(_WIN32)
    DWORD t = static_cast<DWORD>(ms);
    return setsockopt(to_fd(fd), SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&t), sizeof(t)) == 0;
#else
    timeval tv;
    tv.tv_sec = static_cast<long>(ms / 1000);
    tv.tv_usec = static_cast<long>((ms % 1000) * 1000);
    return setsockopt(to_fd(fd), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0;
#endif
}

bool set_send_timeout(uintptr_t fd, uint64_t ms) {
#if defined(_WIN32)
    DWORD t = static_cast<DWORD>(ms);
    return setsockopt(to_fd(fd), SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&t), sizeof(t)) == 0;
#else
    timeval tv;
    tv.tv_sec = static_cast<long>(ms / 1000);
    tv.tv_usec = static_cast<long>((ms % 1000) * 1000);
    return setsockopt(to_fd(fd), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == 0;
#endif
}

// Blocking connect with timeout via select.
bool connect_with_timeout(uintptr_t fd, const sockaddr_in& addr, uint64_t timeout_ms) {
#if defined(_WIN32)
    u_long mode = 1;
    ioctlsocket(to_fd(fd), FIONBIO, &mode);
#else
    int flags = fcntl(to_fd(fd), F_GETFL, 0);
    fcntl(to_fd(fd), F_SETFL, flags | O_NONBLOCK);
#endif

    int rc = ::connect(to_fd(fd), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
    if (rc == 0) {
        // Already connected; restore blocking.
#if defined(_WIN32)
        mode = 0;
        ioctlsocket(to_fd(fd), FIONBIO, &mode);
#else
        int fl = fcntl(to_fd(fd), F_GETFL, 0);
        fcntl(to_fd(fd), F_SETFL, fl & ~O_NONBLOCK);
#endif
        return true;
    }

#if defined(_WIN32)
    int err = WSAGetLastError();
    if (err != WSAEWOULDBLOCK && err != WSAEINPROGRESS) return false;
#else
    if (errno != EINPROGRESS) return false;
#endif

    fd_set wset;
    FD_ZERO(&wset);
    FD_SET(to_fd(fd), &wset);
    timeval tv;
    tv.tv_sec = static_cast<long>(timeout_ms / 1000);
    tv.tv_usec = static_cast<long>((timeout_ms % 1000) * 1000);
    rc = ::select(0, nullptr, &wset, nullptr, &tv);
    if (rc <= 0) return false;

    // Check socket error.
    int so_error = 0;
#if defined(_WIN32)
    int len = sizeof(so_error);
#else
    socklen_t len = sizeof(so_error);
#endif
    if (getsockopt(to_fd(fd), SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&so_error), &len) != 0 || so_error != 0) {
        return false;
    }

#if defined(_WIN32)
    mode = 0;
    ioctlsocket(to_fd(fd), FIONBIO, &mode);
#else
    int fl = fcntl(to_fd(fd), F_GETFL, 0);
    fcntl(to_fd(fd), F_SETFL, fl & ~O_NONBLOCK);
#endif
    return true;
}

}  // namespace

Socket::Socket(Socket&& other) noexcept : fd_(other.fd_) { other.fd_ = kInvalid; }

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        other.fd_ = kInvalid;
    }
    return *this;
}

Socket::~Socket() { close(); }

bool Socket::connect(const std::string& host, uint16_t port, uint64_t timeout_ms) {
    close();

    addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* result = nullptr;
    char port_buf[16];
    snprintf(port_buf, sizeof(port_buf), "%u", static_cast<unsigned>(port));
    int rc = ::getaddrinfo(host.c_str(), port_buf, &hints, &result);
    if (rc != 0 || result == nullptr) return false;

    bool ok = false;
    for (addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
        socket_int_t s = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (s == static_cast<socket_int_t>(~uintptr_t(0))) continue;
        fd_ = to_handle(s);

#if defined(_WIN32)
        BOOL nodelay = TRUE;
        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));
#else
        int nodelay = 1;
        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#endif

        sockaddr_in* sa = reinterpret_cast<sockaddr_in*>(ai->ai_addr);
        if (connect_with_timeout(fd_, *sa, timeout_ms)) {
            ok = true;
            break;
        }
        close_fd(fd_);
        fd_ = kInvalid;
    }
    freeaddrinfo(result);
    return ok;
}

bool Socket::listen(const std::string& host, uint16_t port, uint16_t& bound_port) {
    close();

    socket_int_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == static_cast<socket_int_t>(~uintptr_t(0))) return false;
    fd_ = to_handle(s);

    int reuse = 1;
#if defined(_WIN32)
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
#else
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
#endif

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host.empty() || host == "0.0.0.0" || host == "*") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    }

    if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close_fd(fd_);
        fd_ = kInvalid;
        return false;
    }
    if (::listen(s, 64) != 0) {
        close_fd(fd_);
        fd_ = kInvalid;
        return false;
    }

    sockaddr_in bound;
#if defined(_WIN32)
    int len = sizeof(bound);
#else
    socklen_t len = sizeof(bound);
#endif
    if (getsockname(s, reinterpret_cast<sockaddr*>(&bound), &len) == 0) {
        bound_port = ntohs(bound.sin_port);
    } else {
        close_fd(fd_);
        fd_ = kInvalid;
        return false;
    }
    return true;
}

Socket Socket::accept(uint64_t timeout_ms) {
    fd_set rset;
    FD_ZERO(&rset);
    FD_SET(to_fd(fd_), &rset);
    timeval tv;
    tv.tv_sec = static_cast<long>(timeout_ms / 1000);
    tv.tv_usec = static_cast<long>((timeout_ms % 1000) * 1000);
    int rc = ::select(0, &rset, nullptr, nullptr, &tv);
    if (rc <= 0) return Socket();

    socket_int_t s = ::accept(to_fd(fd_), nullptr, nullptr);
    if (s == static_cast<socket_int_t>(~uintptr_t(0))) return Socket();

    Socket out;
    out.fd_ = to_handle(s);
#if defined(_WIN32)
    BOOL nodelay = TRUE;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));
#else
    int nodelay = 1;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#endif
    return out;
}

void Socket::send_all(const void* data, size_t len, uint64_t timeout_ms) {
    if (len == 0 || !valid()) return;
    if (!set_send_timeout(fd_, timeout_ms)) {
        throw_error(ErrKind::Transport, "failed to set send timeout");
    }
    const char* p = static_cast<const char*>(data);
    size_t sent = 0;
    while (sent < len) {
#if defined(_WIN32)
        int n = ::send(to_fd(fd_), p + sent, static_cast<int>((len - sent) > INT_MAX ? INT_MAX : (len - sent)), 0);
#else
        ssize_t n = ::send(to_fd(fd_), p + sent, len - sent, MSG_NOSIGNAL);
#endif
        if (n <= 0) {
#if defined(_WIN32)
            int err = WSAGetLastError();
            if (err == WSAETIMEDOUT) throw_error(ErrKind::Timeout, "send timed out");
#else
            if (errno == EAGAIN || errno == EWOULDBLOCK) throw_error(ErrKind::Timeout, "send timed out");
#endif
            throw_error(ErrKind::Transport, "send failed");
        }
        sent += static_cast<size_t>(n);
    }
}

size_t Socket::recv_some(void* data, size_t len, uint64_t timeout_ms) {
    if (len == 0 || !valid()) return 0;
    if (!set_recv_timeout(fd_, timeout_ms)) {
        throw_error(ErrKind::Transport, "failed to set receive timeout");
    }
#if defined(_WIN32)
    int n = ::recv(to_fd(fd_), static_cast<char*>(data), static_cast<int>(len > INT_MAX ? INT_MAX : len), 0);
#else
    ssize_t n = ::recv(to_fd(fd_), data, len, 0);
#endif
    if (n == 0) return 0;  // orderly EOF
    if (n < 0) {
#if defined(_WIN32)
        int err = WSAGetLastError();
        if (err == WSAETIMEDOUT) throw_error(ErrKind::Timeout, "receive timed out");
        if (err == WSAECONNRESET) throw_error(ErrKind::Transport, "connection reset");
#else
        if (errno == EAGAIN || errno == EWOULDBLOCK) throw_error(ErrKind::Timeout, "receive timed out");
        if (errno == ECONNRESET) throw_error(ErrKind::Transport, "connection reset");
#endif
        throw_error(ErrKind::Transport, "receive failed");
    }
    return static_cast<size_t>(n);
}

void Socket::shutdown_send() {
    if (!valid()) return;
#if defined(_WIN32)
    ::shutdown(to_fd(fd_), SD_SEND);
#else
    ::shutdown(to_fd(fd_), SHUT_WR);
#endif
}

void Socket::shutdown_both() {
    if (!valid()) return;
#if defined(_WIN32)
    ::shutdown(to_fd(fd_), SD_BOTH);
#else
    ::shutdown(to_fd(fd_), SHUT_RDWR);
#endif
}

void Socket::close() noexcept {
    if (valid()) {
        close_fd(fd_);
        fd_ = kInvalid;
    }
}

std::string Socket::peer_address() const {
    if (!valid()) return "";
#if defined(_WIN32)
    sockaddr_in addr;
    int len = sizeof(addr);
    if (getpeername(to_fd(fd_), reinterpret_cast<sockaddr*>(&addr), &len) != 0) return "";
    char buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
    return std::string(buf) + ":" + std::to_string(ntohs(addr.sin_port));
#else
    sockaddr_in addr;
    socklen_t len = sizeof(addr);
    if (getpeername(to_fd(fd_), reinterpret_cast<sockaddr*>(&addr), &len) != 0) return "";
    char buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
    return std::string(buf) + ":" + std::to_string(ntohs(addr.sin_port));
#endif
}

}  // namespace ctf