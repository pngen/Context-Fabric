#include "context_fabric/transport/socket.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "context_fabric/core/error.h"

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
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
using socket_int_t = SOCKET;
#else
using socket_int_t = int;
#endif

namespace {

using SteadyClock = std::chrono::steady_clock;

SteadyClock::time_point deadline_after(uint64_t timeout_ms) {
    const auto now = SteadyClock::now();
    const auto max_delta = SteadyClock::time_point::max() - now;
    const auto max_ms = std::chrono::duration_cast<std::chrono::milliseconds>(max_delta).count();
    const uint64_t bounded = std::min(timeout_ms, static_cast<uint64_t>(max_ms));
    return now + std::chrono::milliseconds(bounded);
}

uint64_t remaining_ms(SteadyClock::time_point deadline) {
    const auto now = SteadyClock::now();
    if (now >= deadline) return 0;
    const auto delta = deadline - now;
    const auto whole = std::chrono::duration_cast<std::chrono::milliseconds>(delta).count();
    return whole > 0 ? static_cast<uint64_t>(whole) : 1;
}

bool is_invalid_socket(socket_int_t fd) {
#if defined(_WIN32)
    return fd == INVALID_SOCKET;
#else
    return fd < 0;
#endif
}

#if defined(_WIN32)
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
#endif

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
    DWORD t = static_cast<DWORD>(std::min<uint64_t>(std::max<uint64_t>(ms, 1), MAXDWORD));
    return setsockopt(to_fd(fd), SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&t), sizeof(t)) == 0;
#else
    timeval tv{};
    const uint64_t bounded = std::max<uint64_t>(ms, 1);
    const uint64_t max_seconds = static_cast<uint64_t>(std::numeric_limits<decltype(tv.tv_sec)>::max());
    tv.tv_sec = static_cast<decltype(tv.tv_sec)>(std::min<uint64_t>(bounded / 1000, max_seconds));
    tv.tv_usec = tv.tv_sec == static_cast<decltype(tv.tv_sec)>(max_seconds)
                     ? 0
                     : static_cast<decltype(tv.tv_usec)>((bounded % 1000) * 1000);
    return setsockopt(to_fd(fd), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0;
#endif
}

bool set_send_timeout(uintptr_t fd, uint64_t ms) {
#if defined(_WIN32)
    DWORD t = static_cast<DWORD>(std::min<uint64_t>(std::max<uint64_t>(ms, 1), MAXDWORD));
    return setsockopt(to_fd(fd), SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&t), sizeof(t)) == 0;
#else
    timeval tv{};
    const uint64_t bounded = std::max<uint64_t>(ms, 1);
    const uint64_t max_seconds = static_cast<uint64_t>(std::numeric_limits<decltype(tv.tv_sec)>::max());
    tv.tv_sec = static_cast<decltype(tv.tv_sec)>(std::min<uint64_t>(bounded / 1000, max_seconds));
    tv.tv_usec = tv.tv_sec == static_cast<decltype(tv.tv_sec)>(max_seconds)
                     ? 0
                     : static_cast<decltype(tv.tv_usec)>((bounded % 1000) * 1000);
    return setsockopt(to_fd(fd), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == 0;
#endif
}

bool numeric_ipv4(const std::string& host, uint16_t port, sockaddr_in& address) {
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    return inet_pton(AF_INET, host.c_str(), &address.sin_addr) == 1;
}

bool is_localhost(const std::string& host) {
    static constexpr char name[] = "localhost";
    if (host.size() != sizeof(name) - 1) return false;
    for (size_t i = 0; i < host.size(); ++i) {
        char c = host[i];
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
        if (c != name[i]) return false;
    }
    return true;
}

bool resolve_ipv4(const std::string& host, uint16_t port, uint64_t timeout_ms,
                  std::vector<sockaddr_in>& addresses) {
    addresses.clear();
    sockaddr_in numeric{};
    if (numeric_ipv4(host, port, numeric)) {
        addresses.push_back(numeric);
        return true;
    }
    if (is_localhost(host)) {
        numeric.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addresses.push_back(numeric);
        return true;
    }
    if (host.empty() || timeout_ms == 0) return false;

#if defined(_WIN32)
    if (host.size() > static_cast<size_t>(INT_MAX)) return false;
    int wide_chars = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, host.data(),
                                         static_cast<int>(host.size()), nullptr, 0);
    if (wide_chars <= 0) return false;
    std::wstring wide_host(static_cast<size_t>(wide_chars), L'\0');
    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, host.data(), static_cast<int>(host.size()),
                            wide_host.data(), wide_chars) != wide_chars) {
        return false;
    }

    ADDRINFOEXW hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    timeval timeout{};
    const uint64_t seconds = timeout_ms / 1000;
    timeout.tv_sec = static_cast<long>((std::min)(seconds, static_cast<uint64_t>(LONG_MAX)));
    timeout.tv_usec = seconds >= static_cast<uint64_t>(LONG_MAX)
                          ? 0
                          : static_cast<long>((timeout_ms % 1000) * 1000);
    PADDRINFOEXW raw = nullptr;
    int rc = GetAddrInfoExW(wide_host.c_str(), nullptr, NS_ALL, nullptr, &hints, &raw, &timeout,
                            nullptr, nullptr, nullptr);
    struct AddressGuard {
        void operator()(ADDRINFOEXW* value) const noexcept {
            if (value != nullptr) FreeAddrInfoExW(value);
        }
    };
    std::unique_ptr<ADDRINFOEXW, AddressGuard> result(raw);
    if (rc != 0 || result == nullptr) return false;
    for (ADDRINFOEXW* current = result.get(); current != nullptr; current = current->ai_next) {
        if (current->ai_family != AF_INET || current->ai_addr == nullptr ||
            current->ai_addrlen < sizeof(sockaddr_in)) {
            continue;
        }
        sockaddr_in address{};
        std::memcpy(&address, current->ai_addr, sizeof(address));
        address.sin_port = htons(port);
        addresses.push_back(address);
    }
#else
    // POSIX getaddrinfo has no portable caller-supplied deadline or safe
    // cancellation mechanism. Numeric endpoints above remain fully bounded;
    // hostname resolution inherits the platform resolver's deadline.
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    addrinfo* raw = nullptr;
    int rc = ::getaddrinfo(host.c_str(), nullptr, &hints, &raw);
    struct AddressGuard {
        void operator()(addrinfo* value) const noexcept {
            if (value != nullptr) freeaddrinfo(value);
        }
    };
    std::unique_ptr<addrinfo, AddressGuard> result(raw);
    if (rc != 0 || result == nullptr) return false;
    for (addrinfo* current = result.get(); current != nullptr; current = current->ai_next) {
        if (current->ai_family != AF_INET || current->ai_addr == nullptr ||
            current->ai_addrlen < sizeof(sockaddr_in)) {
            continue;
        }
        sockaddr_in address{};
        std::memcpy(&address, current->ai_addr, sizeof(address));
        address.sin_port = htons(port);
        addresses.push_back(address);
    }
#endif
    return !addresses.empty();
}

// Blocking connect with timeout via select.
bool connect_with_timeout(uintptr_t fd, const sockaddr_in& addr, uint64_t timeout_ms) {
    const auto deadline = deadline_after(timeout_ms);
#if defined(_WIN32)
    u_long mode = 1;
    if (ioctlsocket(to_fd(fd), FIONBIO, &mode) != 0) return false;
#else
    int flags = fcntl(to_fd(fd), F_GETFL, 0);
    if (flags < 0 || fcntl(to_fd(fd), F_SETFL, flags | O_NONBLOCK) != 0) return false;
#endif

    int rc = ::connect(to_fd(fd), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
    if (rc == 0) {
        // Already connected; restore blocking.
#if defined(_WIN32)
        mode = 0;
        if (ioctlsocket(to_fd(fd), FIONBIO, &mode) != 0) return false;
#else
        if (fcntl(to_fd(fd), F_SETFL, flags) != 0) return false;
#endif
        return true;
    }

#if defined(_WIN32)
    int err = WSAGetLastError();
    if (err != WSAEWOULDBLOCK && err != WSAEINPROGRESS) return false;
#else
    if (errno != EINPROGRESS) return false;
#endif

    while (true) {
        const uint64_t left = remaining_ms(deadline);
        if (left == 0) return false;
        fd_set wset;
        FD_ZERO(&wset);
        FD_SET(to_fd(fd), &wset);
        timeval tv{};
        tv.tv_sec = static_cast<long>(std::min<uint64_t>(left / 1000, static_cast<uint64_t>(LONG_MAX)));
        tv.tv_usec = static_cast<long>((left % 1000) * 1000);
#if defined(_WIN32)
        rc = ::select(0, nullptr, &wset, nullptr, &tv);
        if (rc < 0 && WSAGetLastError() == WSAEINTR) continue;
#else
        rc = ::select(to_fd(fd) + 1, nullptr, &wset, nullptr, &tv);
        if (rc < 0 && errno == EINTR) continue;
#endif
        if (rc <= 0) return false;
        break;
    }

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
    if (ioctlsocket(to_fd(fd), FIONBIO, &mode) != 0) return false;
#else
    if (fcntl(to_fd(fd), F_SETFL, flags) != 0) return false;
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
    if (host.empty() || port == 0 || timeout_ms == 0) return false;
    const auto deadline = deadline_after(timeout_ms);

    std::vector<sockaddr_in> addresses;
    if (!resolve_ipv4(host, port, remaining_ms(deadline), addresses)) return false;

    bool ok = false;
    for (const sockaddr_in& address : addresses) {
        const uint64_t left = remaining_ms(deadline);
        if (left == 0) break;
        socket_int_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (is_invalid_socket(s)) continue;
        fd_ = to_handle(s);

#if defined(_WIN32)
        BOOL nodelay = TRUE;
        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));
#else
        int nodelay = 1;
        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#endif

        if (connect_with_timeout(fd_, address, left)) {
            ok = true;
            break;
        }
        close_fd(fd_);
        fd_ = kInvalid;
    }
    return ok;
}

bool Socket::listen(const std::string& host, uint16_t port, uint16_t& bound_port) {
    close();

    socket_int_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (is_invalid_socket(s)) return false;
    fd_ = to_handle(s);

#if defined(_WIN32)
    BOOL exclusive = TRUE;
    setsockopt(s, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, reinterpret_cast<const char*>(&exclusive), sizeof(exclusive));
#else
    int reuse = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
#endif

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host.empty() || host == "0.0.0.0" || host == "*") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
            close();
            return false;
        }
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
    if (!valid()) return Socket();
    const auto deadline = deadline_after(timeout_ms);
    int rc = 0;
    while (true) {
        const uint64_t left = remaining_ms(deadline);
        if (left == 0) return Socket();
        fd_set rset;
        FD_ZERO(&rset);
        FD_SET(to_fd(fd_), &rset);
        timeval tv{};
        tv.tv_sec = static_cast<long>(std::min<uint64_t>(left / 1000, static_cast<uint64_t>(LONG_MAX)));
        tv.tv_usec = static_cast<long>((left % 1000) * 1000);
#if defined(_WIN32)
        rc = ::select(0, &rset, nullptr, nullptr, &tv);
        if (rc < 0 && WSAGetLastError() == WSAEINTR) continue;
#else
        rc = ::select(to_fd(fd_) + 1, &rset, nullptr, nullptr, &tv);
        if (rc < 0 && errno == EINTR) continue;
#endif
        if (rc <= 0) return Socket();
        break;
    }

    socket_int_t s = ::accept(to_fd(fd_), nullptr, nullptr);
    if (is_invalid_socket(s)) return Socket();

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
    if (len == 0) return;
    if (data == nullptr) throw_error(ErrKind::InvalidArgument, "cannot send from a null buffer");
    if (!valid()) throw_error(ErrKind::Transport, "cannot send on a closed socket");
    const auto deadline = deadline_after(timeout_ms);
    const char* p = static_cast<const char*>(data);
    size_t sent = 0;
    while (sent < len) {
        const uint64_t left = remaining_ms(deadline);
        if (left == 0) throw_error(ErrKind::Timeout, "send timed out");
        if (!set_send_timeout(fd_, left)) {
            throw_error(ErrKind::Transport, "failed to set send timeout");
        }
#if defined(_WIN32)
        int n = ::send(to_fd(fd_), p + sent, static_cast<int>((len - sent) > INT_MAX ? INT_MAX : (len - sent)), 0);
#else
        ssize_t n = ::send(to_fd(fd_), p + sent, len - sent, MSG_NOSIGNAL);
#endif
        if (n <= 0) {
#if defined(_WIN32)
            int err = WSAGetLastError();
            if (err == WSAEINTR) continue;
            if (err == WSAETIMEDOUT || err == WSAEWOULDBLOCK) {
                throw_error(ErrKind::Timeout, "send timed out");
            }
#else
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) throw_error(ErrKind::Timeout, "send timed out");
#endif
            throw_error(ErrKind::Transport, "send failed");
        }
        sent += static_cast<size_t>(n);
    }
}

size_t Socket::recv_some(void* data, size_t len, uint64_t timeout_ms) {
    if (len == 0) return 0;
    if (data == nullptr) throw_error(ErrKind::InvalidArgument, "cannot receive into a null buffer");
    if (!valid()) throw_error(ErrKind::Transport, "cannot receive on a closed socket");
    const auto deadline = deadline_after(timeout_ms);
    while (true) {
        const uint64_t left = remaining_ms(deadline);
        if (left == 0) throw_error(ErrKind::Timeout, "receive timed out");
        if (!set_recv_timeout(fd_, left)) {
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
            if (err == WSAEINTR) continue;
            if (err == WSAETIMEDOUT || err == WSAEWOULDBLOCK) {
                throw_error(ErrKind::Timeout, "receive timed out");
            }
            if (err == WSAECONNRESET) throw_error(ErrKind::Transport, "connection reset");
#else
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) throw_error(ErrKind::Timeout, "receive timed out");
            if (errno == ECONNRESET) throw_error(ErrKind::Transport, "connection reset");
#endif
            throw_error(ErrKind::Transport, "receive failed");
        }
        return static_cast<size_t>(n);
    }
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
    if (inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf)) == nullptr) return "";
    return std::string(buf) + ":" + std::to_string(ntohs(addr.sin_port));
#else
    sockaddr_in addr;
    socklen_t len = sizeof(addr);
    if (getpeername(to_fd(fd_), reinterpret_cast<sockaddr*>(&addr), &len) != 0) return "";
    char buf[INET_ADDRSTRLEN];
    if (inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf)) == nullptr) return "";
    return std::string(buf) + ":" + std::to_string(ntohs(addr.sin_port));
#endif
}

}  // namespace ctf
