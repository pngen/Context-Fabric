#include "context_fabric/transport/frame.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>

#include "context_fabric/core/error.h"
#include "context_fabric/core/integrity.h"
#include "context_fabric/core/version.h"

namespace ctf {

namespace {

using SteadyClock = std::chrono::steady_clock;

SteadyClock::time_point deadline_after(uint64_t timeout_ms) {
    const auto now = SteadyClock::now();
    const auto max_delta = SteadyClock::time_point::max() - now;
    const auto max_ms = std::chrono::duration_cast<std::chrono::milliseconds>(max_delta).count();
    const uint64_t bounded = std::min(timeout_ms, static_cast<uint64_t>(max_ms));
    return now + std::chrono::milliseconds(bounded);
}

uint64_t remaining_or_timeout(SteadyClock::time_point deadline, const char* operation) {
    const auto now = SteadyClock::now();
    if (now >= deadline) throw_error(ErrKind::Timeout, operation);
    const auto whole = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
    return whole > 0 ? static_cast<uint64_t>(whole) : 1;
}

bool receive_cancelled(const std::atomic<bool>* running, const std::atomic<bool>* closed) {
    return (running != nullptr && !running->load(std::memory_order_acquire)) ||
           (closed != nullptr && closed->load(std::memory_order_acquire));
}

size_t recv_interruptible(Socket& sock, void* data, size_t len, bool deadline_active,
                          SteadyClock::time_point deadline, const std::atomic<bool>* running,
                          const std::atomic<bool>* closed) {
    const bool poll = running != nullptr || closed != nullptr;
    for (;;) {
        if (receive_cancelled(running, closed)) {
            throw_error(ErrKind::Transport, "frame receive cancelled");
        }
        uint64_t attempt_ms = deadline_active
                                  ? remaining_or_timeout(deadline, "frame receive timed out")
                                  : uint64_t{500};
        if (poll) attempt_ms = (std::min)(attempt_ms, uint64_t{500});
        try {
            return sock.recv_some(data, len, attempt_ms);
        } catch (const CfError& e) {
            if (e.kind() != ErrKind::Timeout || !poll) throw;
            if (receive_cancelled(running, closed)) {
                throw_error(ErrKind::Transport, "frame receive cancelled");
            }
            // A polling timeout consumes no bytes. Retain the caller's exact
            // progress and keep polling until cancellation or the frame's
            // absolute deadline.
            if (deadline_active && SteadyClock::now() >= deadline) throw;
        }
    }
}

}  // namespace

void validate_frame_header(const FrameHeader& h) {
    if (h.magic != kFrameMagic) {
        throw_error(ErrKind::Protocol, "frame magic mismatch", static_cast<uint64_t>(h.magic));
    }
    if (h.version != kProtocolVersion) {
        throw_error(ErrKind::Protocol, "protocol version mismatch", h.version);
    }
    if (h.payload_len > kFrameMaxPayload) {
        throw_error(ErrKind::Protocol, "frame payload exceeds maximum", h.payload_len);
    }
    if ((h.flags & kFrameFlagChecksum) == 0) {
        throw_error(ErrKind::Protocol, "frame missing mandatory checksum flag");
    }
    if ((h.flags & static_cast<uint16_t>(~kFrameFlagChecksum)) != 0) {
        throw_error(ErrKind::Protocol, "frame contains unsupported flags", h.flags);
    }
    if (h.reserved != 0) {
        throw_error(ErrKind::Protocol, "frame reserved field must be zero", h.reserved);
    }
}

void encode_frame_header(const FrameHeader& h, uint8_t* out) {
    if (out == nullptr) throw_error(ErrKind::InvalidArgument, "frame header output buffer is null");
    out[0] = static_cast<uint8_t>(h.magic >> 24);
    out[1] = static_cast<uint8_t>((h.magic >> 16) & 0xFF);
    out[2] = static_cast<uint8_t>((h.magic >> 8) & 0xFF);
    out[3] = static_cast<uint8_t>(h.magic & 0xFF);
    out[4] = static_cast<uint8_t>(h.version >> 8);
    out[5] = static_cast<uint8_t>(h.version & 0xFF);
    out[6] = static_cast<uint8_t>(h.flags >> 8);
    out[7] = static_cast<uint8_t>(h.flags & 0xFF);
    out[8] = static_cast<uint8_t>(h.type >> 8);
    out[9] = static_cast<uint8_t>(h.type & 0xFF);
    out[10] = static_cast<uint8_t>(h.reserved >> 8);
    out[11] = static_cast<uint8_t>(h.reserved & 0xFF);
    uint64_t rid = h.request_id;
    for (int i = 0; i < 8; ++i) out[12 + i] = static_cast<uint8_t>((rid >> (8 * (7 - i))) & 0xFF);
    uint64_t nid = h.node_id;
    for (int i = 0; i < 8; ++i) out[20 + i] = static_cast<uint8_t>((nid >> (8 * (7 - i))) & 0xFF);
    out[28] = static_cast<uint8_t>(h.payload_len >> 24);
    out[29] = static_cast<uint8_t>((h.payload_len >> 16) & 0xFF);
    out[30] = static_cast<uint8_t>((h.payload_len >> 8) & 0xFF);
    out[31] = static_cast<uint8_t>(h.payload_len & 0xFF);
    out[32] = static_cast<uint8_t>(h.payload_crc >> 24);
    out[33] = static_cast<uint8_t>((h.payload_crc >> 16) & 0xFF);
    out[34] = static_cast<uint8_t>((h.payload_crc >> 8) & 0xFF);
    out[35] = static_cast<uint8_t>(h.payload_crc & 0xFF);
    out[36] = static_cast<uint8_t>(h.header_crc >> 24);
    out[37] = static_cast<uint8_t>((h.header_crc >> 16) & 0xFF);
    out[38] = static_cast<uint8_t>((h.header_crc >> 8) & 0xFF);
    out[39] = static_cast<uint8_t>(h.header_crc & 0xFF);
}

FrameHeader decode_frame_header(const uint8_t* data, size_t len) {
    if (len != kFrameHeaderSize) {
        throw_error(ErrKind::Protocol, "frame header has wrong size", static_cast<uint64_t>(len));
    }
    if (data == nullptr) throw_error(ErrKind::Protocol, "frame header buffer is null");
    FrameHeader h;
    h.magic = (static_cast<uint32_t>(data[0]) << 24) | (static_cast<uint32_t>(data[1]) << 16) |
              (static_cast<uint32_t>(data[2]) << 8) | data[3];
    h.version = static_cast<uint16_t>((static_cast<uint16_t>(data[4]) << 8) | data[5]);
    h.flags = static_cast<uint16_t>((static_cast<uint16_t>(data[6]) << 8) | data[7]);
    h.type = static_cast<uint16_t>((static_cast<uint16_t>(data[8]) << 8) | data[9]);
    h.reserved = static_cast<uint16_t>((static_cast<uint16_t>(data[10]) << 8) | data[11]);
    uint64_t rid = 0, nid = 0;
    for (int i = 0; i < 8; ++i) rid = (rid << 8) | data[12 + i];
    for (int i = 0; i < 8; ++i) nid = (nid << 8) | data[20 + i];
    h.request_id = rid;
    h.node_id = nid;
    h.payload_len = (static_cast<uint32_t>(data[28]) << 24) | (static_cast<uint32_t>(data[29]) << 16) |
                    (static_cast<uint32_t>(data[30]) << 8) | data[31];
    h.payload_crc = (static_cast<uint32_t>(data[32]) << 24) | (static_cast<uint32_t>(data[33]) << 16) |
                    (static_cast<uint32_t>(data[34]) << 8) | data[35];
    h.header_crc = (static_cast<uint32_t>(data[36]) << 24) | (static_cast<uint32_t>(data[37]) << 16) |
                   (static_cast<uint32_t>(data[38]) << 8) | data[39];
    return h;
}

void send_frame(Socket& sock, MessageType type, uint64_t request_id, uint64_t node_id,
                const std::vector<uint8_t>& payload, uint64_t timeout_ms) {
    if (payload.size() > kFrameMaxPayload) {
        throw_error(ErrKind::Capacity, "cannot send frame larger than maximum payload");
    }

    FrameHeader h;
    h.magic = kFrameMagic;
    h.version = kProtocolVersion;
    h.flags = kFrameFlagChecksum;
    h.type = type;
    h.request_id = request_id;
    h.node_id = node_id;
    h.payload_len = static_cast<uint32_t>(payload.size());
    h.payload_crc = Crc32C::compute(payload.data(), payload.size());

    uint8_t header[kFrameHeaderSize];
    encode_frame_header(h, header);
    h.header_crc = Crc32C::compute(header, kFrameHeaderSize);
    encode_frame_header(h, header);

    const auto deadline = deadline_after(timeout_ms);
    sock.send_all(header, kFrameHeaderSize, remaining_or_timeout(deadline, "frame send timed out"));
    if (!payload.empty()) {
        sock.send_all(payload.data(), payload.size(), remaining_or_timeout(deadline, "frame send timed out"));
    }
}

FrameHeader recv_frame(Socket& sock, std::vector<uint8_t>& payload, uint64_t timeout_ms,
                       uint32_t max_payload, const std::atomic<bool>* running,
                       const std::atomic<bool>* closed) {
    if (max_payload > kFrameMaxPayload) {
        throw_error(ErrKind::InvalidArgument, "frame receive limit exceeds protocol maximum", max_payload);
    }
    payload.clear();
    const bool interruptible = running != nullptr || closed != nullptr;
    auto deadline = interruptible ? SteadyClock::time_point::max() : deadline_after(timeout_ms);
    bool frame_started = !interruptible;
    uint8_t header[kFrameHeaderSize];
    size_t got = 0;
    while (got < kFrameHeaderSize) {
        size_t n = recv_interruptible(sock, header + got, kFrameHeaderSize - got, frame_started,
                                      deadline, running, closed);
        if (n == 0) throw_error(ErrKind::Transport, "connection closed during frame header");
        got += n;
        if (!frame_started) {
            frame_started = true;
            deadline = deadline_after(timeout_ms);
        }
    }

    FrameHeader h = decode_frame_header(header, kFrameHeaderSize);
    // Validate structure before trusting the length field.
    validate_frame_header(h);
    if (h.payload_len > max_payload) {
        throw_error(ErrKind::Protocol, "frame payload exceeds connection limit", h.payload_len);
    }

    // Verify header checksum covers the header itself (with the crc field
    // zeroed, matching how the sender computed it).
    FrameHeader checksum_header = h;
    checksum_header.header_crc = 0;
    uint8_t zeroed[kFrameHeaderSize];
    encode_frame_header(checksum_header, zeroed);
    uint32_t expected_header_crc = Crc32C::compute(zeroed, kFrameHeaderSize);
    if (expected_header_crc != h.header_crc) {
        throw_error(ErrKind::Integrity, "frame header checksum mismatch");
    }

    try {
        payload.resize(h.payload_len);
        got = 0;
        while (got < h.payload_len) {
            size_t n = recv_interruptible(sock, payload.data() + got, h.payload_len - got, true,
                                          deadline, running, closed);
            if (n == 0) throw_error(ErrKind::Transport, "connection closed during frame payload");
            got += n;
        }

        if (h.payload_crc != Crc32C::compute(payload.data(), payload.size())) {
            throw_error(ErrKind::Integrity, "frame payload checksum mismatch");
        }
    } catch (...) {
        payload.clear();
        throw;
    }
    return h;
}

}  // namespace ctf
