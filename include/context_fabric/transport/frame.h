#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/core/object_id.h"
#include "context_fabric/core/generation.h"
#include "context_fabric/core/node.h"
#include "context_fabric/transport/socket.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Wire frame.
//
//   offset  size  field
//   0       4     magic (0x43465241 "CFRA")
//   4       2     protocol version
//   6       2     flags
//   8       2     message type (see protocol/messages.h)
//   10      2     reserved
//   12      8     request id
//   20      8     node id (sender)
//   28      4     payload length
//   32      4     payload crc32c
//   36      4     header crc32c (over bytes 0..36)
//   40      N     payload
//
// All integers big-endian. The header checksum covers the entire fixed header
// so no length field can be modified in transit undetected. Every frame is
// strictly bounds-checked on receive; lengths are never trusted from the wire.
// ---------------------------------------------------------------------------
inline constexpr uint32_t kFrameHeaderSize = 40;
inline constexpr uint32_t kFrameMaxPayload = 256u * 1024 * 1024;  // 256 MiB hard cap
inline constexpr uint16_t kFrameFlagChecksum = 1u << 0;

using MessageType = uint16_t;

struct FrameHeader {
    uint32_t magic = 0;
    uint16_t version = 0;
    uint16_t flags = 0;
    MessageType type = 0;
    uint16_t reserved = 0;
    uint64_t request_id = 0;
    uint64_t node_id = 0;
    uint32_t payload_len = 0;
    uint32_t payload_crc = 0;
    uint32_t header_crc = 0;
};

// Validates the structural fields of a decoded header. The raw header checksum
// is verified by recv_frame(), where the original bytes are available. Throws
// CfError(Protocol) on any structural violation.
void validate_frame_header(const FrameHeader& h);

// Serialize a header into the 40-byte wire form.
void encode_frame_header(const FrameHeader& h, uint8_t* out);

// Decode a header from 40 bytes. Throws CfError(Protocol) if the buffer is
// not exactly the header size or the magic is wrong.
FrameHeader decode_frame_header(const uint8_t* data, size_t len);

// Send one complete frame (header + payload) over `sock` with an operation
// timeout. Never assumes a single send() delivers the whole frame.
void send_frame(Socket& sock, MessageType type, uint64_t request_id, uint64_t node_id,
                const std::vector<uint8_t>& payload, uint64_t timeout_ms);

// Receive one complete frame. Returns the header; the payload is placed in
// `payload`. Handles partial reads. Throws CfError(Protocol) for malformed
// frames and CfError(Timeout) if the header does not arrive in time.
// When `running`/`closed` are supplied, an idle stream is polled until a frame
// starts or cancellation is requested. Once its first byte arrives, the whole
// frame must complete within `timeout_ms`; partial progress is never discarded.
FrameHeader recv_frame(Socket& sock, std::vector<uint8_t>& payload, uint64_t timeout_ms,
                       uint32_t max_payload = kFrameMaxPayload,
                       const std::atomic<bool>* running = nullptr,
                       const std::atomic<bool>* closed = nullptr);

}  // namespace ctf
