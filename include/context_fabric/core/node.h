#pragma once

#include <cstdint>
#include <string>

#include "context_fabric/core/types.h"

namespace ctf {

// ---------------------------------------------------------------------------
// NodeId: identity of a node process in the fabric. Unique per node.
// ---------------------------------------------------------------------------
class NodeId {
public:
    NodeId() = default;
    explicit NodeId(uint64_t v) noexcept : value_(v) {}

    static NodeId deterministic(const std::string& name);
    static NodeId random();

    [[nodiscard]] bool valid() const noexcept { return value_ != 0; }
    [[nodiscard]] uint64_t value() const noexcept { return value_; }
    [[nodiscard]] std::string to_string() const;

    friend bool operator==(const NodeId& a, const NodeId& b) noexcept { return a.value_ == b.value_; }
    friend bool operator!=(const NodeId& a, const NodeId& b) noexcept { return a.value_ != b.value_; }
    friend bool operator<(const NodeId& a, const NodeId& b) noexcept { return a.value_ < b.value_; }

    // Human-readable label for the node (e.g. "node-a"). Not an identity.
    std::string label;

private:
    uint64_t value_ = 0;
};

// ---------------------------------------------------------------------------
// NodeAddress: how to reach a node's data-plane listener.
// Host may be an IP literal or hostname. The protocol does not assume
// loopback; the local multi-process fabric simply uses 127.0.0.1.
// ---------------------------------------------------------------------------
struct NodeAddress {
    std::string host;
    uint16_t port = 0;

    [[nodiscard]] bool valid() const noexcept { return !host.empty() && port != 0; }
    [[nodiscard]] std::string to_string() const;
};

}  // namespace ctf

namespace std {
template <>
struct hash<ctf::NodeId> {
    size_t operator()(const ctf::NodeId& n) const noexcept {
        return static_cast<size_t>(n.value());
    }
};
}  // namespace std