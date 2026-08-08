#include "context_fabric/core/node.h"

#include <cstdio>
#include <cstring>

#include "context_fabric/core/object_id.h"

namespace ctf {

NodeId NodeId::deterministic(const std::string& name) {
    return NodeId(stable_hash64_str(name));
}

NodeId NodeId::random() {
    ObjectId o = ObjectId::random();
    return NodeId(o.hi() ^ (o.lo() * 0x9E3779B97F4A7C15ull));
}

std::string NodeId::to_string() const {
    char buf[24];
    snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(value_));
    return std::string(buf);
}

std::string NodeAddress::to_string() const {
    char buf[64];
    snprintf(buf, sizeof(buf), "%s:%u", host.c_str(), static_cast<unsigned>(port));
    return std::string(buf);
}

}  // namespace ctf