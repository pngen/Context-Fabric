#include "context_fabric/core/json.h"

#include <cstdio>
#include <cmath>

namespace ctf {

static void json_escape(std::string& out, const char* s) {
    out.push_back('"');
    for (const char* p = s; *p; ++p) {
        switch (*p) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(*p) < 0x20) {
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned>(*p));
                    out += buf;
                } else {
                    out.push_back(*p);
                }
        }
    }
    out.push_back('"');
}

std::string json_escape_string(const std::string& s) {
    std::string out;
    json_escape(out, s.c_str());
    return out;
}

JsonObject& JsonObject::field(const char* name, const char* value) {
    std::string v;
    json_escape(v, value);
    entries_.push_back({name, std::move(v)});
    return *this;
}

JsonObject& JsonObject::field(const char* name, bool value) {
    entries_.push_back({name, value ? "true" : "false"});
    return *this;
}

JsonObject& JsonObject::field(const char* name, int64_t value) {
    entries_.push_back({name, std::to_string(value)});
    return *this;
}

JsonObject& JsonObject::field(const char* name, uint64_t value) {
    entries_.push_back({name, std::to_string(value)});
    return *this;
}

JsonObject& JsonObject::field(const char* name, double value, int precision) {
    char buf[64];
    if (std::isnan(value) || std::isinf(value)) {
        snprintf(buf, sizeof(buf), "0");
    } else {
        snprintf(buf, sizeof(buf), "%.*f", precision, value);
    }
    entries_.push_back({name, buf});
    return *this;
}

void JsonObject::write_to(std::string& out) const {
    out.push_back('{');
    for (size_t i = 0; i < entries_.size(); ++i) {
        if (i != 0) out.push_back(',');
        out.push_back('"');
        out += entries_[i].key;
        out += "\":";
        out += entries_[i].value_json;
    }
    out.push_back('}');
}

}  // namespace ctf