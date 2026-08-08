#include "context_fabric/core/json.h"

#include <algorithm>
#include <cstdio>
#include <cmath>
#include <iomanip>
#include <locale>
#include <sstream>
#include <string_view>

namespace ctf {

static bool valid_utf8_sequence(std::string_view s, size_t offset, size_t& length) noexcept {
    const auto byte = [&](size_t i) { return static_cast<unsigned char>(s[offset + i]); };
    const unsigned char lead = byte(0);
    if (lead >= 0xC2 && lead <= 0xDF) {
        length = 2;
    } else if (lead >= 0xE0 && lead <= 0xEF) {
        length = 3;
    } else if (lead >= 0xF0 && lead <= 0xF4) {
        length = 4;
    } else {
        return false;
    }
    if (length > s.size() - offset) return false;
    for (size_t i = 1; i < length; ++i) {
        if ((byte(i) & 0xC0) != 0x80) return false;
    }
    const unsigned char second = byte(1);
    if ((lead == 0xE0 && second < 0xA0) || (lead == 0xED && second > 0x9F) ||
        (lead == 0xF0 && second < 0x90) || (lead == 0xF4 && second > 0x8F)) {
        return false;
    }
    return true;
}

static void append_byte_escape(std::string& out, unsigned char c) {
    char buf[8];
    snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned>(c));
    out += buf;
}

static void json_escape(std::string& out, std::string_view s) {
    out.push_back('"');
    for (size_t offset = 0; offset < s.size();) {
        const char c = s[offset];
        const unsigned char raw = static_cast<unsigned char>(c);
        if (raw >= 0x80) {
            size_t sequence_length = 0;
            if (valid_utf8_sequence(s, offset, sequence_length)) {
                out.append(s.substr(offset, sequence_length));
                offset += sequence_length;
            } else {
                append_byte_escape(out, raw);
                ++offset;
            }
            continue;
        }
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (raw < 0x20) {
                    append_byte_escape(out, raw);
                } else {
                    out.push_back(c);
                }
        }
        ++offset;
    }
    out.push_back('"');
}

std::string json_escape_string(const std::string& s) {
    std::string out;
    json_escape(out, s);
    return out;
}

JsonObject& JsonObject::field(const char* name, const char* value) {
    std::string v;
    json_escape(v, value != nullptr ? std::string_view(value) : std::string_view());
    entries_.push_back({name != nullptr ? name : "", std::move(v)});
    return *this;
}

JsonObject& JsonObject::field(const char* name, const std::string& value) {
    std::string v;
    json_escape(v, value);
    entries_.push_back({name != nullptr ? name : "", std::move(v)});
    return *this;
}

JsonObject& JsonObject::field(const char* name, bool value) {
    entries_.push_back({name != nullptr ? name : "", value ? "true" : "false"});
    return *this;
}

JsonObject& JsonObject::field(const char* name, int64_t value) {
    entries_.push_back({name != nullptr ? name : "", std::to_string(value)});
    return *this;
}

JsonObject& JsonObject::field(const char* name, uint64_t value) {
    entries_.push_back({name != nullptr ? name : "", std::to_string(value)});
    return *this;
}

JsonObject& JsonObject::field(const char* name, double value, int precision) {
    std::string encoded;
    if (std::isnan(value) || std::isinf(value)) {
        encoded = "null";
    } else {
        std::ostringstream stream;
        stream.imbue(std::locale::classic());
        stream << std::fixed << std::setprecision((std::clamp)(precision, 0, 18)) << value;
        encoded = stream.str();
    }
    entries_.push_back({name != nullptr ? name : "", std::move(encoded)});
    return *this;
}

void JsonObject::write_to(std::string& out) const {
    out.push_back('{');
    for (size_t i = 0; i < entries_.size(); ++i) {
        if (i != 0) out.push_back(',');
        json_escape(out, entries_[i].key);
        out.push_back(':');
        out += entries_[i].value_json;
    }
    out.push_back('}');
}

}  // namespace ctf
