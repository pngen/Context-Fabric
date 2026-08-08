#include "context_fabric/core/serializer.h"

#include <cstring>

namespace ctf {

static_assert(sizeof(uint8_t) == 1);

void Writer::u8(uint8_t v) { buf_.push_back(v); }
void Writer::u16(uint16_t v) {
    buf_.push_back(static_cast<uint8_t>(v >> 8));
    buf_.push_back(static_cast<uint8_t>(v & 0xFF));
}
void Writer::u32(uint32_t v) {
    buf_.push_back(static_cast<uint8_t>(v >> 24));
    buf_.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf_.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf_.push_back(static_cast<uint8_t>(v & 0xFF));
}
void Writer::u64(uint64_t v) {
    for (int i = 7; i >= 0; --i) buf_.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
}
void Writer::i64(int64_t v) { u64(static_cast<uint64_t>(v)); }
void Writer::f64(double v) {
    uint64_t bits;
    static_assert(sizeof(bits) == sizeof(v));
    std::memcpy(&bits, &v, sizeof(bits));
    u64(bits);
}
void Writer::append(const uint8_t* p, size_t n) { buf_.insert(buf_.end(), p, p + n); }
void Writer::bytes(const uint8_t* p, size_t n) {
    if (n > 0xFFFFFFFFu) throw_error(ErrKind::Capacity, "serialized field exceeds u32 length limit");
    u32(static_cast<uint32_t>(n));
    append(p, n);
}
void Writer::str(const std::string& s) {
    bytes(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}
void Writer::str_vector(const std::vector<std::string>& v) {
    if (v.size() > 0xFFFFFFFFu) throw_error(ErrKind::Capacity, "string vector too large");
    u32(static_cast<uint32_t>(v.size()));
    for (const auto& s : v) str(s);
}

[[noreturn]] void Reader::overrun(const char* what) {
    throw_error(ErrKind::Protocol, std::string("reader overrun while reading ") + what);
}

uint8_t Reader::u8() {
    if (p_ >= end_) overrun("u8");
    return *p_++;
}
uint16_t Reader::u16() {
    if (end_ - p_ < 2) overrun("u16");
    uint16_t v = static_cast<uint16_t>((static_cast<uint16_t>(p_[0]) << 8) | p_[1]);
    p_ += 2;
    return v;
}
uint32_t Reader::u32() {
    if (end_ - p_ < 4) overrun("u32");
    uint32_t v = (static_cast<uint32_t>(p_[0]) << 24) | (static_cast<uint32_t>(p_[1]) << 16) |
                 (static_cast<uint32_t>(p_[2]) << 8) | p_[3];
    p_ += 4;
    return v;
}
uint64_t Reader::u64() {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | u8();
    return v;
}
int64_t Reader::i64() { return static_cast<int64_t>(u64()); }
double Reader::f64() {
    uint64_t bits = u64();
    double v;
    std::memcpy(&v, &bits, sizeof(v));
    return v;
}
void Reader::bytes(uint8_t* out, size_t n) {
    if (remaining() < n) overrun("bytes");
    std::memcpy(out, p_, n);
    p_ += n;
}
std::vector<uint8_t> Reader::bytes(size_t n) {
    if (remaining() < n) overrun("bytes");
    std::vector<uint8_t> v(p_, p_ + n);
    p_ += n;
    return v;
}
std::string Reader::str() {
    uint32_t n = u32();
    if (remaining() < n) overrun("string");
    std::string s(reinterpret_cast<const char*>(p_), n);
    p_ += n;
    return s;
}
std::vector<std::string> Reader::str_vector_strict() {
    uint32_t count = u32();
    if (count > 100000) throw_error(ErrKind::Protocol, "string vector count unreasonable");
    std::vector<std::string> v;
    v.reserve(count);
    for (uint32_t i = 0; i < count; ++i) v.push_back(str());
    return v;
}

}  // namespace ctf