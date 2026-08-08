#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "context_fabric/core/error.h"

namespace ctf {

// ---------------------------------------------------------------------------
// Deterministic binary writer. All integers are big-endian on the wire, so
// serialized records are byte-stable across platforms. Lengths are explicitly
// bounded; the writer grows its buffer but never more than is asked for.
// ---------------------------------------------------------------------------
class Writer {
public:
    Writer() = default;
    explicit Writer(size_t reserve) { buf_.reserve(reserve); }

    void u8(uint8_t v);
    void u16(uint16_t v);
    void u32(uint32_t v);
    void u64(uint64_t v);
    void i64(int64_t v);
    void f64(double v);
    void bytes(const uint8_t* p, size_t n);
    void str(const std::string& s);                     // u32 len + utf-8 bytes
    void str_vector(const std::vector<std::string>& v); // u32 count + strs
    void raw(const std::vector<uint8_t>& v) { append(v.data(), v.size()); }

    void append(const uint8_t* p, size_t n);
    [[nodiscard]] const std::vector<uint8_t>& data() const noexcept { return buf_; }
    [[nodiscard]] size_t size() const noexcept { return buf_.size(); }

private:
    std::vector<uint8_t> buf_;
};

// ---------------------------------------------------------------------------
// Deterministic binary reader with strict bounds checking on every field.
// Any overrun or malformed length throws CfError(Protocol).
// ---------------------------------------------------------------------------
class Reader {
public:
    Reader(const uint8_t* data, size_t len) : p_(data), end_(data + len) {}

    uint8_t u8();
    uint16_t u16();
    uint32_t u32();
    uint64_t u64();
    int64_t i64();
    double f64();
    void bytes(uint8_t* out, size_t n);
    std::vector<uint8_t> bytes(size_t n);
    std::string str();                       // bounded by remaining length
    std::vector<std::string> str_vector_strict();

    [[nodiscard]] bool at_end() const noexcept { return p_ == end_; }
    [[nodiscard]] size_t remaining() const noexcept {
        return static_cast<size_t>(end_ - p_);
    }

private:
    const uint8_t* p_;
    const uint8_t* end_;

    [[noreturn]] void overrun(const char* what);
};

}  // namespace ctf