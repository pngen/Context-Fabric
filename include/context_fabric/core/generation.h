#pragma once

#include <cstdint>
#include <string>

#include "context_fabric/core/object_id.h"

namespace ctf {

// ---------------------------------------------------------------------------
// ContextGeneration: an explicit version of an object. Every mutation creates
// or advances a generation. At most one writable authoritative generation can
// exist at a time, and every physical copy identifies the exact generation it
// represents. An object is addressable as (ObjectId, Generation).
// ---------------------------------------------------------------------------
class Generation {
public:
    Generation() = default;

    static Generation first() noexcept { return Generation(1); }
    static Generation from_value(uint64_t v) noexcept { return Generation(v); }
    static Generation from_hex(const std::string& hex);

    [[nodiscard]] bool valid() const noexcept { return value_ != 0; }
    [[nodiscard]] uint64_t value() const noexcept { return value_; }

    [[nodiscard]] Generation next() const noexcept { return Generation(value_ + 1); }
    [[nodiscard]] std::string to_hex() const;

    friend bool operator==(const Generation& a, const Generation& b) noexcept {
        return a.value_ == b.value_;
    }
    friend bool operator!=(const Generation& a, const Generation& b) noexcept {
        return a.value_ != b.value_;
    }
    friend bool operator<(const Generation& a, const Generation& b) noexcept {
        return a.value_ < b.value_;
    }
    friend bool operator>(const Generation& a, const Generation& b) noexcept {
        return a.value_ > b.value_;
    }

private:
    explicit Generation(uint64_t v) noexcept : value_(v) {}

    uint64_t value_ = 0;
};

}  // namespace ctf

namespace std {
template <>
struct hash<ctf::Generation> {
    size_t operator()(const ctf::Generation& g) const noexcept {
        return static_cast<size_t>(g.value());
    }
};
}  // namespace std