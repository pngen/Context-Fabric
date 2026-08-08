#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace ctf {

// ---------------------------------------------------------------------------
// Small strict argument parser for the CLI.
// ---------------------------------------------------------------------------
class Args {
public:
    Args(int argc, char** argv) {
        for (int i = 1; i < argc; ++i) tokens_.emplace_back(argv[i]);
    }

    // Get the next positional token ("" if none).
    [[nodiscard]] std::string next() {
        if (pos_ >= tokens_.size()) return "";
        return tokens_[pos_++];
    }
    [[nodiscard]] bool has_next() const noexcept { return pos_ < tokens_.size(); }

    // Fetch the value for --name (or --name value). Returns nullopt if absent.
    [[nodiscard]] std::optional<std::string> get(const std::string& name) const {
        for (size_t i = 0; i + 1 < tokens_.size(); ++i) {
            if (tokens_[i] == "--" + name) return tokens_[i + 1];
        }
        return std::nullopt;
    }

    // Fetch the value for a flag that may be written as --name=value.
    [[nodiscard]] std::optional<std::string> get_eq(const std::string& name) const {
        for (const auto& t : tokens_) {
            if (t.rfind("--" + name + "=", 0) == 0) return t.substr(name.size() + 3);
        }
        return std::nullopt;
    }

    [[nodiscard]] std::optional<std::string> value(const std::string& name) const {
        auto a = get(name);
        if (a) return a;
        return get_eq(name);
    }

    [[nodiscard]] bool has(const std::string& name) const {
        for (const auto& t : tokens_) {
            if (t == "--" + name) return true;
        }
        return false;
    }

    [[nodiscard]] std::optional<uint64_t> u64(const std::string& name) const {
        auto v = value(name);
        if (!v) return std::nullopt;
        return parse_u64(*v);
    }
    [[nodiscard]] std::optional<int64_t> i64(const std::string& name) const {
        auto v = value(name);
        if (!v) return std::nullopt;
        return parse_i64(*v);
    }
    [[nodiscard]] std::optional<uint16_t> u16(const std::string& name) const {
        auto v = u64(name);
        if (!v || *v > 65535) return std::nullopt;
        return static_cast<uint16_t>(*v);
    }

    [[nodiscard]] const std::vector<std::string>& tokens() const noexcept { return tokens_; }

    static std::optional<uint64_t> parse_u64(const std::string& s) {
        if (s.empty()) return std::nullopt;
        uint64_t v = 0;
        for (char c : s) {
            if (c < '0' || c > '9') return std::nullopt;
            v = v * 10 + static_cast<uint64_t>(c - '0');
        }
        return v;
    }
    static std::optional<int64_t> parse_i64(const std::string& s) {
        if (s.empty()) return std::nullopt;
        bool neg = s[0] == '-';
        auto v = parse_u64(neg ? s.substr(1) : s);
        if (!v) return std::nullopt;
        return neg ? -static_cast<int64_t>(*v) : static_cast<int64_t>(*v);
    }

private:
    std::vector<std::string> tokens_;
    size_t pos_ = 0;
};

// ---------------------------------------------------------------------------
// CLI entry point. Returns an exit code.
// ---------------------------------------------------------------------------
int cli_main(int argc, char** argv);

}  // namespace ctf