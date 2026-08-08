#pragma once

#include <cstdint>
#include <initializer_list>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
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
        for (size_t i = pos_; i + 1 < tokens_.size(); ++i) {
            if (tokens_[i] == "--" + name && tokens_[i + 1].rfind("--", 0) != 0) {
                return tokens_[i + 1];
            }
        }
        return std::nullopt;
    }

    // Fetch the value for a flag that may be written as --name=value.
    [[nodiscard]] std::optional<std::string> get_eq(const std::string& name) const {
        for (size_t i = pos_; i < tokens_.size(); ++i) {
            const auto& t = tokens_[i];
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
        for (size_t i = pos_; i < tokens_.size(); ++i) {
            if (tokens_[i] == "--" + name) return true;
        }
        return false;
    }

    // Strictly validate all tokens remaining after consumed command words.
    // Value options accept either `--name value` or `--name=value`; flags
    // accept only `--name`. Unknown, duplicate, missing-value, and positional
    // tokens are rejected.
    [[nodiscard]] std::optional<std::string> validate_remaining(
        std::initializer_list<std::string_view> value_options,
        std::initializer_list<std::string_view> flag_options = {}) const {
        auto contains = [](std::initializer_list<std::string_view> options, std::string_view name) {
            for (std::string_view option : options) {
                if (option == name) return true;
            }
            return false;
        };

        std::vector<std::string> seen;
        for (size_t i = pos_; i < tokens_.size();) {
            const std::string& token = tokens_[i];
            if (token.rfind("--", 0) != 0 || token.size() == 2) {
                return "unexpected positional argument '" + token + "'";
            }

            size_t equals = token.find('=');
            std::string name = token.substr(2, equals == std::string::npos ? std::string::npos : equals - 2);
            bool is_value = contains(value_options, name);
            bool is_flag = contains(flag_options, name);
            if (!is_value && !is_flag) return "unknown option '--" + name + "'";
            for (const auto& prior : seen) {
                if (prior == name) return "duplicate option '--" + name + "'";
            }
            seen.push_back(name);

            if (equals != std::string::npos) {
                if (!is_value) return "flag '--" + name + "' does not take a value";
                if (equals + 1 == token.size()) return "option '--" + name + "' requires a value";
                ++i;
                continue;
            }
            if (is_flag) {
                ++i;
                continue;
            }
            if (i + 1 >= tokens_.size() || tokens_[i + 1].rfind("--", 0) == 0) {
                return "option '--" + name + "' requires a value";
            }
            i += 2;
        }
        return std::nullopt;
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
            uint64_t digit = static_cast<uint64_t>(c - '0');
            if (v > (std::numeric_limits<uint64_t>::max() - digit) / 10) return std::nullopt;
            v = v * 10 + digit;
        }
        return v;
    }
    static std::optional<int64_t> parse_i64(const std::string& s) {
        if (s.empty()) return std::nullopt;
        bool neg = s[0] == '-';
        std::string magnitude = neg ? s.substr(1) : s;
        auto v = parse_u64(magnitude);
        if (!v) return std::nullopt;
        constexpr uint64_t max_positive = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        if (!neg) {
            if (*v > max_positive) return std::nullopt;
            return static_cast<int64_t>(*v);
        }
        if (*v > max_positive + 1) return std::nullopt;
        if (*v == max_positive + 1) return std::numeric_limits<int64_t>::min();
        return -static_cast<int64_t>(*v);
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
