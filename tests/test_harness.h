#pragma once

#include <cstdio>
#include <exception>
#include <functional>
#include <string>
#include <vector>

#include "context_fabric/core/error.h"

namespace ctf_test {

// ---------------------------------------------------------------------------
// Lightweight project-owned test harness.
// ---------------------------------------------------------------------------
struct TestCase {
    std::string name;
    std::function<void()> fn;
};

inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> r;
    return r;
}

struct Register {
    Register(const std::string& name, std::function<void()> fn) {
        registry().push_back({name, std::move(fn)});
    }
};

struct Failure {
    std::string message;
};

inline int& failures() {
    static int n = 0;
    return n;
}

inline int& checks() {
    static int n = 0;
    return n;
}

}  // namespace ctf_test

#define CF_TEST(name)                                                                              \
    static void cf_test_fn_##name();                                                              \
    static ::ctf_test::Register cf_reg_##name(#name, cf_test_fn_##name);                          \
    static void cf_test_fn_##name()

#define CF_CHECK(cond)                                                                             \
    do {                                                                                           \
        ++::ctf_test::checks();                                                                    \
        if (!(cond)) {                                                                             \
            ++::ctf_test::failures();                                                              \
            std::printf("    FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                        \
        }                                                                                          \
    } while (0)

#define CF_REQUIRE(cond)                                                                           \
    do {                                                                                           \
        ++::ctf_test::checks();                                                                    \
        if (!(cond)) {                                                                             \
            ++::ctf_test::failures();                                                              \
            std::printf("    FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                        \
            return;                                                                                \
        }                                                                                          \
    } while (0)

#define CF_CHECK_THROWS(expr, ekind)                                                                             \
    do {                                                                                                        \
        ++::ctf_test::checks();                                                                                 \
        bool caught = false;                                                                                    \
        bool failure_reported = false;                                                                          \
        try {                                                                                                   \
            (void)(expr);                                                                                       \
        } catch (const ctf::CfError& e) {                                                                       \
            caught = (e.kind() == (ekind));                                                                     \
            if (!caught) {                                                                                      \
                ++::ctf_test::failures();                                                                       \
                failure_reported = true;                                                                        \
                std::printf("    FAIL %s:%d: threw wrong kind %s\n", __FILE__, __LINE__,                        \
                            ctf::err_kind_name(e.kind()));                                                      \
            }                                                                                                   \
        } catch (...) {                                                                                         \
            ++::ctf_test::failures();                                                                           \
            failure_reported = true;                                                                            \
            std::printf("    FAIL %s:%d: threw non-CfError\n", __FILE__, __LINE__);                             \
        }                                                                                                       \
        if (!caught && !failure_reported) {                                                                     \
            ++::ctf_test::failures();                                                                           \
            std::printf("    FAIL %s:%d: expected throw of %s\n", __FILE__, __LINE__,                           \
                        ctf::err_kind_name(ekind));                                                             \
        }                                                                                                       \
    } while (0)

#define CF_CHECK_NOTHROW(expr)                                                                     \
    do {                                                                                           \
        ++::ctf_test::checks();                                                                    \
        try {                                                                                      \
            (void)(expr);                                                                          \
        } catch (const std::exception& e) {                                                        \
            ++::ctf_test::failures();                                                              \
            std::printf("    FAIL %s:%d: unexpected throw: %s\n", __FILE__, __LINE__, e.what());   \
        } catch (...) {                                                                            \
            ++::ctf_test::failures();                                                              \
            std::printf("    FAIL %s:%d: unexpected unknown throw\n", __FILE__, __LINE__);         \
        }                                                                                          \
    } while (0)

#define CF_TEST_MAIN()                                                                             \
    int cf_test_run(int argc, char** argv) {                                                       \
        std::vector<std::string> filter;                                                           \
        for (int i = 1; i < argc; ++i) filter.emplace_back(argv[i]);                               \
        int run = 0;                                                                               \
        for (const auto& t : ::ctf_test::registry()) {                                             \
            bool sel = true;                                                                       \
            if (!filter.empty()) {                                                                 \
                sel = false;                                                                       \
                for (const auto& f : filter) {                                                     \
                    if (t.name.find(f) != std::string::npos) sel = true;                           \
                }                                                                                  \
            }                                                                                      \
            if (!sel) continue;                                                                    \
            ++run;                                                                                 \
            std::printf("[ RUN     ] %s\n", t.name.c_str());                                       \
            std::fflush(stdout);                                                                   \
            int before = ::ctf_test::failures();                                                   \
            try {                                                                                    \
                t.fn();                                                                              \
            } catch (const std::exception& e) {                                                      \
                ++::ctf_test::failures();                                                            \
                std::printf("    UNCAUGHT EXCEPTION: %s\n", e.what());                               \
            } catch (...) {                                                                          \
                ++::ctf_test::failures();                                                            \
                std::printf("    UNCAUGHT UNKNOWN EXCEPTION\n");                                     \
            }                                                                                        \
            bool ok = ::ctf_test::failures() == before;                                            \
            std::printf("[ %s ] %s\n", ok ? "      OK" : "  FAILED", t.name.c_str());              \
        }                                                                                          \
        std::printf("\n%d tests run, %d checks, %d failures\n", run, ::ctf_test::checks(),         \
                    ::ctf_test::failures());                                                       \
        if (!filter.empty() && run == 0) {                                                        \
            std::printf("no tests matched the requested filters\n");                            \
            return 2;                                                                            \
        }                                                                                         \
        return ::ctf_test::failures() == 0 ? 0 : 1;                                                \
    }
