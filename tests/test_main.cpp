#include "test_harness.h"

#include <exception>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <psapi.h>
#include <dbghelp.h>
#pragma comment(lib, "dbghelp.lib")
#pragma comment(lib, "psapi.lib")

LONG WINAPI cf_crash_handler(EXCEPTION_POINTERS* ep) {
    std::fprintf(stderr, "CRASH: exception code 0x%08lX at 0x%llX\n",
                 static_cast<unsigned long>(ep->ExceptionRecord->ExceptionCode),
                 reinterpret_cast<unsigned long long>(ep->ExceptionRecord->ExceptionAddress));
    if (ep->ContextRecord) {
        std::fprintf(stderr, "  RIP=0x%llX RSP=0x%llX RBP=0x%llX RAX=0x%llX RBX=0x%llX RCX=0x%llX RDX=0x%llX\n",
                     static_cast<unsigned long long>(ep->ContextRecord->Rip),
                     static_cast<unsigned long long>(ep->ContextRecord->Rsp),
                     static_cast<unsigned long long>(ep->ContextRecord->Rbp),
                     static_cast<unsigned long long>(ep->ContextRecord->Rax),
                     static_cast<unsigned long long>(ep->ContextRecord->Rbx),
                     static_cast<unsigned long long>(ep->ContextRecord->Rcx),
                     static_cast<unsigned long long>(ep->ContextRecord->Rdx));
    }
    HMODULE mods[64];
    DWORD needed = 0;
    if (EnumProcessModulesEx(GetCurrentProcess(), mods, sizeof(mods), &needed, LIST_MODULES_ALL)) {
        DWORD count = needed / sizeof(HMODULE);
        for (DWORD i = 0; i < count; ++i) {
            uintptr_t base = reinterpret_cast<uintptr_t>(mods[i]);
            uintptr_t addr = reinterpret_cast<uintptr_t>(ep->ExceptionRecord->ExceptionAddress);
            if (addr >= base && addr < base + 0x100000) {
                char name[MAX_PATH];
                if (GetModuleBaseNameA(GetCurrentProcess(), mods[i], name, MAX_PATH)) {
                    std::fprintf(stderr, "  in module %s at offset 0x%llX\n", name,
                                 static_cast<unsigned long long>(addr - base));
                }
            }
        }
    }
    std::fflush(stderr);
    {
        HANDLE h = CreateFileA("C:\\Users\\pauln\\AppData\\Local\\Temp\\opencode\\crash.dmp", GENERIC_WRITE, 0,
                               nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (h != INVALID_HANDLE_VALUE) {
            MINIDUMP_EXCEPTION_INFORMATION mei;
            mei.ThreadId = GetCurrentThreadId();
            mei.ExceptionPointers = ep;
            mei.ClientPointers = TRUE;
            MiniDumpWriteDump(GetCurrentProcess(), GetCurrentProcessId(), h, MiniDumpWithFullMemory, &mei, nullptr,
                              nullptr);
            CloseHandle(h);
        }
    }
    return EXCEPTION_EXECUTE_HANDLER;
}
#endif

CF_TEST_MAIN()

int main(int argc, char** argv) {
#if defined(_WIN32)
    SetUnhandledExceptionFilter(cf_crash_handler);
    _set_abort_behavior(0, _WRITE_ABORT_MSG);
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_CHECK_ALWAYS_DF | _CRTDBG_CHECK_CRT_DF);
    _set_invalid_parameter_handler([](const wchar_t* expr, const wchar_t*, const wchar_t*, unsigned, uintptr_t) {
        std::fprintf(stderr, "INVALID_PARAMETER: %ls\n", expr ? expr : L"?");
        std::fflush(stderr);
    });
    _set_purecall_handler([]() {
        std::fprintf(stderr, "PURECALL\n");
        std::fflush(stderr);
        std::abort();
    });
#endif
    std::set_terminate([]() {
        std::fprintf(stderr, "TERMINATE called\n");
        std::fflush(stderr);
        void* frames[32];
        USHORT n = CaptureStackBackTrace(0, 32, frames, nullptr);
        for (USHORT i = 0; i < n; ++i) {
            std::fprintf(stderr, "  frame %u: 0x%llX\n", i,
                         reinterpret_cast<unsigned long long>(frames[i]));
        }
        std::fflush(stderr);
        std::abort();
    });
    return cf_test_run(argc, argv);
}