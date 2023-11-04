#pragma once

#include <cstdint>
#include <cstring>
#include "base/defines.h"

#ifdef NDEBUG
#    define ALLOCATOR_ASLR 0
#else
#    define ALLOCATOR_ASLR 1
#endif

#include <pcg_random.hpp>
#include <Common/thread_local_rng.h>

#if !defined(OS_DARWIN) && !defined(OS_FREEBSD)
#    include <malloc.h>
#endif

#include <algorithm>
#include <cstdlib>
#include <sys/mman.h>

#include <Core/Defines.h>
#include <base/getPageSize.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>

#include <Common/Allocator_fwd.h>

#include <base/errnoToString.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>


extern const size_t POPULATE_THRESHOLD;

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_ALLOCATE_MEMORY;
extern const int CANNOT_MUNMAP;
extern const int LOGICAL_ERROR;
}

}

///
/// Ad-hoc allocator for cases when we continiously extend one memory region (e.g. `HashTable` buffer).
/// Uses special logic for allocating up to `max_size_for_arena` bytes
/// and after that falls back to `FallbackAllocator` (supposed to be our good old `Allocator`).
///
/// Assumed call sequence:
/// 1. alloc(initial buffer size)
/// 2. any number of realloc(larger size)
/// 3. free(last allocated buffer)
///
template <size_t max_size_for_arena, typename FallbackAllocator = void>
class YAArenaAllocator
{
public:
    /// Allocate memory range.
    /// NOTE: must be called once for each `NewAllocator` instance
    void * alloc(size_t size, size_t alignment = 0)
    {
        /* std::cerr << fmt::format("alloc size={}", size) << std::endl; */
        checkSize(size);
        auto trace = CurrentMemoryTracker::alloc(size);
        ptr = allocNoTrack(size, alignment);
        trace.onAlloc(ptr, size);
        return ptr;
    }

    /// Free memory range.
    /// NOTE: This method must be called only once to free all the memory allocated previously at once.
    void free(void * buf, size_t size)
    {
        chassert(buf == ptr);
        chassert(size == current_size);

        try
        {
            /* std::cerr << fmt::format("free size={}", size) << std::endl; */
            checkSize(size);
            freeNoTrack(buf);
            auto trace = CurrentMemoryTracker::free(size);
            trace.onFree(buf, size);
        }
        catch (...)
        {
            DB::tryLogCurrentException("YAArenaAllocator::free");
            throw;
        }
    }

    /// Enlarge memory range.
    /// Data from old range is moved to the beginning of new range.
    /// Address of memory range could change.
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0)
    {
        /* std::cerr << fmt::format("alloc old_size={}, new_size={}", old_size, new_size) << std::endl; */
        chassert(buf == ptr);
        chassert(old_size == current_size);
        chassert(alignment == 0 || reinterpret_cast<uintptr_t>(ptr) % alignment == 0);

        auto trace_alloc = CurrentMemoryTracker::alloc(new_size - old_size);
        trace_alloc.onAlloc(buf, new_size - old_size);

        checkSize(new_size);

        if (old_size == new_size)
        {
            /// nothing to do.
            /// BTW, it's not possible to change alignment while doing realloc.
        }
        else if (new_size > max_size_for_arena)
        {
            /// TODO:
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "");
        }
        else
        {
            prefaultPages(reinterpret_cast<char *>(ptr) + current_size, new_size - current_size);
        }

        current_size = new_size;
        return ptr;
    }

protected:
    static constexpr size_t getStackThreshold() { return 0; }

private:
    void * ptr = nullptr;
    size_t current_size = 0;

    void * allocNoTrack(size_t size, size_t alignment)
    {
        chassert(ptr == nullptr);
        chassert(current_size == 0);

        checkSize(size);

        if (alignment)
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "");

        if (size > max_size_for_arena)
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "");

        ptr = static_cast<char *>(mmap(nullptr, max_size_for_arena, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
        current_size = size;

        prefaultPages(ptr, size);

        return ptr;
    }

    void freeNoTrack(void * buf)
    {
        chassert(buf == ptr);

        /// TODO: create one allocator instance per whole TwoLevelHashTable, because `munmap` performs horrible.
        /// At least we will do `max_threads` calls to `munmap` instead of `max_threads * 256`
        /* if (0 != munmap(buf, max_size_for_arena)) */
        /*     DB::throwFromErrno(fmt::format("Allocator: Cannot munmap {}.", ReadableSize(current_size)), DB::ErrorCodes::CANNOT_MUNMAP); */
    }

    void checkSize(size_t size)
    {
        /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
        if (size >= 0x8000000000000000ULL)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to allocator. It indicates an error.", size);
    }

    /// Address passed to madvise is required to be aligned to the page boundary.
    auto adjustToPageSize(void * buf, size_t len, size_t page_size)
    {
        const uintptr_t address_numeric = reinterpret_cast<uintptr_t>(buf);
        const size_t next_page_start = ((address_numeric + page_size - 1) / page_size) * page_size;
        return std::make_pair(reinterpret_cast<void *>(next_page_start), len - (next_page_start - address_numeric));
    }

    void prefaultPages([[maybe_unused]] void * buf_, [[maybe_unused]] size_t len_)
    {
        static const size_t page_size = ::getPageSize();
        if (len_ < page_size) /// Rounded address should be still within [buf, buf + len).
            return;

        auto [buf, len] = adjustToPageSize(buf_, len_, page_size);

#if defined(MADV_POPULATE_WRITE)
        if (auto res = ::madvise(buf, len, MADV_POPULATE_WRITE); res < 0)
            LOG_TRACE(
                LogFrequencyLimiter(&Poco::Logger::get("Allocator"), 1),
                "Attempt to populate pages failed: {} (EINVAL is expected for kernels < 5.14)",
                errnoToString(res));
#endif

        if (len_ >= 2 * 1024 * 1024)
            if (auto res = ::madvise(buf, len, MADV_HUGEPAGE); res < 0)
                LOG_TRACE(LogFrequencyLimiter(&Poco::Logger::get("Allocator"), 1), "Request for huge pages failed: {}", errnoToString(res));
    }
};
