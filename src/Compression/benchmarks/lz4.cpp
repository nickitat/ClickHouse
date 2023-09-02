#include <benchmark/benchmark.h>

#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/LZ4_decompress_faster.h>
#include <Core/Defines.h>
#include <IO/ReadBufferFromFile.h>

#include <filesystem>
#include <stdexcept>
#include <string_view>
#include <vector>

using namespace DB;

constexpr char TEST_FILE[] = "/home/ubuntu/ClickHouse/WatchID.bin";
constexpr size_t CHECKSUM_LEN = 16;

template <size_t unroll, size_t interleave>
class ColumnFromFile : public benchmark::Fixture
{
public:
    void SetUp(const ::benchmark::State &) override
    {
        const auto data_size = std::filesystem::file_size(TEST_FILE);
        data.resize(data_size);
        DB::ReadBufferFromFile rb(TEST_FILE);
        if (rb.read(data.data(), data.size()) != data.size())
            throw std::logic_error{"File wasn't read completely"};

        /// Extract all blocks
        size_t off = 0;
        while (off < data.size())
        {
            off += CHECKSUM_LEN; // Ignore checksums
            const auto * ptr = data.data() + off;

            CompressedBlock cb;
            cb.compressed_size = ICompressionCodec::readCompressedBlockSize(ptr); // header size included
            cb.data = std::string_view{ptr + COMPRESSED_BLOCK_HEADER_SIZE, ptr + cb.compressed_size};
            cb.uncompressed_size = ICompressionCodec::readDecompressedBlockSize(ptr);
            off += cb.compressed_size;

            if (cb.uncompressed_size > DBMS_DEFAULT_BUFFER_SIZE)
                throw std::logic_error{"Unexpectedly large uncompressed block size"};

            blocks.push_back(std::move(cb));
        }

        dest.resize(DBMS_DEFAULT_BUFFER_SIZE);
    }

    void test(benchmark::State & st)
    {
        LZ4::PerformanceStatistics stats(3); // Method chosen randomly, the point is to fix it to remove one extra source of entropy
        for (auto _ : st)
        {
            for (const auto & block : blocks)
            {
                if (!LZ4::decompress<unroll, interleave>(block.data.data(), dest.data(), block.data.size(), block.uncompressed_size, stats))
                    throw std::logic_error{"Source isn't a valid lz-encoded blob"};
            }
        }
    }

private:
    struct CompressedBlock
    {
        size_t compressed_size;
        size_t uncompressed_size;
        std::string_view data;
    };
    std::vector<CompressedBlock> blocks;

    std::string data;
    std::string dest;
};


#define OK_GOOGLE(unroll, interleave) \
    BENCHMARK_TEMPLATE_DEFINE_F(ColumnFromFile, Unroll##unroll##_Interleave##interleave, unroll, interleave)(benchmark::State & st) \
    { \
        test(st); \
    } \
    BENCHMARK_REGISTER_F(ColumnFromFile, Unroll##unroll##_Interleave##interleave);

OK_GOOGLE(1, 1);
OK_GOOGLE(1, 2);
OK_GOOGLE(1, 4);
OK_GOOGLE(1, 8);
OK_GOOGLE(2, 1);
OK_GOOGLE(2, 2);
OK_GOOGLE(2, 4);
OK_GOOGLE(4, 1);
OK_GOOGLE(4, 2);
OK_GOOGLE(4, 4);
