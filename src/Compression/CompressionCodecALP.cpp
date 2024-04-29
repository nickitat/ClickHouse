#include <bit>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <alp/compressor.hpp>
#include <alp/decompressor.hpp>
#include <base/unaligned.h>
#include <Common/Exception.h>
#include "base/types.h"

#include <libdivide.h>
#include <boost/integer/common_factor.hpp>
#include <libdivide-config.h>

#include <alp.hpp>

namespace DB
{

class CompressionCodecALP : public ICompressionCodec
{
public:
    explicit CompressionCodecALP();

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    virtual bool isCompression() const override { return true; }

    virtual bool isGenericCompression() const override { return false; }

    virtual bool isFloatingPointTimeSeriesCodec() const override { return true; }

    virtual bool isExperimental() const override { return true; }
};


namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
extern const int BAD_ARGUMENTS;
}

CompressionCodecALP::CompressionCodecALP()
{
    setCodecDescription("ALP", {});
}

UInt32 CompressionCodecALP::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return sizeof(size_t) + uncompressed_size * 8;
}

uint8_t CompressionCodecALP::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::ALP);
}

void CompressionCodecALP::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/true);
}

UInt32 CompressionCodecALP::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if (reinterpret_cast<uintptr_t>(source) % sizeof(double))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "doCompressData source ({}) % sizeof(double)", reinterpret_cast<uintptr_t>(source));
    if (reinterpret_cast<uintptr_t>(dest) % sizeof(double))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "doCompressData dest ({}) % sizeof(double)", reinterpret_cast<uintptr_t>(dest));

    if (source_size % sizeof(double))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "source_size ({}) % sizeof(double)", source_size);
    const size_t values = source_size / sizeof(double);
    unalignedStore<size_t>(dest, values);
    dest += sizeof(size_t);

    auto compressor = std::make_unique<alp::AlpCompressor>();
    std::vector<double> in(values);
    for (size_t i = 0; i < values; ++i)
    {
        /* memcpy(in.data() + i, source + i * sizeof(double), sizeof(double)); */
        in[i] = unalignedLoad<double>(source + i * sizeof(double));
        if (in[i] < 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "in[i] < 0: {}", in[i]);
    }
    compressor->compress(in.data(), values, reinterpret_cast<uint8_t *>(dest));
    return sizeof(size_t) + static_cast<UInt32>(compressor->get_size());
}

void CompressionCodecALP::doDecompressData(const char * source, UInt32, char * dest, UInt32 uncompressed_size) const
{
    if (reinterpret_cast<uintptr_t>(source) % sizeof(double))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "doDecompressData source ({}) % sizeof(double)", reinterpret_cast<uintptr_t>(source));
    if (reinterpret_cast<uintptr_t>(dest) % sizeof(double))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "doDecompressData dest ({}) % sizeof(double)", reinterpret_cast<uintptr_t>(dest));

    const auto values = unalignedLoad<size_t>(source);
    if (uncompressed_size != values * sizeof(double))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "uncompressed_size ({}) != values ({}) * sizeof(double)", uncompressed_size, values);
    source += sizeof(size_t);

    std::vector<double> res(alp::AlpApiUtils::align_value<size_t, alp::config::VECTOR_SIZE>(values));
    auto decompressor = std::make_unique<alp::AlpDecompressor>();
    decompressor->decompress(reinterpret_cast<uint8_t *>(const_cast<char *>(source)), values, res.data());
    memcpy(dest, res.data(), values * 8);
}

void registerCodecALP(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::ALP);
    auto codec_builder = [&](const ASTPtr &) -> CompressionCodecPtr { return std::make_shared<CompressionCodecALP>(); };
    factory.registerCompressionCodec("ALP", method_code, codec_builder);
}

}
