#pragma once

#include <Core/SortDescription.h>
#include <Interpreters/sortBlock.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/AggregatingTransform.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace detail
{

    inline const AggregatedChunkInfo * getInfoFromChunk(const Chunk & chunk)
    {
        const auto & info = chunk.getChunkInfo();
        if (!info)
            throw Exception("Chunk info was not set for chunk.", ErrorCodes::LOGICAL_ERROR);

        const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
        if (!agg_info)
            throw Exception("Chunk should have AggregatedChunkInfo.", ErrorCodes::LOGICAL_ERROR);

        return agg_info;
    }
}


///
class ChooseMergingAlgorithmTransform : public IProcessor
{
public:
    explicit ChooseMergingAlgorithmTransform(const Block & header_, size_t num_inputs_)
        : IProcessor(InputPorts(num_inputs_, header_), {}), num_inputs(num_inputs_), read_chunks(num_inputs_)
    {
    }

    String getName() const override { return "ChooseMergingAlgorithmTransform"; }


    void work() override { }

    IProcessor::Status prepare() override
    {
        /// Read first time from each input to understand if we have two-level aggregation.
        if (!read_from_all_inputs)
        {
            readFromAllInputs();
            if (!read_from_all_inputs)
                return Status::NeedData;
        }

        /// Check if merging processors were already created.
        if (outputs.empty())
            createProcessors();

        if (!processors.empty())
            return IProcessor::Status::ExpandPipeline;

        if (outputs.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "outputs.empty() == true");

        auto in = inputs.begin();
        auto out = outputs.begin();

        bool need_data = false;

        for (size_t i = 0; i < num_inputs; ++i, ++in, ++out)
        {
            if (in->isFinished())
                continue;

            if (out->isFinished())
                in->close();

            if (!out->canPush())
                continue;

            in->setNeeded();
            if (!in->hasData())
            {
                need_data = true;
                continue;
            }

            if (!read_chunks[i].empty())
            {
                out->push(std::move(read_chunks[i]));
                read_chunks[i] = Chunk{};
            }
            else
            {
                auto chunk = in->pull();
                out->push(std::move(chunk));
            }
        }

        return need_data ? Status::NeedData : Status::Finished;
    }


private:
    void readFromAllInputs()
    {
        auto in = inputs.begin();
        auto out = outputs.begin();
        read_from_all_inputs = true;

        for (size_t i = 0; i < num_inputs; ++i, ++in, ++out)
        {
            if (in->isFinished())
                continue;

            if (read_from_input[i])
                continue;

            in->setNeeded();

            if (!in->hasData())
            {
                read_from_all_inputs = false;
                continue;
            }

            auto chunk = in->pull();
            read_from_input[i] = true;
            processChunk(std::move(chunk), i);
        }
    }

    void processChunk(Chunk chunk, size_t input)
    {
        if (!chunk.hasRows())
            return;

        const auto * info = detail::getInfoFromChunk(chunk);
        Int32 bucket = info->bucket_num;
        bool is_overflows = info->is_overflows;

        if (!is_overflows && bucket == -1)
            some_input_has_single_level_chunks = true;

        if (info->is_bucket_sorted)
            some_input_has_sorted_chunk = true;

        read_chunks[input] = std::move(chunk);
    }

    void createProcessors() { }

    size_t num_inputs;

    bool some_input_has_single_level_chunks = false;
    bool some_input_has_sorted_chunk = false;

    ///
    Processors processors;

    ///
    Chunks read_chunks;

    bool read_from_all_inputs = false;
    std::vector<bool> read_from_input;
};


/// Has several inputs and single output.
/// Read from inputs merged buckets with aggregated data, sort them by bucket number and block number.
/// Presumption: inputs return chunks with increasing bucket and block number, there is at most one chunk with the given bucket and block number.
class SortingAggregatedForMemoryBoundMergingTransform : public IProcessor
{
public:
    explicit SortingAggregatedForMemoryBoundMergingTransform(const Block & header_, size_t num_inputs_, SortDescription)
        : IProcessor(InputPorts(num_inputs_, header_), {header_})
        , header(header_)
        , num_inputs(num_inputs_)
        , last_chunk_id(num_inputs, {std::numeric_limits<Int32>::min(), 0})
        , is_input_finished(num_inputs, false)
    {
    }

    String getName() const override { return "SortingAggregatedForMemoryBoundMergingTransform"; }

    Status prepare() override
    {
        auto & output = outputs.front();

        if (output.isFinished())
        {
            for (auto & input : inputs)
                input.close();

            return Status::Finished;
        }

        if (!output.canPush())
        {
            for (auto & input : inputs)
                input.setNotNeeded();

            return Status::PortFull;
        }

        /// Push if have chunk that is the next in order
        bool pushed_to_output = tryPushChunk();

        bool need_data = false;
        bool all_finished = true;

        /// Try read new chunk
        auto in = inputs.begin();
        for (size_t input_num = 0; input_num < num_inputs; ++input_num, ++in)
        {
            if (in->isFinished())
            {
                is_input_finished[input_num] = true;
                continue;
            }

            /// We want to keep not more than `num_inputs` chunks in memory (and there will be only a single chunk with the given (bucket_id, chunk_num)).
            const bool bucket_from_this_input_still_in_memory = chunks.contains(last_chunk_id[input_num]);
            if (bucket_from_this_input_still_in_memory)
            {
                all_finished = false;
                continue;
            }

            in->setNeeded();

            if (!in->hasData())
            {
                need_data = true;
                all_finished = false;
                continue;
            }

            auto chunk = in->pull();
            addChunk(std::move(chunk), input_num);

            if (in->isFinished())
            {
                is_input_finished[input_num] = true;
            }
            else
            {
                /// If chunk was pulled, then we need data from this port.
                need_data = true;
                all_finished = false;
            }
        }

        if (pushed_to_output)
            return Status::PortFull;

        if (tryPushChunk())
            return Status::PortFull;

        if (need_data)
            return Status::NeedData;

        if (!all_finished)
            throw Exception(
                "SortingAggregatedForMemoryBoundMergingTransform has read bucket, but couldn't push it.", ErrorCodes::LOGICAL_ERROR);

        if (overflow_chunk)
        {
            output.push(std::move(overflow_chunk));
            return Status::PortFull;
        }

        output.finish();
        return Status::Finished;
    }

private:
    bool tryPushChunk()
    {
        auto & output = outputs.front();

        if (chunks.empty())
            return false;

        /// Chunk with min id
        auto it = chunks.begin();
        auto current_chunk_id = it->first;

        /// Check if it is actually next in order
        for (size_t input = 0; input < num_inputs; ++input)
            if (!is_input_finished[input] && last_chunk_id[input] < current_chunk_id)
                return false;

        LOG_DEBUG(
            &Poco::Logger::get("debug"),
            "SortingAggregatedForMemoryBoundMergingTransform pushing chunk with bucket_id={}, chunk_num={}",
            current_chunk_id.bucket_id,
            current_chunk_id.chunk_num);

        output.push(std::move(it->second));
        chunks.erase(it);
        return true;
    }

    void addChunk(Chunk chunk, size_t from_input)
    {
        if (!chunk.hasRows())
            return;

        const auto & info = chunk.getChunkInfo();
        if (!info)
            throw Exception(
                "Chunk info was not set for chunk in SortingAggregatedForMemoryBoundMergingTransform.", ErrorCodes::LOGICAL_ERROR);

        const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
        if (!agg_info)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Chunk should have AggregatedChunkInfo in SortingAggregatedForMemoryBoundMergingTransform.");

        Int32 bucket_id = agg_info->bucket_num;
        bool is_overflows = agg_info->is_overflows;
        UInt64 chunk_num = agg_info->chunk_num;

        LOG_DEBUG(
            &Poco::Logger::get("debug"),
            "SortingAggregatedForMemoryBoundMergingTransform addChunk bucket_id={}, chunk_num={}",
            bucket_id,
            chunk_num);

        if (is_overflows)
            overflow_chunk = std::move(chunk);
        else
        {
            const auto chunk_id = ChunkId{bucket_id, chunk_num};
            if (chunks.contains(chunk_id))
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "SortingAggregatedForMemoryBoundMergingTransform already got bucket with bucket_id={}, chunk_num={}",
                    bucket_id,
                    chunk_num);
            }

            chunks[chunk_id] = std::move(chunk);
            last_chunk_id[from_input] = chunk_id;
        }
    }

    struct ChunkId
    {
        Int32 bucket_id; // -1 for single-level HT, or 0..255 for two-level
        UInt64 chunk_num;

        bool operator<(const ChunkId & other) const
        {
            return std::make_pair(bucket_id, chunk_num) < std::make_pair(other.bucket_id, other.chunk_num);
        }
    };

    Block header;
    size_t num_inputs;

    std::vector<ChunkId> last_chunk_id;
    std::vector<bool> is_input_finished;
    std::map<ChunkId, Chunk> chunks;
    Chunk overflow_chunk;
};

}
