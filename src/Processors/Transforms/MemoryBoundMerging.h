#pragma once

#include <algorithm>
#include <limits>
#include <Core/SortDescription.h>
#include <Interpreters/sortBlock.h>
#include <Processors/IProcessor.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <QueryPipeline/Pipe.h>

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


/// Has several inputs and single output.
/// Read from inputs merged buckets with aggregated data, sort them by bucket number and block number.
/// Presumption: inputs return chunks with increasing bucket and block number, there is at most one chunk with the given bucket and block number.
class SortingAggregatedForMemoryBoundMergingTransform : public IProcessor
{
public:
    explicit SortingAggregatedForMemoryBoundMergingTransform(const Block & header_, size_t num_inputs_)
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SortingAggregatedForMemoryBoundMergingTransform has read bucket, but couldn't push it.");

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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk info was not set for chunk in SortingAggregatedForMemoryBoundMergingTransform.");

        const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
        if (!agg_info)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Chunk should have AggregatedChunkInfo in SortingAggregatedForMemoryBoundMergingTransform.");

        Int32 bucket_id = agg_info->bucket_num;
        bool is_overflows = agg_info->is_overflows;
        UInt64 chunk_num = agg_info->chunk_num;

        /*LOG_DEBUG(
            &Poco::Logger::get("debug"),
            "SortingAggregatedForMemoryBoundMergingTransform addChunk bucket_id={}, chunk_num={}",
            bucket_id,
            chunk_num);*/

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


/// In case of distributed aggregation we don't know which aggregation algorithm a remote node will choose.
/// In theory we could receive all types of buckets for merge: single-level, two-level unsorted, two-level sorted (if external memory bound aggregation took place).
/// This transform analyzes input buckets, chooses merging algorithm and sort unsorted buckets if needed.
class ChooseMergingAlgorithmTransform : public IProcessor
{
public:
    explicit ChooseMergingAlgorithmTransform(
        const Block & header_,
        size_t num_inputs_,
        AggregatingTransformParamsPtr params_,
        size_t temporary_data_merge_threads_,
        SortDescription group_by_sort_description_,
        size_t max_block_bytes_,
        bool memory_bound_merging_enabled_)
        : IProcessor(InputPorts(num_inputs_, header_), {params_->getHeader()})
        , num_inputs(num_inputs_)
        , read_chunks(num_inputs)
        , single_level_chunks(num_inputs)
        , last_bucket_num(num_inputs, std::numeric_limits<Int32>::min())
        , read_from_input(num_inputs, false)
        , converted_chunks(num_inputs)
        , params(params_)
        , temporary_data_merge_threads(temporary_data_merge_threads_)
        , group_by_sort_description(std::move(group_by_sort_description_))
        , max_block_bytes(max_block_bytes_)
        , memory_bound_merging_enabled(memory_bound_merging_enabled_)
    {
    }

    String getName() const override { return "ChooseMergingAlgorithmTransform"; }

    void work() override { }

    void closeAllPorts()
    {
        for (auto & in : inputs)
            in.close();
        for (auto & out : outputs)
            out.finish();
    }

    IProcessor::Status prepare() override
    {
        /// Read first time from each input to understand what kinds of buckets do we have.
        if (!read_from_all_inputs)
        {
            readFromAllInputs();
            if (!read_from_all_inputs)
                return Status::NeedData;

            if (some_input_has_single_level_chunks && some_input_has_two_level_chunks)
                should_read_only_single_level = true;
            else
                for (size_t input = 0; input < num_inputs; ++input)
                    convertSingleLevelToTwoLevelIfNeeded(input);
        }

        /// Check if merging processors were already created.
        if (!processors_created)
            createProcessors();

        if (!processors.empty())
            return IProcessor::Status::ExpandPipeline;

        if (outputs.size() != num_inputs + 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No output ports created");

        bool need_data = false;
        bool pushed_something = false;

        auto & merged_input = inputs.back();
        auto & merged_output = outputs.front();
        if (merged_output.canPush())
        {
            if (merged_input.hasData())
            {
                auto chunk = merged_input.pull();
                merged_output.push(std::move(chunk));
                pushed_something = true;
            }
            else
            {
                merged_input.setNeeded();
                need_data = true;
            }
        }
        else if (merged_output.isFinished())
        {
            closeAllPorts();
            return Status::Finished;
        }
        if (merged_input.isFinished())
        {
            closeAllPorts();
            return Status::Finished;
        }

        /// Output ports (i.e. actual merging transforms) were already created. Here we just forward input chunks to them.
        auto in = inputs.begin();
        auto out = std::next(outputs.begin());

        bool all_finished = true;

        bool all_single_level_finished = true;

        for (size_t i = 0; i < num_inputs; ++i, ++in, ++out)
        {
            /*LOG_DEBUG(
                &Poco::Logger::get("debug"),
                "i={} isFinished={} out->isFinished()={} out->canPush()={} all_single_level_finished={} should_read_only_single_level={}",
                i,
                in->isFinished(),
                out->isFinished(),
                out->canPush(),
                all_single_level_finished,
                should_read_only_single_level);*/

            if (out->isFinished())
            {
                // LOG_DEBUG(&Poco::Logger::get("debug"), "out->isFinished()");
                in->close();
                continue;
            }

            all_finished &= in->isFinished();

            if (!in->isFinished() && !single_level_chunks[i].empty())
                all_single_level_finished = false;

            if (should_read_only_single_level && single_level_chunks[i].empty())
            {
                in->setNotNeeded();
                continue;
            }

            if (!out->canPush())
            {
                in->setNotNeeded();
                // LOG_DEBUG(&Poco::Logger::get("debug"), "i={} !out->canPush()", i);
                continue;
            }

            if (!converted_chunks[i].empty())
            {
                out->push(std::move(converted_chunks[i].back()));
                converted_chunks[i].pop_back();
                pushed_something = true;
                continue;
            }

            if (read_chunks[i])
            {
                out->push(std::move(read_chunks[i]));
                read_chunks[i] = Chunk{};
                pushed_something = true;
                continue;
            }

            if (in->isFinished())
            {
                convertSingleLevelToTwoLevelIfNeeded(i);

                if (!converted_chunks[i].empty())
                {
                    out->push(std::move(converted_chunks[i].back()));
                    converted_chunks[i].pop_back();
                    pushed_something = true;
                }

                if (converted_chunks[i].empty() && !read_chunks[i])
                    out->finish();
                // LOG_DEBUG(&Poco::Logger::get("debug"), "i={} in->isFinished()", i);
                continue;
            }

            // LOG_DEBUG(&Poco::Logger::get("debug"), "i={}", i);

            in->setNeeded();
            if (!in->hasData())
            {
                // LOG_DEBUG(&Poco::Logger::get("debug"), "!in->hasData()");
                need_data = true;
                continue;
            }

            auto chunk = in->pull();
            // if (const auto * info = detail::getInfoFromChunk(chunk); info && info->bucket_num == -1)
            //    throw Exception(ErrorCodes::LOGICAL_ERROR, "single level chunks are not expected on this stage");
            processChunk(std::move(chunk), i);

            if (out->canPush() && read_chunks[i])
            {
                out->push(std::move(read_chunks[i]));
                read_chunks[i] = Chunk{};
                pushed_something = true;
            }
        }

        if (should_read_only_single_level && all_single_level_finished)
            should_read_only_single_level = false;

        if (should_read_only_single_level)
            return Status::NeedData;

        // LOG_DEBUG(&Poco::Logger::get("debug"), "need_data={}, all_finished={}, pushed_something={}", need_data, all_finished, pushed_something);

        bool a = std::all_of(read_chunks.begin(), read_chunks.end(), [](const auto & chunk) { return !chunk; });
        bool b = std::all_of(converted_chunks.begin(), converted_chunks.end(), [](const auto & chunks) { return chunks.empty(); });
        if (all_finished && (merged_input.isFinished() || merged_output.isFinished()) && a && b)
        {
            closeAllPorts();
            return Status::Finished;
        }

        return need_data ? Status::NeedData : pushed_something ? Status::Ready : Status::PortFull;
    }

private:
    void readFromAllInputs()
    {
        auto in = inputs.begin();
        read_from_all_inputs = true;

        for (size_t i = 0; i < num_inputs; ++i, ++in)
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
            processChunk(std::move(chunk), i);
        }
    }

    void processChunk(Chunk chunk, size_t input)
    {
        if (!chunk.hasRows())
            return;

        const auto * info = detail::getInfoFromChunk(chunk);
        const auto bucket_num = info->bucket_num;

        /// todo: think once again about overflow chunk

        // LOG_DEBUG(&Poco::Logger::get("debug"), "processChunk input {} bucket_num {}", input, bucket_num);

        if (!info->is_overflows && info->bucket_num == -1)
            some_input_has_single_level_chunks = true;

        if (info->bucket_num >= 0)
            some_input_has_two_level_chunks = true;

        if (info->is_bucket_sorted)
            some_input_has_sorted_chunk = true;

        if (info->is_overflows)
            LOG_DEBUG(&Poco::Logger::get("debug"), "overflow chunk input {}", input);

        if (bucket_num == -1 && !info->is_overflows)
            single_level_chunks[input].emplace_back(std::move(chunk));
        else
        {
            if (read_chunks[input])
                throw Exception(ErrorCodes::LOGICAL_ERROR, "We already have a buffered chunk for input {}", input);
            read_chunks[input] = std::move(chunk);
        }

        read_from_input[input] = true;
        last_bucket_num[input] = bucket_num;
    }

    void convertSingleLevelToTwoLevelIfNeeded(size_t input)
    {
        if (single_level_chunks[input].empty())
            return;

        // LOG_DEBUG(&Poco::Logger::get("debug"), "convertSingleLevelToTwoLevelIfNeeded input {}", input);

        if (some_input_has_two_level_chunks && some_input_has_single_level_chunks)
        {
            std::map<Int32, Chunk> res;
            for (auto & chunk : single_level_chunks[input])
            {
                const auto & header = getInputs().front().getHeader();
                auto block = header.cloneWithColumns(chunk.detachColumns());
                auto blocks = params->aggregator.convertBlockToTwoLevel(block);

                for (auto & cur_block : blocks)
                {
                    if (!cur_block)
                        continue;

                    auto chunk_info = std::make_shared<AggregatedChunkInfo>(cur_block.info);
                    const auto bucket_num = chunk_info->bucket_num;
                    auto bucket_chunk = Chunk(cur_block.getColumns(), cur_block.rows(), chunk_info);
                    if (res.find(bucket_num) != res.end())
                        res[bucket_num].append(bucket_chunk);
                    else
                        res[bucket_num] = std::move(bucket_chunk);
                }
            }
            for (auto & [bucket_num, chunk] : res)
            {
                const auto & header = getInputs().front().getHeader();
                auto block = header.cloneWithColumns(chunk.detachColumns());

                // if (some_input_has_sorted_chunk)
                //    sortBlock(block, params->params.sort_description);

                chunk.setColumns(block.getColumns(), block.rows());

                // auto chunk_info = std::make_shared<AggregatedChunkInfo>(block.info);
                // auto bucket_chunk = Chunk(block.getColumns(), block.rows(), chunk_info);
                converted_chunks[input].emplace_back(std::move(chunk));
            }
            std::reverse(converted_chunks[input].begin(), converted_chunks[input].end());
        }
        else
        {
            // if (single_level_chunks[input].size() > 1)
            // throw Exception(ErrorCodes::LOGICAL_ERROR, "");
            converted_chunks[input].insert(
                converted_chunks[input].end(),
                std::move_iterator(single_level_chunks[input].begin()),
                std::move_iterator(single_level_chunks[input].end()));
        }

        single_level_chunks[input].clear();
    }

    void createProcessors()
    {
        const auto & header = inputs.front().getHeader();

        if (!memory_bound_merging_enabled || !some_input_has_sorted_chunk)
        {
            Pipe pipe{std::make_shared<GroupingAggregatedTransform>(header, num_inputs, params), true /* allow_have_inputs */};

            if (num_inputs <= 1)
            {
                pipe.addTransform(std::make_shared<MergingAggregatedBucketTransform>(params));
                return;
            }

            pipe.resize(temporary_data_merge_threads);

            pipe.addSimpleTransform([this](const Block &) { return std::make_shared<MergingAggregatedBucketTransform>(params); });

            pipe.addTransform(std::make_shared<SortingAggregatedTransform>(temporary_data_merge_threads, params));

            processors = Pipe::detachProcessors(std::move(pipe));
        }
        else
        {
            auto finish_aggregating = std::make_shared<FinishAggregatingInOrderTransform>(
                header, num_inputs, params, group_by_sort_description, params->params.max_block_size, max_block_bytes);
            Pipe pipe{std::move(finish_aggregating), true /* allow_have_inputs */};

            pipe.resize(temporary_data_merge_threads);

            const auto & required_sort_description = !params->final ? group_by_sort_description : SortDescription{};

            pipe.addSimpleTransform([&](const Block &)
                                    { return std::make_shared<MergingAggregatedBucketTransform>(params, required_sort_description); });

            pipe.addTransform(std::make_shared<SortingAggregatedForMemoryBoundMergingTransform>(pipe.getHeader(), pipe.numOutputPorts()));

            processors = Pipe::detachProcessors(std::move(pipe));
        }

        processors_created = true;
    }

    Processors expandPipeline() override
    {
        if (processors.empty())
            throw Exception("No processors prepared for expandPipeline()", ErrorCodes::LOGICAL_ERROR);

        const auto & header = inputs.front().getHeader();

        /// Connect merging output with our output
        if (outputs.size() != 1 || processors.back()->getOutputs().size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Single output port is expected");

        /// We create new input in the current transform
        inputs.emplace_back(params->getHeader(), this);
        connect(processors.back()->getOutputs().front(), inputs.back(), true);

        /// Connect our outputs with merging inputs
        auto & merging_inputs = processors.front()->getInputs();

        if (merging_inputs.size() != num_inputs)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of inputs don't match");

        for (auto & in : merging_inputs)
        {
            outputs.emplace_back(header, this);
            connect(outputs.back(), in);
        }

        auto ret = std::move(processors);
        processors.clear();
        return ret;
    }

    size_t num_inputs;

    bool some_input_has_single_level_chunks = false;
    bool some_input_has_two_level_chunks = false;
    bool some_input_has_sorted_chunk = false;
    bool processors_created = false;

    ///
    Processors processors;

    ///
    Chunks read_chunks;
    std::vector<Chunks> single_level_chunks;
    std::vector<Int32> last_bucket_num;

    bool should_read_only_single_level = false;

    bool read_from_all_inputs = false;
    std::vector<bool> read_from_input;

    /// if input contained a single-level HT and sme other inputs contained two-level HT,
    /// we will convert single-level tables to two-level and store obtained chunks in this vector.
    std::vector<Chunks> converted_chunks;

    AggregatingTransformParamsPtr params;
    size_t temporary_data_merge_threads;
    SortDescription group_by_sort_description;
    size_t max_block_bytes;
    bool memory_bound_merging_enabled;
};

}
