#include <limits>
#include <Core/SortCursor.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Merges/Algorithms/FinishAggregatingInOrderAlgorithm.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

#include <base/range.h>

using namespace DB;

namespace
{

const AggregatedChunkInfo * getInfoFromChunk(const Chunk & chunk)
{
    const auto & info = chunk.getChunkInfo();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk info was not set for chunk.");

    const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
    if (!agg_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should have AggregatedChunkInfo.");

    return agg_info;
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FinishAggregatingInOrderAlgorithm::State::State(
    const Chunk & chunk, const SortDescriptionWithPositions & desc, Int64 total_bytes_, Int32 bucket_num_)
    : all_columns(chunk.getColumns()), num_rows(chunk.getNumRows()), total_bytes(total_bytes_), bucket_num(bucket_num_)
{
    if (!chunk)
        return;

    sorting_columns.reserve(desc.size());
    for (const auto & column_desc : desc)
        sorting_columns.emplace_back(all_columns[column_desc.column_number].get());
}

FinishAggregatingInOrderAlgorithm::FinishAggregatingInOrderAlgorithm(
    const Block & header_,
    size_t num_inputs_,
    AggregatingTransformParamsPtr params_,
    const SortDescription & description_,
    size_t max_block_size_,
    size_t max_block_bytes_)
    : header(header_)
    , num_inputs(num_inputs_)
    , params(params_)
    , sort_description(description_)
    , max_block_size(max_block_size_)
    , max_block_bytes(max_block_bytes_)
{
    for (const auto & column_description : description_)
        description.emplace_back(column_description, header_.getPositionByName(column_description.column_name));
}

void FinishAggregatingInOrderAlgorithm::initialize(Inputs inputs)
{
    current_inputs = std::move(inputs);
    states.resize(num_inputs);
    for (size_t i = 0; i < num_inputs; ++i)
        consume(current_inputs[i], i);
}

void FinishAggregatingInOrderAlgorithm::consume(Input & input, size_t source_num)
{
    if (!input.chunk.hasRows())
        return;

    const auto & info = input.chunk.getChunkInfo();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk info was not set for chunk in FinishAggregatingInOrderAlgorithm");

    Int64 allocated_bytes = 0;
    Int32 bucket_num = -1;
    bool is_bucket_sorted = false;

    /// Will be set by AggregatingInOrderTransform during local aggregation; will be nullptr during merging on initiator.
    if (const auto * arenas_info = typeid_cast<const ChunkInfoWithAllocatedBytes *>(info.get()))
    {
        allocated_bytes = arenas_info->allocated_bytes;
    }
    else
    {
        const auto * chunk_info = getInfoFromChunk(input.chunk);
        if (chunk_info->is_overflows)
        {
            overflow_chunks.emplace_back(std::move(input.chunk));
            inputs_to_update.push_back(source_num);
            return;
        }

        bucket_num = chunk_info->bucket_num;
        is_bucket_sorted = chunk_info->is_bucket_sorted;
    }

    // ++bucket_nums[bucket_num];
    current_bucket_num = std::min(current_bucket_num, bucket_num);

    /* LOG_DEBUG( */
    /* &Poco::Logger::get("debug"), */
    /* "FinishAggregatingInOrderAlgorithm header:{}, chunk:{}", */
    /* header.dumpStructure(), */
    /* input.chunk.dumpStructure()); */
    Block block = header.cloneWithColumns(input.chunk.detachColumns());
    if (!sort_description.empty() && !is_bucket_sorted)
    {
        sortBlock(block, sort_description);
        LOG_DEBUG(&Poco::Logger::get("debug"), "FinishAggregatingInOrderAlgorithm::consume unsorted bucket");
    }
    Chunk chunk(block.getColumns(), block.rows());

    states[source_num] = State(chunk, description, allocated_bytes, bucket_num);

    /*std::string s;
    for (size_t i = 0; i < num_inputs; ++i)
        s += std::to_string(states[i].bucket_num) + ", ";
    LOG_DEBUG(
        &Poco::Logger::get("debug"),
        "FinishAggregatingInOrderAlgorithm::consume current_bucket_num={}, states={}",
        *bucket_nums.begin(),
        s);*/
}

size_t FinishAggregatingInOrderAlgorithm::getRowToCompareWith(const FinishAggregatingInOrderAlgorithm::State & state) const
{
    const size_t max_block_size_per_input = std::max<size_t>(1, max_block_size / num_inputs);
    return std::min<size_t>(state.current_row + max_block_size_per_input, state.num_rows - 1);
}

IMergingAlgorithm::Status FinishAggregatingInOrderAlgorithm::merge()
{
    if (!inputs_to_update.empty())
    {
        Status status(inputs_to_update.back());
        inputs_to_update.pop_back();
        return status;
    }

    /* if (!chunks.empty() && accumulated_rows >= max_block_size) */
    /* return Status(prepareToMerge()); */

    /// Find the input with smallest last row.
    std::optional<size_t> best_input;
    for (size_t i = 0; i < num_inputs; ++i)
    {
        if (!states[i].isValid(current_bucket_num) || states[i].bucket_num != current_bucket_num)
            continue;

        if (!best_input
            || less(
                states[i].sorting_columns,
                states[*best_input].sorting_columns,
                getRowToCompareWith(states[i]),
                getRowToCompareWith(states[*best_input]),
                description))
        {
            best_input = i;
        }
    }

    /* for (size_t i = 0; i < num_inputs; ++i) */
    /* { */
    /* LOG_DEBUG( */
    /* &Poco::Logger::get("debug"), */
    /* "input={}, bucket_num={}, current_row={}, num_rows={}, to_row={}, isValid={}", */
    /* i, */
    /* states[i].bucket_num, */
    /* states[i].current_row, */
    /* states[i].num_rows, */
    /* states[i].to_row, */
    /* states[i].isValid(current_bucket_num)); */
    /* } */
    /* LOG_DEBUG(&Poco::Logger::get("debug"), "best_input={}", best_input.value_or(-1)); */

    if (!best_input)
    {
        if (!chunks.empty())
        {
            Chunk chunk = prepareToMerge();
            return Status(std::move(chunk), chunks.empty() && overflow_chunks.empty());
        }
        return Status(prepareToMerge(), true);
    }

    /// Chunk at best_input will be aggregated entirely.
    auto & best_state = states[*best_input];
    best_state.to_row = getRowToCompareWith(states[*best_input]) + 1;

    /// Find the positions up to which need to aggregate in other chunks.
    for (size_t i = 0; i < num_inputs; ++i)
    {
        if (!states[i].isValid(current_bucket_num) || i == *best_input)
            continue;

        auto indices = collections::range(states[i].current_row, getRowToCompareWith(states[i]) + 1);
        auto it = std::upper_bound(
            indices.begin(),
            indices.end(),
            getRowToCompareWith(best_state),
            [&](size_t lhs_pos, size_t rhs_pos)
            { return less(best_state.sorting_columns, states[i].sorting_columns, lhs_pos, rhs_pos, description); });

        states[i].to_row = (it == indices.end() ? getRowToCompareWith(states[i]) + 1 : *it);
    }

    addToAggregation();

    /// At least one chunk should be fully (todo: is it always a good idea?) aggregated.
    Status status;
    if (!inputs_to_update.empty())
    {
        status.required_source = inputs_to_update.back();
        inputs_to_update.pop_back();
    }

    /// Do not merge blocks, if there are too few rows or bytes.
    if (accumulated_rows >= max_block_size /*|| accumulated_bytes >= max_block_bytes*/)
        status.chunk = prepareToMerge();

    return status;
}

Chunk FinishAggregatingInOrderAlgorithm::prepareToMerge()
{
    /// todo: fixme
    /* accumulated_rows = 0; */
    /* accumulated_bytes = 0; */

    if (accumulated_rows > 1e5)
        LOG_DEBUG(&Poco::Logger::get("debug"), "current_bucket_num={}, accumulated_rows={}", current_bucket_num, accumulated_rows);

    auto info = std::make_shared<ChunksToMerge>();
    if (!chunks.empty())
    {
        const auto bucket_num = getInfoFromChunk(chunks.front())->bucket_num;
        size_t i = 0;
        while (i < chunks.size() && getInfoFromChunk(chunks[i])->bucket_num == bucket_num)
        {
            /* LOG_DEBUG(&Poco::Logger::get("debug"), "input={}, current_bucket_num={}", i, chunks[i].dumpStructure()); */
            accumulated_rows -= chunks[i].getNumRows();
            accumulated_bytes -= chunks[i].bytes();
            ++i;
            /*--bucket_nums[bucket_num];
            if (bucket_nums[bucket_num] == 0)
                bucket_nums.erase(bucket_num);
            else if (bucket_nums[bucket_num] < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "bug :( {} {}", bucket_num, bucket_nums[bucket_num]);*/
        }

        std::vector<Chunk> new_chunks;
        new_chunks.reserve(chunks.size() - i);
        new_chunks.insert(new_chunks.end(), std::move_iterator(chunks.begin() + i), std::move_iterator(chunks.end()));
        chunks.erase(chunks.begin() + i, chunks.end());

        info->chunks = std::make_unique<Chunks>(std::move(chunks));
        info->chunk_num = chunk_num++;
        info->bucket_num = bucket_num;

        chunks = std::move(new_chunks);
    }
    else
    {
        if (!overflow_chunks.empty())
        {
            info->chunks = std::make_unique<Chunks>(std::move(overflow_chunks));
            info->is_overflows = true;
        }
        else
            info->chunks = std::make_unique<Chunks>();
    }

    Chunk chunk;
    chunk.setChunkInfo(std::move(info));
    return chunk;
}

void FinishAggregatingInOrderAlgorithm::addToAggregation()
{
    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & state = states[i];
        if (!state.isValid(current_bucket_num))
            continue;

        const size_t current_rows = state.to_row - state.current_row;
        if (current_rows == state.num_rows)
        {
            chunks.emplace_back(state.all_columns, current_rows);
        }
        else
        {
            Columns new_columns;
            new_columns.reserve(state.all_columns.size());
            for (const auto & column : state.all_columns)
                new_columns.emplace_back(column->cut(state.current_row, current_rows));

            chunks.emplace_back(std::move(new_columns), current_rows);
        }

        auto chunk_info = std::make_shared<AggregatedChunkInfo>(false, current_bucket_num, true, chunk_num++);
        chunks.back().setChunkInfo(std::move(chunk_info));
        states[i].current_row = states[i].to_row;

        /// We assume that sizes in bytes of rows are almost the same.
        accumulated_bytes += static_cast<size_t>(static_cast<double>(states[i].total_bytes) * current_rows / states[i].num_rows);
        accumulated_rows += current_rows;

        if (!states[i].isValid(current_bucket_num) && states[i].bucket_num <= current_bucket_num)
            inputs_to_update.push_back(i);
    }

    /// Let's update current_bucket_num
    current_bucket_num = std::numeric_limits<Int32>::max();
    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & state = states[i];
        if (state.current_row < state.num_rows)
            current_bucket_num = std::min(current_bucket_num, state.bucket_num);
    }
}

}
