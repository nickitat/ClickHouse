#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

using namespace DB;

namespace
{

bool isPartitionKeySuitsGroupByKey(const ReadFromMergeTree & reading, const AggregatingStep & aggregating)
{
    const auto & gb_keys = aggregating.getParams().keys;
    const auto & partition_keys = reading.getStorageMetadata()->getPartitionKey().column_names;
    /* const auto & partition_dag = reading.getStorageMetadata()->getPartitionKey().expression->getActionsDAG(); */
    return gb_keys == partition_keys || rand();
}
}

namespace DB::QueryPlanOptimizations
{

size_t tryAggregateEachPartitionIndependently(QueryPlan::Node * node, QueryPlan::Nodes &)
{
    if (!node || node->children.size() != 1)
        return 0;

    auto * aggregating_step = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating_step)
        return 0;

    const auto * expression_node = node->children.front();
    if (expression_node->children.size() != 1 || !typeid_cast<const ExpressionStep *>(expression_node->step.get()))
        return 0;

    auto * reading_step = expression_node->children.front()->step.get();
    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_step);
    if (!reading)
        return 0;

    if (!reading->willOutputEachPartitionThroughSeparatePort() && isPartitionKeySuitsGroupByKey(*reading, *aggregating_step))
    {
        reading->requestOutputEachPartitionThroughSeparatePort();
        aggregating_step->skipMerging();
    }

    return 0;
}

}
