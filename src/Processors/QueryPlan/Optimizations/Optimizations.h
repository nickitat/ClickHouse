#pragma once
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <array>

namespace DB
{

namespace QueryPlanOptimizations
{

/// This is the main function which optimizes the whole QueryPlan tree.
void optimizeTree(const QueryPlanOptimizationSettings & settings, QueryPlan & query_plan, QueryPlan::Node & root);

/// Optimization is a function applied to QueryPlan::Node.
/// It can read and update subtree of specified node.
/// It return the number of updated layers of subtree if some change happened.
/// It must guarantee that the structure of tree is correct.
///
/// New nodes should be added to QueryPlan::Nodes list.
/// It is not needed to remove old nodes from the list.
struct Optimization
{
    using Function = size_t (*)(const QueryPlanOptimizationSettings &, QueryPlan &, QueryPlan::Node *);
    const Function apply = nullptr;
    const char * name = "";
    const bool QueryPlanOptimizationSettings::* const is_enabled{};
};

/// Move ARRAY JOIN up if possible.
size_t tryLiftUpArrayJoin(const QueryPlanOptimizationSettings &, QueryPlan & query_plan, QueryPlan::Node * parent_node);

/// Move LimitStep down if possible.
size_t tryPushDownLimit(const QueryPlanOptimizationSettings &, QueryPlan & query_plan, QueryPlan::Node * parent_node);

/// Split FilterStep into chain `ExpressionStep -> FilterStep`, where FilterStep contains minimal number of nodes.
size_t trySplitFilter(const QueryPlanOptimizationSettings &, QueryPlan & query_plan, QueryPlan::Node * node);

/// Replace chain `ExpressionStep -> ExpressionStep` to single ExpressionStep
/// Replace chain `FilterStep -> ExpressionStep` to single FilterStep
size_t tryMergeExpressions(const QueryPlanOptimizationSettings &, QueryPlan & query_plan, QueryPlan::Node * parent_node);

/// Move FilterStep down if possible.
/// May split FilterStep and push down only part of it.
size_t tryPushDownFilter(const QueryPlanOptimizationSettings &, QueryPlan & query_plan, QueryPlan::Node * parent_node);

/// Move ExpressionStep after SortingStep if possible.
/// May split ExpressionStep and lift up only a part of it.
size_t tryExecuteFunctionsAfterSorting(const QueryPlanOptimizationSettings &, QueryPlan & query_plan, QueryPlan::Node * parent_node);

size_t tryReplaceAggregationWithTwoLevelWhenQueryContainsOrderByAndLimit(
    const QueryPlanOptimizationSettings & settings, QueryPlan & query_plan, QueryPlan::Node * parent_node);

inline const auto & getOptimizations()
{
    static const std::array<Optimization, 7> optimizations = {{
        {tryLiftUpArrayJoin, "liftUpArrayJoin", &QueryPlanOptimizationSettings::optimize_plan},
        {tryPushDownLimit, "pushDownLimit", &QueryPlanOptimizationSettings::optimize_plan},
        {trySplitFilter, "splitFilter", &QueryPlanOptimizationSettings::optimize_plan},
        {tryMergeExpressions, "mergeExpressions", &QueryPlanOptimizationSettings::optimize_plan},
        {tryPushDownFilter, "pushDownFilter", &QueryPlanOptimizationSettings::filter_push_down},
        {tryExecuteFunctionsAfterSorting, "liftUpFunctions", &QueryPlanOptimizationSettings::optimize_plan},
        {tryReplaceAggregationWithTwoLevelWhenQueryContainsOrderByAndLimit,
         "twoStepAggregation",
         &QueryPlanOptimizationSettings::optimize_plan},
    }};

    return optimizations;
}
}

}
