// #include <Interpreters/ActionsDAG.h>
#include <Core/SortDescription.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace
{

const DB::DataStream & getChildOutputStream(DB::QueryPlan::Node & node)
{
    if (node.children.size() != 1)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Node \"{}\" is expected to have only one child.", node.step->getName());
    return node.children.front()->step->getOutputStream();
}

bool aggregatingInOrderIsApplicable(const DB::Aggregator::Params & /*aggregator_params*/, const DB::SortDescription & /*sort_description*/)
{
    /*NameSet sort_columns;
    for (const auto & col : sorting_step->getSortDescription())
        sort_columns.insert(col.column_name);*/
    return true;
}
}

namespace DB::QueryPlanOptimizations
{

size_t tryReplaceAggregationWithAggregationInOrderInCaseOfOrderByWithLimit(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/)
{
    if (parent_node->children.size() != 1)
        return 0;

    LOG_DEBUG(&Poco::Logger::get("debug"), "pncs={}", parent_node->children.size());
    QueryPlan::Node * intermediate_node = parent_node->children.front();
    if (!intermediate_node || intermediate_node->children.size() != 1)
        return 0;

    LOG_DEBUG(&Poco::Logger::get("debug"), "incs={}", intermediate_node->children.size());
    QueryPlan::Node * child_node = intermediate_node->children.front();
    if (!child_node)
        return 0;

    auto & parent_step = parent_node->step;
    auto & intermediate_step = intermediate_node->step;
    auto & child_step = child_node->step;
    LOG_DEBUG(
        &Poco::Logger::get("debug"),
        "{} {} {}",
        static_cast<void *>(parent_step.get()),
        static_cast<void *>(intermediate_step.get()),
        static_cast<void *>(child_step.get()));
    auto * sorting_step = typeid_cast<SortingStep *>(parent_step.get());
    auto * expression_step = typeid_cast<ExpressionStep *>(intermediate_step.get());
    auto * aggregating_step = typeid_cast<AggregatingStep *>(child_step.get());

    if (!expression_step)
        return 0;

    LOG_DEBUG(&Poco::Logger::get("debug"), "!expression_step->getExpression()->trivial()={}", !expression_step->getExpression()->trivial());
    if (!expression_step->getExpression()->trivial())
        return 0;

    // limitPushDown optimization will happen somewhen and then we will jump into the business
    if (!sorting_step || !aggregating_step || !sorting_step->hasLimit())
        return 0;

    LOG_DEBUG(&Poco::Logger::get("debug"), "!sorting_step->hasLimit()={}", !sorting_step->hasLimit());

    // now let's check that AggregatingInOrder is applicable
    if (!aggregatingInOrderIsApplicable(aggregating_step->getParams(), sorting_step->getSortDescription()))
        return 0;

    // Sorting (parent_node) -> ExpressionStep (intermediate_step) -> AggregatingStep (child_node)
    /*auto & node_with_needed = nodes.emplace_back();
    std::swap(node_with_needed.children, child_node->children);
    child_node->children = {&node_with_needed};

    node_with_needed.step = std::make_unique<ExpressionStep>(getChildOutputStream(node_with_needed), std::move(needed_for_sorting));
    node_with_needed.step->setStepDescription(child_step->getStepDescription());*/

    std::swap(parent_step, child_step);
    // AggregatingStep (parent_node) -> ExpressionStep (intermediate_step) -> Sorting (child_node)

    sorting_step->updateInputStream(getChildOutputStream(*child_node));
    auto input_header = sorting_step->getInputStreams().at(0).header;
    sorting_step->updateOutputStream(std::move(input_header));
    sorting_step->updateLimit(0);

    expression_step->updateInputStream(sorting_step->getOutputStream(), false);

    /*struct InputOrderInfo
    {
        SortDescription order_key_fixed_prefix_descr;
        SortDescription order_key_prefix_descr;
        int direction;
        UInt64 limit;

        InputOrderInfo(
            const SortDescription & order_key_fixed_prefix_descr_,
            const SortDescription & order_key_prefix_descr_,
            int direction_,
            UInt64 limit_)
            : order_key_fixed_prefix_descr(order_key_fixed_prefix_descr_)
            , order_key_prefix_descr(order_key_prefix_descr_)
            , direction(direction_)
            , limit(limit_)
        {
        }

        bool operator==(const InputOrderInfo &) const = default;
    };*/

    auto group_by_info = std::make_shared<InputOrderInfo>(sorting_step->getSortDescription(), sorting_step->getSortDescription(), 1, 0);
    aggregating_step->updateGroupByInfo(group_by_info, sorting_step->getSortDescription());
    aggregating_step->updateInputStream(expression_step->getOutputStream());

    /*auto description = parent_step->getStepDescription();
    parent_step = std::make_unique<DB::ExpressionStep>(child_step->getOutputStream(), std::move(unneeded_for_sorting));
    parent_step->setStepDescription(description + " [lifted up part]");*/

    return 3;
}
}
