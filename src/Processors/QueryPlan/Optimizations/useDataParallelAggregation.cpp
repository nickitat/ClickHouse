#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <__algorithm/ranges_any_of.h>

#include <stack>
#include <unordered_map>

using namespace DB;

namespace
{

using NodeSet = std::unordered_set<const ActionsDAG::Node *>;

struct Frame
{
    const ActionsDAG::Node * node = nullptr;
    size_t next_child = 0;
};

auto print_node = [](const ActionsDAG::Node * node_)
{
    String children;
    for (const auto & child : node_->children)
        children += fmt::format("{}, ", static_cast<const void *>(child));
    LOG_DEBUG(
        &Poco::Logger::get("debug"),
        "current node {} {} {} {}",
        static_cast<const void *>(node_),
        node_->result_name,
        node_->type,
        children);
};

bool isInjectiveFunction(const ActionsDAG::Node * node)
{
    if (node->function_base->isInjective({}))
        return true;

    size_t fixed_args = 0;
    for (const auto & child : node->children)
        if (child->type == ActionsDAG::ActionType::COLUMN)
            ++fixed_args;
    static const std::vector<String> injective = {"plus", "minus"};
    return (fixed_args + 1 >= node->children.size()) && (std::ranges::find(injective, node->function_base->getName()) != injective.end());
}

void removeInjectiveColumnsFromResultsRecursively(
    const ActionsDAGPtr & actions, const ActionsDAG::Node * cur_node, NodeSet & irreducible, NodeSet & visited, bool & invalid)
{
    if (visited.contains(cur_node))
        return;
    visited.insert(cur_node);

    print_node(cur_node);

    switch (cur_node->type)
    {
        case ActionsDAG::ActionType::ALIAS:
            assert(cur_node->children.size() == 1);
            removeInjectiveColumnsFromResultsRecursively(actions, cur_node->children.at(0), irreducible, visited, invalid);
            break;
        case ActionsDAG::ActionType::ARRAY_JOIN:
            invalid = true;
            break;
        case ActionsDAG::ActionType::COLUMN:
            irreducible.insert(cur_node);
            break;
        case ActionsDAG::ActionType::FUNCTION:
            LOG_DEBUG(&Poco::Logger::get("debug"), "{} {}", __LINE__, isInjectiveFunction(cur_node));
            if (!isInjectiveFunction(cur_node))
                irreducible.insert(cur_node);
            else
                for (const auto & child : cur_node->children)
                    removeInjectiveColumnsFromResultsRecursively(actions, child, irreducible, visited, invalid);
            break;
        case ActionsDAG::ActionType::INPUT:
            irreducible.insert(cur_node);
            break;
    }
}

/// Removes injective functions recursively from result columns until it is no longer possible.
bool removeInjectiveColumnsFromResultsRecursively(ActionsDAGPtr actions)
{
    NodeSet irreducible;
    NodeSet visited;
    bool invalid = false;

    for (const auto & node : actions->getOutputs())
        removeInjectiveColumnsFromResultsRecursively(actions, node, irreducible, visited, invalid);

    LOG_DEBUG(&Poco::Logger::get("debug"), "irreducible nodes:");
    for (const auto & node : irreducible)
        print_node(node);

    return invalid;
}

bool isPartitionKeySuitsGroupByKey(const ReadFromMergeTree & reading, ActionsDAGPtr group_by_actions, const AggregatingStep & aggregating)
{
    /// 0. Partition key columns should be a subset of group by key columns.
    /// 1. Optimization is applicable if partition by expression is a deterministic function of col1, ..., coln and group by keys are injective functions of some of col1, ..., coln.

    if (aggregating.isGroupingSets() || group_by_actions->hasStatefulFunctions())
        return false;

    /// Check that PK columns is a subset of GBK columns.
    const auto partition_actions = reading.getStorageMetadata()->getPartitionKey().expression->getActionsDAG().clone();
    const auto & gb_keys = group_by_actions->getRequiredColumnsNames();

    LOG_DEBUG(&Poco::Logger::get("debug"), "group by req cols: {}", fmt::join(gb_keys, ", "));
    LOG_DEBUG(&Poco::Logger::get("debug"), "partition by cols: {}", fmt::join(partition_actions->getRequiredColumnsNames(), ", "));

    for (const auto & col : partition_actions->getRequiredColumnsNames())
        if (std::ranges::find(gb_keys, col) == gb_keys.end())
            return false;

    /* /// PK is always a deterministic expression without constants. No need to check. */

    /* /// We will work only with subexpression that depends on partition key columns. */
    LOG_DEBUG(&Poco::Logger::get("debug"), "group by actions before:\n{}", group_by_actions->dumpDAG());
    LOG_DEBUG(&Poco::Logger::get("debug"), "partition by actions before:\n{}", partition_actions->dumpDAG());

    /// For cases like `partition by col + group by col+1` or `partition by hash(col) + group by hash(col)`
    if (removeInjectiveColumnsFromResultsRecursively(group_by_actions))
        return false;

    LOG_DEBUG(&Poco::Logger::get("debug"), "group by actions after:\n{}", group_by_actions->dumpDAG());
    LOG_DEBUG(&Poco::Logger::get("debug"), "partition by actions after:\n{}", partition_actions->dumpDAG());

    const auto & pkey_nodes = reading.getStorageMetadata()->getPartitionKey().expression->getActionsDAG().getNodes();
    if (!pkey_nodes.empty())
    {
        const auto & func_node = pkey_nodes.back();
        LOG_DEBUG(&Poco::Logger::get("debug"), "{} {} {}", func_node.type, func_node.is_deterministic, func_node.children.size());
        if (func_node.type == ActionsDAG::ActionType::FUNCTION && func_node.function->getName() == "modulo"
            && func_node.children.size() == 2)
        {
            const auto & arg1 = func_node.children.front();
            const auto & arg2 = func_node.children.back();
            LOG_DEBUG(&Poco::Logger::get("debug"), "{} {} {}", arg1->type, arg1->result_name, arg2->type);
            if (arg1->type == ActionsDAG::ActionType::INPUT && arg1->result_name == gb_keys[0]
                && arg2->type == ActionsDAG::ActionType::COLUMN && typeid_cast<const ColumnConst *>(arg2->column.get()))
                return true;
        }
    }

    return false;
}
}

namespace DB::QueryPlanOptimizations
{

size_t tryAggregatePartitionsIndependently(QueryPlan::Node * node, QueryPlan::Nodes &)
{
    if (!node || node->children.size() != 1)
        return 0;

    auto * aggregating_step = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating_step)
        return 0;

    const auto * expression_node = node->children.front();
    const auto * expression_step = typeid_cast<const ExpressionStep *>(expression_node->step.get());
    if (expression_node->children.size() != 1 || !expression_step)
        return 0;

    auto * reading_step = expression_node->children.front()->step.get();

    if (const auto * filter = typeid_cast<const FilterStep *>(reading_step))
    {
        const auto * filter_node = expression_node->children.front();
        if (filter_node->children.size() != 1 || !filter_node->children.front()->step)
            return 0;
        reading_step = filter_node->children.front()->step.get();
    }

    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_step);
    if (!reading)
        return 0;

    if (!reading->willOutputEachPartitionThroughSeparatePort()
        && isPartitionKeySuitsGroupByKey(*reading, expression_step->getExpression()->clone(), *aggregating_step))
    {
        if (reading->requestOutputEachPartitionThroughSeparatePort())
            aggregating_step->skipMerging();
    }

    return 0;
}

}
