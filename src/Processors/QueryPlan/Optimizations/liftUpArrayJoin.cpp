#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>

#include <Processors/QueryPlan/SortingStep.h>
#include <base/logger_useful.h>
#include <Poco/Logger.h>

namespace DB::QueryPlanOptimizations
{

size_t tryLiftUpArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;
    auto * expression_step = typeid_cast<ExpressionStep *>(parent.get());
    auto * filter_step = typeid_cast<FilterStep *>(parent.get());
    auto * array_join_step = typeid_cast<ArrayJoinStep *>(child.get());

    if (!(expression_step || filter_step) || !array_join_step)
        return 0;

    const auto & array_join = array_join_step->arrayJoin();
    const auto & expression = expression_step ? expression_step->getExpression()
                                              : filter_step->getExpression();

    auto split_actions = expression->splitActionsBeforeArrayJoin(array_join->columns);

    /// No actions can be moved before ARRAY JOIN.
    if (split_actions.first->trivial())
        return 0;

    auto description = parent->getStepDescription();

    /// All actions was moved before ARRAY JOIN. Swap Expression and ArrayJoin.
    if (split_actions.second->trivial())
    {
        auto expected_header = parent->getOutputStream().header;

        /// Expression/Filter -> ArrayJoin
        std::swap(parent, child);
        /// ArrayJoin -> Expression/Filter

        if (expression_step)
            child = std::make_unique<ExpressionStep>(child_node->children.at(0)->step->getOutputStream(),
                                                     std::move(split_actions.first));
        else
            child = std::make_unique<FilterStep>(child_node->children.at(0)->step->getOutputStream(),
                                                 std::move(split_actions.first),
                                                 filter_step->getFilterColumnName(),
                                                 filter_step->removesFilterColumn());

        child->setStepDescription(std::move(description));

        array_join_step->updateInputStream(child->getOutputStream(), expected_header);
        return 2;
    }

    /// Add new expression step before ARRAY JOIN.
    /// Expression/Filter -> ArrayJoin -> Something
    auto & node = nodes.emplace_back();
    node.children.swap(child_node->children);
    child_node->children.emplace_back(&node);
    /// Expression/Filter -> ArrayJoin -> node -> Something

    node.step = std::make_unique<ExpressionStep>(node.children.at(0)->step->getOutputStream(),
                                                 std::move(split_actions.first));
    node.step->setStepDescription(description);
    array_join_step->updateInputStream(node.step->getOutputStream(), {});

    if (expression_step)
        parent = std::make_unique<ExpressionStep>(array_join_step->getOutputStream(), split_actions.second);
    else
        parent = std::make_unique<FilterStep>(array_join_step->getOutputStream(), split_actions.second,
                                              filter_step->getFilterColumnName(), filter_step->removesFilterColumn());

    parent->setStepDescription(description + " [split]");
    return 3;
}


size_t tryLiftUpFunctions(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;
    auto * parent_sorting_step = typeid_cast<SortingStep *>(parent.get());
    auto * child_expression_step = typeid_cast<ExpressionStep *>(child.get());

    if (!parent_sorting_step || !child_expression_step)
        return 0;

    const auto & expression = child_expression_step->getExpression();
    const auto & sort_columns = parent_sorting_step->getSortDescription();

    for (auto sc : sort_columns)
        LOG_TRACE(&Poco::Logger::get("Optimizer"), "sort_columns: {}", sc.column_name);

    auto split_actions = expression->splitActionsBySortingDescription(sort_columns);
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "source: {}", expression->dumpDAG());
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "first: {}", split_actions.first->dumpDAG());
    LOG_TRACE(&Poco::Logger::get("Optimizer"), "second: {}", split_actions.second->dumpDAG());

    /// No calculations can be postponed.
    if (split_actions.second->trivial())
        return 0;

    auto description = parent->getStepDescription();

    /// All actions was moved before ARRAY JOIN. Swap Expression and ArrayJoin.
    if (split_actions.first->trivial())
    {
        auto expected_header = parent->getOutputStream().header;

        /// Sorting -> Expression
        std::swap(parent, child);
        /// Expression -> Sorting


        // child->setStepDescription(std::move(description));

        // child_sorting_step
        child_expression_step->updateInputStream(child_node->children.at(0)->step->getOutputStream(), expected_header);

        parent = std::make_unique<ExpressionStep>(child->getOutputStream(), std::move(split_actions.second));

        return 2;
    }

    (void)nodes;
    return 0;

    /*
    /// Add new expression step before ARRAY JOIN.
    /// Expression/Filter -> ArrayJoin -> Something
    auto & node = nodes.emplace_back();
    node.children.swap(child_node->children);
    child_node->children.emplace_back(&node);
    /// Expression/Filter -> ArrayJoin -> node -> Something

    node.step = std::make_unique<ExpressionStep>(node.children.at(0)->step->getOutputStream(), std::move(split_actions.first));
    node.step->setStepDescription(description);
    array_join_step->updateInputStream(node.step->getOutputStream(), {});

    if (expression_step)
        parent = std::make_unique<ExpressionStep>(array_join_step->getOutputStream(), split_actions.second);
    else
        parent = std::make_unique<FilterStep>(
            array_join_step->getOutputStream(),
            split_actions.second,
            filter_step->getFilterColumnName(),
            filter_step->removesFilterColumn());

    parent->setStepDescription(description + " [split]");
    return 3;*/
}

}
