#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Set.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Exception.h>
#include "Core/Field.h"
#include "Interpreters/PreparedSets.h"
#include "Processors/QueryPlan/LimitStep.h"

using namespace DB;

namespace
{
SetPtr createSet(ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    auto size_limits = SizeLimits{settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode};
    return std::make_shared<Set>(size_limits, /* fill_set_elements */ false, settings.transform_null_in);
}

bool update(QueryPlan::Node & node)
{
    if (auto * step = typeid_cast<AggregatingStep *>(node.step.get()))
    {
        auto aggregator_params = step->getParams();
        aggregator_params.aggregates.clear();
        aggregator_params.aggregates_size = 0;
        step->setParams(std::move(aggregator_params));
        step->updateInputStream(node.step->getInputStreams().front());
        return true;
    }
    if (node.children.empty())
        return false;
    bool agg_found = false;
    for (auto & child : node.children)
        agg_found |= update(*child);
    if (agg_found)
    {
        LOG_DEBUG(&Poco::Logger::get("debug"), "step {}", node.step->getName());
        if (auto * transforming_step = dynamic_cast<ITransformingStep *>(node.step.get()))
            transforming_step->updateInputStream(node.children.front()->step->getOutputStream());
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "");

        if (auto * expression_step = typeid_cast<ExpressionStep *>(node.step.get()))
        {
            ActionsDAGPtr actions = expression_step->getExpression();
            const auto input_stream = expression_step->getInputStreams().front();
            const auto description = expression_step->getStepDescription();
            node.step = std::make_unique<ExpressionStep>(input_stream, std::move(actions));
            node.step->setStepDescription(description);
        }
    }
    return agg_found;
}

void addCreatingSetsStep(QueryPlan & plan, QueryPlan subquery_plan, SetPtr set, ContextPtr context)
{
    auto subquery = SubqueryForSet{std::make_unique<QueryPlan>(std::move(subquery_plan)), set, /* table */ nullptr};
    SubqueriesForSets subqueries;
    subqueries["superset"] = std::move(subquery);
    addCreatingSetsStep(plan, std::move(subqueries), /* network_limits */ {}, context);
}

void addSubqueryWithPreaggregation(QueryPlan & plan, const QueryPlan::Node & root, SetPtr set, ContextPtr context)
{
    auto cloned_plan = plan.cloneSubtree(root);
    update(cloned_plan.getNodes().back());
    ::addCreatingSetsStep(plan, std::move(cloned_plan), set, context);
}

ASTPtr buildTupleOfGroupByKeys(const Aggregator::Params & aggregator_params)
{
    ASTPtr group_by_columns = std::make_shared<ASTExpressionList>();
    for (const auto & col : aggregator_params.keys)
        group_by_columns->children.emplace_back(std::make_shared<ASTIdentifier>(aggregator_params.src_header.getNames()[col]));
    return makeASTFunction("tuple", group_by_columns->children);
}

const ActionsDAG::Node & addTupleFunction(ActionsDAGPtr & actions, const Aggregator::Params & aggregator_params, ContextPtr context)
{
    auto group_by_keys_as_tuple = buildTupleOfGroupByKeys(aggregator_params);
    auto syntax_result = TreeRewriter(context).analyze(group_by_keys_as_tuple, aggregator_params.src_header.getNamesAndTypesList());
    actions = ExpressionAnalyzer(group_by_keys_as_tuple, syntax_result, context).getActionsDAG(false);
    return actions->findInIndex(group_by_keys_as_tuple->getColumnName());
}

const ActionsDAG::Node & addColumnSet(ActionsDAGPtr & actions, SetPtr set)
{
    static size_t set_index = 0;
    const auto column_name = fmt::format("_set_for_group_by_optimization_{}", set_index);
    auto column = ColumnWithTypeAndName(ColumnSet::create(1, set)->getPtr(), std::make_shared<DataTypeSet>(), column_name);
    return actions->addColumn(column);
}

const ActionsDAG::Node & addInFunction(
    ActionsDAGPtr & actions, const ActionsDAG::Node & group_by_keys_tuple_node, const ActionsDAG::Node & set_node, ContextPtr context)
{
    ActionsDAG::NodeRawConstPtrs args;
    args.emplace_back(&group_by_keys_tuple_node);
    args.emplace_back(&set_node);
    auto in_function = FunctionFactory::instance().get("in", context);
    const auto & in_function_node = actions->addFunction(in_function, args, in_function->getName());
    // Make in() column one of the result comumns
    actions->addOrReplaceInIndex(in_function_node);
    return in_function_node;
}

std::unique_ptr<FilterStep>
buildFilterStep(DataStream input_stream, const Aggregator::Params & aggregator_params, SetPtr set, ContextPtr context)
{
    ActionsDAGPtr actions;

    // 1. Add function tuple(group_by_key_col_1, ... , group_by_key_col_N)
    const auto & group_by_keys_tuple_node = addTupleFunction(actions, aggregator_params, context);

    // 2. Add ColumnSet
    const auto & set_node = addColumnSet(actions, set);

    // 3. Add function in(tuple(...), col_set)
    const auto & in_function_node = addInFunction(actions, group_by_keys_tuple_node, set_node, context);

    // 4. Finally made a FilterStep from actions
    return std::make_unique<FilterStep>(
        std::move(input_stream), std::move(actions), in_function_node.result_name, /* remove_filter_column */ true);
}

bool orderByKeysContainAggregates(const SortDescription & sort_description, const Aggregator::Params & aggregator_params)
{
    for (const auto & col : sort_description)
        if (!aggregator_params.src_header.findByName(col.column_name))
            return true;
    return false;
}
}


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::QueryPlanOptimizations
{

// todo:
// * drop offset (whole limit step in outer plan)
// * support having
// * do not preallocate in outer query
// * in theory, outer reading may be more efficient if we filter by in()
// * should it work with distributed aggregation ?
// * what if some functions applied to the table columns ?
size_t tryReplaceAggregationWithTwoLevelWhenQueryContainsOrderByAndLimit(
    const QueryPlanOptimizationSettings & settings, QueryPlan & plan, QueryPlan::Node * parent_node)
{
    auto context = settings.context;

    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    if (!child_node || child_node->children.size() != 1)
        return 0;

    QueryPlan::Node * grand_child = child_node->children.front();
    if (!grand_child)
        return 0;

    auto & parent_step = parent_node->step;
    auto & grand_child_step = grand_child->step;
    auto * sorting_step = typeid_cast<SortingStep *>(parent_step.get());
    auto * aggregating_step = typeid_cast<AggregatingStep *>(grand_child_step.get());

    if (!sorting_step || !aggregating_step)
        return 0;

    if (orderByKeysContainAggregates(sorting_step->getSortDescription(), aggregating_step->getParams()))
        return 0;

    // Set with group by keys obtined from preaggregation
    auto set = createSet(context);

    // Adds a sequence of steps necessary to obtain the set of resulting group by keys
    addSubqueryWithPreaggregation(plan, *parent_node, set, context);

    // Then we need to build a filter for rows with group by keys value from the resulting set
    auto & filter_by_set_node = plan.getNodes().emplace_back();
    filter_by_set_node.step = buildFilterStep(aggregating_step->getInputStreams().front(), aggregating_step->getParams(), set, context);

    // Aggregating -> ...
    std::swap(filter_by_set_node.children, grand_child->children);
    grand_child->children = {&filter_by_set_node};
    aggregating_step->updateInputStream(filter_by_set_node.step->getOutputStream());
    // Aggregating -> Filter -> ...

    return 0;
}
}
