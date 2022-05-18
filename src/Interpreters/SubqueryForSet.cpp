#include <Interpreters/SubqueryForSet.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/PreparedSets.h>

namespace DB
{

SubqueryForSet::SubqueryForSet() = default;
SubqueryForSet::~SubqueryForSet() = default;
SubqueryForSet::SubqueryForSet(SubqueryForSet &&) noexcept = default;
SubqueryForSet & SubqueryForSet::operator= (SubqueryForSet &&) noexcept = default;

SubqueryForSet::SubqueryForSet(std::unique_ptr<QueryPlan> source_, SetPtr set_, StoragePtr table_)
    : source(std::move(source_)), set(std::move(set_)), table(std::move(table_))
{
}
}
