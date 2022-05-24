#pragma once
#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

/// Create NullSource with specified structure.
class ReadNothingStep : public ISourceStep
{
public:
    explicit ReadNothingStep(Block output_header);

    ReadNothingStep(const ReadNothingStep &) = default;

    String getName() const override { return "ReadNothing"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    std::unique_ptr<IQueryPlanStep> clone() const override { return std::make_unique<ReadNothingStep>(*this); }
};

}
