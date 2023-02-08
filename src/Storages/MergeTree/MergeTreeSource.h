#pragma once
#include <Processors/ISource.h>

namespace DB
{

class IMergeTreeSelectAlgorithm;
using MergeTreeSelectAlgorithmPtr = std::unique_ptr<IMergeTreeSelectAlgorithm>;

struct ChunkAndProgress;

struct ChunksAndProgress
{
    using Chunks = std::list<Chunk>;

    Chunks chunks;
    size_t num_read_rows = 0;
    size_t num_read_bytes = 0;
};

class MergeTreeSource final : public ISource
{
public:
    explicit MergeTreeSource(MergeTreeSelectAlgorithmPtr algorithm_);
    ~MergeTreeSource() override;

    std::string getName() const override;

    Status prepare() override;

#if defined(OS_LINUX)
    int schedule() override;
#endif

protected:
    std::optional<Chunk> tryGenerate() override;

    void onCancel() override;

private:
    MergeTreeSelectAlgorithmPtr algorithm;

    ChunksAndProgress::Chunks chunks;

#if defined(OS_LINUX)
    struct AsyncReadingState;
    std::unique_ptr<AsyncReadingState> async_reading_state;
#endif

    std::optional<Chunk> reportProgress(ChunkAndProgress hunk);
    std::optional<Chunk> reportProgress(ChunksAndProgress hunk);
};

}
