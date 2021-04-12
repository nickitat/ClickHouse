#pragma once
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <functional>
#include <optional>
using namespace Coordination;
using namespace zkutil;

std::string generateRandomPath(const std::string & prefix, size_t length = 5);

std::string generateRandomData(size_t size);

class IGenerator
{
public:
    virtual void startup(ZooKeeper & /*zookeeper*/) {}
    virtual ZooKeeperRequestPtr generate() = 0;
    virtual void teardown(ZooKeeper & /*zookeeper*/) {}
    virtual ~IGenerator() = default;
};

class CreateRequestGenerator final : public IGenerator
{
public:
    explicit CreateRequestGenerator(
        std::string path_prefix_ = "/",
        std::optional<uint64_t> path_length_ = std::nullopt,
        std::optional<uint64_t> data_size_ = std::nullopt)
        : path_prefix(path_prefix_)
        , path_length(path_length_)
        , data_size(data_size_)
    {}

    ZooKeeperRequestPtr generate() override;

private:
    std::string path_prefix;
    std::optional<uint64_t> path_length;
    std::optional<uint64_t> data_size;
    std::unordered_set<std::string> paths_created;
};
