#pragma once
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "streamlog/errors.h"
#include "streamlog/partition_log.h"

namespace streamlog {

// Owns every PartitionLog and its lifetime. Directory layout:
//   <data_dir>/<topic>-<partition>/   (one dir per partition)
// Existing topics are rediscovered from disk on construction.
class TopicManager {
public:
    // `default_partitions` is used when produce auto-creates a topic.
    explicit TopicManager(std::string data_dir, uint32_t default_partitions = 1,
                          PartitionLogOptions log_opts = {});

    TopicManager(const TopicManager&) = delete;
    TopicManager& operator=(const TopicManager&) = delete;

    // Recover topics from disk. Call once before serving.
    Status load();

    Status create_topic(const std::string& name, uint32_t num_partitions);

    // nullptr when the topic/partition does not exist.
    PartitionLog* get(const std::string& topic, uint32_t partition);

    // Like get() but auto-creates the topic with default_partitions.
    Status get_or_create(const std::string& topic, uint32_t partition,
                         PartitionLog*& out);

    // hash(key) % num_partitions. -1 when the topic is unknown.
    int route(const std::string& topic, const std::string& key);

    // 0 when the topic is unknown.
    uint32_t num_partitions(const std::string& topic);

private:
    Status open_partition_locked(const std::string& topic, uint32_t partition);

    std::string data_dir_;
    uint32_t default_partitions_;
    PartitionLogOptions log_opts_;
    std::mutex mu_;
    std::map<std::string, std::vector<std::unique_ptr<PartitionLog>>> topics_;
};

}
