#include "streamlog/topic_manager.h"

#include <charconv>
#include <filesystem>
#include <functional>

namespace fs = std::filesystem;

namespace streamlog {

TopicManager::TopicManager(std::string data_dir, uint32_t default_partitions,
                           PartitionLogOptions log_opts)
    : data_dir_(std::move(data_dir)),
      default_partitions_(default_partitions == 0 ? 1 : default_partitions),
      log_opts_(log_opts) {}

Status TopicManager::load() {
    std::error_code ec;
    fs::create_directories(data_dir_, ec);
    if (ec) return Status::IO_ERROR;

    // Discover "<topic>-<partition>" directories; collect max partition per topic.
    std::map<std::string, uint32_t> partition_counts;
    for (const auto& entry : fs::directory_iterator(data_dir_, ec)) {
        if (ec) return Status::IO_ERROR;
        if (!entry.is_directory()) continue;
        std::string name = entry.path().filename().string();
        size_t dash = name.rfind('-');
        if (dash == std::string::npos || dash + 1 >= name.size()) continue;
        std::string topic = name.substr(0, dash);
        std::string idx_str = name.substr(dash + 1);
        uint32_t idx = 0;
        auto [p, err] = std::from_chars(idx_str.data(),
                                        idx_str.data() + idx_str.size(), idx);
        if (err != std::errc{} || p != idx_str.data() + idx_str.size()) continue;
        auto it = partition_counts.find(topic);
        uint32_t count = idx + 1;
        if (it == partition_counts.end() || it->second < count)
            partition_counts[topic] = count;
    }

    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& [topic, count] : partition_counts) {
        for (uint32_t p = 0; p < count; ++p) {
            Status s = open_partition_locked(topic, p);
            if (s != Status::OK) return s;
        }
    }
    return Status::OK;
}

Status TopicManager::open_partition_locked(const std::string& topic,
                                           uint32_t partition) {
    auto& logs = topics_[topic];
    if (logs.size() <= partition) logs.resize(partition + 1);
    if (logs[partition]) return Status::OK;
    std::string dir = data_dir_ + "/" + topic + "-" + std::to_string(partition);
    return PartitionLog::open(dir, logs[partition], log_opts_);
}

Status TopicManager::create_topic(const std::string& name,
                                  uint32_t num_partitions) {
    if (name.empty() || num_partitions == 0) return Status::INVALID_ARGUMENT;
    std::lock_guard<std::mutex> lock(mu_);
    for (uint32_t p = 0; p < num_partitions; ++p) {
        Status s = open_partition_locked(name, p);
        if (s != Status::OK) return s;
    }
    return Status::OK;
}

PartitionLog* TopicManager::get(const std::string& topic, uint32_t partition) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = topics_.find(topic);
    if (it == topics_.end() || partition >= it->second.size()) return nullptr;
    return it->second[partition].get();
}

Status TopicManager::get_or_create(const std::string& topic, uint32_t partition,
                                   PartitionLog*& out) {
    {
        PartitionLog* log = get(topic, partition);
        if (log != nullptr) {
            out = log;
            return Status::OK;
        }
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = topics_.find(topic);
    if (it == topics_.end()) {
        // Auto-create the whole topic with the default partition count.
        if (partition >= default_partitions_) return Status::NOT_FOUND;
        for (uint32_t p = 0; p < default_partitions_; ++p) {
            Status s = open_partition_locked(topic, p);
            if (s != Status::OK) return s;
        }
    } else if (partition >= it->second.size()) {
        return Status::NOT_FOUND;  // partition beyond the topic's count
    }
    out = topics_[topic][partition].get();
    return out != nullptr ? Status::OK : Status::NOT_FOUND;
}

int TopicManager::route(const std::string& topic, const std::string& key) {
    uint32_t n = num_partitions(topic);
    if (n == 0) n = default_partitions_;
    return static_cast<int>(std::hash<std::string>{}(key) % n);
}

uint32_t TopicManager::num_partitions(const std::string& topic) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = topics_.find(topic);
    return it == topics_.end() ? 0 : static_cast<uint32_t>(it->second.size());
}

}
