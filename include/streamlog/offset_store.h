#pragma once
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <tuple>

#include "streamlog/errors.h"

namespace streamlog {

// Durable consumer offsets: append-only `__offsets` file + in-memory map.
// Entry format (little-endian):
//   [group_len:4][group][topic_len:4][topic][partition:4][offset:8]
// Last entry for a (group, topic, partition) wins.
class OffsetStore {
public:
    OffsetStore() = default;
    ~OffsetStore();
    OffsetStore(const OffsetStore&) = delete;
    OffsetStore& operator=(const OffsetStore&) = delete;

    // Open (or create) the offsets file and replay it into memory.
    Status open(const std::string& path);

    // Append the commit to the file, fsync, update the map.
    Status commit(const std::string& group, const std::string& topic,
                  uint32_t partition, uint64_t offset);

    // Last committed offset; 0 when the group has never committed.
    uint64_t fetch(const std::string& group, const std::string& topic,
                   uint32_t partition);

private:
    using Key = std::tuple<std::string, std::string, uint32_t>;
    std::mutex mu_;
    std::map<Key, uint64_t> offsets_;
    int fd_ = -1;
    uint64_t file_size_ = 0;
};

}
