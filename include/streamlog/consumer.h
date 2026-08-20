#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "streamlog/errors.h"
#include "streamlog/record.h"
#include "streamlog/tcp_server.h"

namespace streamlog {

// Synchronous consumer (v1): single topic-partition per subscribe.
// subscribe() resumes from the group's committed offset.
class Consumer {
public:
    Consumer() = default;
    Consumer(const Consumer&) = delete;
    Consumer& operator=(const Consumer&) = delete;

    Status connect(const std::string& host, int port);

    // Fetch the committed offset for (group, topic, partition) and position
    // there. Never-committed groups start at offset 0.
    Status subscribe(const std::string& topic, uint32_t partition,
                     const std::string& group = "default");

    // FETCH from the current position, append records to `out`, advance the
    // position. Empty `out` with OK means caught up to the high watermark.
    Status poll(std::vector<Record>& out, uint32_t max_bytes = 1024 * 1024);

    // COMMIT_OFFSET the current position (i.e. everything polled so far is
    // marked processed).
    Status commit();

    // Override the position (e.g. replay from 0).
    void seek(uint64_t offset) { position_ = offset; }

    uint64_t position() const { return position_; }
    uint64_t high_watermark() const { return high_watermark_; }

private:
    std::unique_ptr<Conn> conn_;
    std::string topic_;
    std::string group_;
    uint32_t partition_ = 0;
    uint64_t position_ = 0;
    uint64_t high_watermark_ = 0;
};

}
