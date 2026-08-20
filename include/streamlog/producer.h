#pragma once
#include <cstdint>
#include <memory>
#include <string>

#include "streamlog/errors.h"
#include "streamlog/tcp_server.h"

namespace streamlog {

// Synchronous producer (v1): one PRODUCE, wait for the ACK. Batching later.
class Producer {
public:
    Producer() = default;
    Producer(const Producer&) = delete;
    Producer& operator=(const Producer&) = delete;

    Status connect(const std::string& host, int port);

    // Key-routed produce (broker picks the partition).
    Status send(const std::string& topic, const std::string& key,
                const std::string& value, uint64_t& offset);

    // Explicit-partition produce.
    Status send_to(const std::string& topic, int32_t partition,
                   const std::string& key, const std::string& value,
                   uint64_t& offset);

private:
    std::unique_ptr<Conn> conn_;
};

}
