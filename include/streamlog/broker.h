#pragma once
#include <cstdint>
#include <string>
#include <thread>

#include "streamlog/errors.h"
#include "streamlog/offset_store.h"
#include "streamlog/protocol.h"
#include "streamlog/tcp_server.h"
#include "streamlog/topic_manager.h"

namespace streamlog {

struct BrokerOptions {
    std::string data_dir;
    uint32_t default_partitions = 1;
    PartitionLogOptions log_opts;
};

// Request dispatch + orchestration. No storage internals, no socket internals.
class Broker {
public:
    explicit Broker(BrokerOptions opts);
    ~Broker();
    Broker(const Broker&) = delete;
    Broker& operator=(const Broker&) = delete;

    // Recover state from disk, bind the port, start serving on a background
    // thread. Pass port 0 for an ephemeral port (see port()).
    Status start(int port);

    // Stop serving, join the serve thread. Data stays on disk.
    void stop();

    int port() const { return server_.port(); }
    TopicManager& topics() { return topics_; }

private:
    void handle_conn(Conn& conn);
    // Each handler encodes its response frame into `out`.
    void handle_produce(ByteBuffer& payload, ByteBuffer& out);
    void handle_fetch(ByteBuffer& payload, ByteBuffer& out);
    void handle_commit(ByteBuffer& payload, ByteBuffer& out);
    void handle_metadata(ByteBuffer& payload, ByteBuffer& out);

    BrokerOptions opts_;
    TopicManager topics_;
    OffsetStore offsets_;
    TcpServer server_;
    std::thread serve_thread_;
    bool started_ = false;
};

}
