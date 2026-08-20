#include "streamlog/broker.h"

#include <chrono>

namespace streamlog {

namespace {
uint64_t now_millis() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

void write_error(Status code, const std::string& message, ByteBuffer& out) {
    ByteBuffer payload;
    encode(ErrorResp{.code = static_cast<uint32_t>(code), .message = message},
           payload);
    encode_frame(MsgType::ERROR, payload, out);
}
}

Broker::Broker(BrokerOptions opts)
    : opts_(opts),
      topics_(opts.data_dir, opts.default_partitions, opts.log_opts) {}

Broker::~Broker() { stop(); }

Status Broker::start(int port) {
    Status s = topics_.load();
    if (s != Status::OK) return s;
    s = offsets_.open(opts_.data_dir + "/__offsets");
    if (s != Status::OK) return s;
    s = server_.listen(port);
    if (s != Status::OK) return s;

    serve_thread_ = std::thread(
        [this]() { server_.serve([this](Conn& c) { handle_conn(c); }); });
    started_ = true;
    return Status::OK;
}

void Broker::stop() {
    if (!started_) return;
    server_.stop();
    if (serve_thread_.joinable()) serve_thread_.join();
    started_ = false;
}

void Broker::handle_conn(Conn& conn) {
    while (true) {
        ByteBuffer frame;
        if (conn.read_frame_bytes(frame) != Status::OK) return;  // peer closed

        MsgType type{};
        ByteBuffer payload;
        ByteBuffer response;
        if (read_frame(frame, type, payload) != Status::OK) {
            write_error(Status::CORRUPT, "malformed frame", response);
        } else {
            switch (type) {
                case MsgType::PRODUCE:
                    handle_produce(payload, response);
                    break;
                case MsgType::FETCH:
                    handle_fetch(payload, response);
                    break;
                case MsgType::COMMIT_OFFSET:
                    handle_commit(payload, response);
                    break;
                case MsgType::METADATA:
                    handle_metadata(payload, response);
                    break;
                default:
                    write_error(Status::INVALID_ARGUMENT, "unknown message type",
                                response);
            }
        }
        const auto& bytes = response.data();
        if (conn.write_all(bytes.data(), bytes.size()) != Status::OK) return;
    }
}

void Broker::handle_produce(ByteBuffer& payload, ByteBuffer& out) {
    ProduceReq req;
    if (decode(payload, req) != Status::OK || req.topic.empty()) {
        write_error(Status::CORRUPT, "bad produce request", out);
        return;
    }

    int partition = req.partition >= 0 ? req.partition
                                       : topics_.route(req.topic, req.key);
    PartitionLog* log = nullptr;
    Status s = topics_.get_or_create(req.topic, static_cast<uint32_t>(partition),
                                     log);
    if (s != Status::OK) {
        write_error(s, "no such topic/partition", out);
        return;
    }

    Record rec{.timestamp = now_millis(), .key = req.key, .value = req.value};
    uint64_t offset = 0;
    s = log->append(rec, offset);
    if (s != Status::OK) {
        write_error(s, "append failed", out);
        return;
    }
    // Durability before the ack: flush unless every append already fsyncs.
    if (!opts_.log_opts.fsync_each_append) {
        s = log->flush();
        if (s != Status::OK) {
            write_error(s, "flush failed", out);
            return;
        }
    }

    ByteBuffer resp;
    encode(AckResp{.offset = offset}, resp);
    encode_frame(MsgType::ACK, resp, out);
}

void Broker::handle_fetch(ByteBuffer& payload, ByteBuffer& out) {
    FetchReq req;
    if (decode(payload, req) != Status::OK) {
        write_error(Status::CORRUPT, "bad fetch request", out);
        return;
    }
    PartitionLog* log = topics_.get(req.topic, req.partition);
    if (log == nullptr) {
        write_error(Status::NOT_FOUND, "no such topic/partition", out);
        return;
    }

    // Accumulate records until max_bytes is exceeded (always at least one).
    FetchResp resp;
    uint64_t cur = req.offset;
    uint64_t hw = log->high_watermark();
    uint32_t bytes_used = 0;
    while (cur < hw) {
        Record rec;
        Status s = log->read(cur, rec);
        if (s != Status::OK) {
            write_error(s, "read failed", out);
            return;
        }
        uint32_t sz = encoded_size(rec);
        if (!resp.records.empty() && bytes_used + sz > req.max_bytes) break;
        bytes_used += sz;
        resp.records.push_back(std::move(rec));
        ++cur;
    }
    resp.next_offset = cur;
    resp.high_watermark = hw;

    ByteBuffer body;
    encode(resp, body);
    encode_frame(MsgType::FETCH, body, out);
}

void Broker::handle_commit(ByteBuffer& payload, ByteBuffer& out) {
    CommitReq req;
    if (decode(payload, req) != Status::OK || req.group.empty()) {
        write_error(Status::CORRUPT, "bad commit request", out);
        return;
    }
    Status s = offsets_.commit(req.group, req.topic, req.partition, req.offset);
    if (s != Status::OK) {
        write_error(s, "commit failed", out);
        return;
    }
    ByteBuffer resp;
    encode(AckResp{.offset = req.offset}, resp);
    encode_frame(MsgType::ACK, resp, out);
}

void Broker::handle_metadata(ByteBuffer& payload, ByteBuffer& out) {
    MetadataReq req;
    if (decode(payload, req) != Status::OK) {
        write_error(Status::CORRUPT, "bad metadata request", out);
        return;
    }
    MetadataResp resp;
    resp.num_partitions = topics_.num_partitions(req.topic);
    resp.committed_offset = offsets_.fetch(req.group, req.topic, req.partition);
    PartitionLog* log = topics_.get(req.topic, req.partition);
    resp.high_watermark = log != nullptr ? log->high_watermark() : 0;

    ByteBuffer body;
    encode(resp, body);
    encode_frame(MsgType::METADATA, body, out);
}

}
