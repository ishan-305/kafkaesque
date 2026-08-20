#include "streamlog/consumer.h"
#include "client_util.h"

namespace streamlog {

Status Consumer::connect(const std::string& host, int port) {
    return client::tcp_connect(host, port, conn_);
}

Status Consumer::subscribe(const std::string& topic, uint32_t partition,
                           const std::string& group) {
    if (!conn_) return Status::IO_ERROR;
    topic_ = topic;
    partition_ = partition;
    group_ = group;

    ByteBuffer payload;
    encode(MetadataReq{.topic = topic, .group = group, .partition = partition},
           payload);

    MsgType resp_type{};
    ByteBuffer resp;
    Status s = client::request(*conn_, MsgType::METADATA, payload, resp_type, resp);
    if (s != Status::OK) return s;
    if (resp_type == MsgType::ERROR) return client::error_status(resp);
    if (resp_type != MsgType::METADATA) return Status::CORRUPT;

    MetadataResp meta;
    s = decode(resp, meta);
    if (s != Status::OK) return s;
    position_ = meta.committed_offset;
    high_watermark_ = meta.high_watermark;
    return Status::OK;
}

Status Consumer::poll(std::vector<Record>& out, uint32_t max_bytes) {
    if (!conn_) return Status::IO_ERROR;

    ByteBuffer payload;
    encode(FetchReq{.topic = topic_, .partition = partition_, .offset = position_,
                    .max_bytes = max_bytes},
           payload);

    MsgType resp_type{};
    ByteBuffer resp;
    Status s = client::request(*conn_, MsgType::FETCH, payload, resp_type, resp);
    if (s != Status::OK) return s;
    if (resp_type == MsgType::ERROR) return client::error_status(resp);
    if (resp_type != MsgType::FETCH) return Status::CORRUPT;

    FetchResp fetched;
    s = decode(resp, fetched);
    if (s != Status::OK) return s;

    position_ = fetched.next_offset;
    high_watermark_ = fetched.high_watermark;
    for (Record& r : fetched.records) out.push_back(std::move(r));
    return Status::OK;
}

Status Consumer::commit() {
    if (!conn_) return Status::IO_ERROR;

    ByteBuffer payload;
    encode(CommitReq{.group = group_, .topic = topic_, .partition = partition_,
                     .offset = position_},
           payload);

    MsgType resp_type{};
    ByteBuffer resp;
    Status s = client::request(*conn_, MsgType::COMMIT_OFFSET, payload, resp_type,
                               resp);
    if (s != Status::OK) return s;
    if (resp_type == MsgType::ERROR) return client::error_status(resp);
    if (resp_type != MsgType::ACK) return Status::CORRUPT;
    return Status::OK;
}

}
