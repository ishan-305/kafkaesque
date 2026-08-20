#include "streamlog/producer.h"
#include "client_util.h"

namespace streamlog {

Status Producer::connect(const std::string& host, int port) {
    return client::tcp_connect(host, port, conn_);
}

Status Producer::send(const std::string& topic, const std::string& key,
                      const std::string& value, uint64_t& offset) {
    return send_to(topic, -1, key, value, offset);
}

Status Producer::send_to(const std::string& topic, int32_t partition,
                         const std::string& key, const std::string& value,
                         uint64_t& offset) {
    if (!conn_) return Status::IO_ERROR;

    ByteBuffer payload;
    encode(ProduceReq{.topic = topic, .partition = partition, .key = key,
                      .value = value},
           payload);

    MsgType resp_type{};
    ByteBuffer resp;
    Status s = client::request(*conn_, MsgType::PRODUCE, payload, resp_type, resp);
    if (s != Status::OK) return s;
    if (resp_type == MsgType::ERROR) return client::error_status(resp);
    if (resp_type != MsgType::ACK) return Status::CORRUPT;

    AckResp ack;
    s = decode(resp, ack);
    if (s != Status::OK) return s;
    offset = ack.offset;
    return Status::OK;
}

}
