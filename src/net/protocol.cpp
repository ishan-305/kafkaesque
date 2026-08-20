#include "streamlog/protocol.h"

namespace streamlog {

namespace {

void put_string(ByteBuffer& out, const std::string& s) {
    out.put_u32(static_cast<uint32_t>(s.size()));
    out.put_bytes(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

Status get_string(ByteBuffer& in, std::string& s) {
    uint32_t len = 0;
    Status st = in.get_u32(len);
    if (st != Status::OK) return st;
    if (len > in.remaining()) return Status::SHORT_READ;
    s.resize(len);
    if (len > 0) {
        st = in.get_bytes(reinterpret_cast<uint8_t*>(s.data()), len);
        if (st != Status::OK) return st;
    }
    return Status::OK;
}

}

void encode(const ProduceReq& m, ByteBuffer& out) {
    put_string(out, m.topic);
    out.put_u32(static_cast<uint32_t>(m.partition));
    put_string(out, m.key);
    put_string(out, m.value);
}

Status decode(ByteBuffer& in, ProduceReq& out) {
    Status s = get_string(in, out.topic);
    if (s != Status::OK) return s;
    uint32_t p = 0;
    s = in.get_u32(p);
    if (s != Status::OK) return s;
    out.partition = static_cast<int32_t>(p);
    s = get_string(in, out.key);
    if (s != Status::OK) return s;
    return get_string(in, out.value);
}

void encode(const AckResp& m, ByteBuffer& out) { out.put_u64(m.offset); }

Status decode(ByteBuffer& in, AckResp& out) { return in.get_u64(out.offset); }

void encode(const ErrorResp& m, ByteBuffer& out) {
    out.put_u32(m.code);
    put_string(out, m.message);
}

Status decode(ByteBuffer& in, ErrorResp& out) {
    Status s = in.get_u32(out.code);
    if (s != Status::OK) return s;
    return get_string(in, out.message);
}

void encode(const FetchReq& m, ByteBuffer& out) {
    put_string(out, m.topic);
    out.put_u32(m.partition);
    out.put_u64(m.offset);
    out.put_u32(m.max_bytes);
}

Status decode(ByteBuffer& in, FetchReq& out) {
    Status s = get_string(in, out.topic);
    if (s != Status::OK) return s;
    s = in.get_u32(out.partition);
    if (s != Status::OK) return s;
    s = in.get_u64(out.offset);
    if (s != Status::OK) return s;
    return in.get_u32(out.max_bytes);
}

void encode(const FetchResp& m, ByteBuffer& out) {
    out.put_u32(static_cast<uint32_t>(m.records.size()));
    for (const Record& r : m.records) serialize(r, out);
    out.put_u64(m.next_offset);
    out.put_u64(m.high_watermark);
}

Status decode(ByteBuffer& in, FetchResp& out) {
    uint32_t count = 0;
    Status s = in.get_u32(count);
    if (s != Status::OK) return s;
    out.records.clear();
    out.records.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        Record r;
        s = deserialize(in, r);
        if (s != Status::OK) return s;
        out.records.push_back(std::move(r));
    }
    s = in.get_u64(out.next_offset);
    if (s != Status::OK) return s;
    return in.get_u64(out.high_watermark);
}

void encode(const CommitReq& m, ByteBuffer& out) {
    put_string(out, m.group);
    put_string(out, m.topic);
    out.put_u32(m.partition);
    out.put_u64(m.offset);
}

Status decode(ByteBuffer& in, CommitReq& out) {
    Status s = get_string(in, out.group);
    if (s != Status::OK) return s;
    s = get_string(in, out.topic);
    if (s != Status::OK) return s;
    s = in.get_u32(out.partition);
    if (s != Status::OK) return s;
    return in.get_u64(out.offset);
}

void encode(const MetadataReq& m, ByteBuffer& out) {
    put_string(out, m.topic);
    put_string(out, m.group);
    out.put_u32(m.partition);
}

Status decode(ByteBuffer& in, MetadataReq& out) {
    Status s = get_string(in, out.topic);
    if (s != Status::OK) return s;
    s = get_string(in, out.group);
    if (s != Status::OK) return s;
    return in.get_u32(out.partition);
}

void encode(const MetadataResp& m, ByteBuffer& out) {
    out.put_u32(m.num_partitions);
    out.put_u64(m.committed_offset);
    out.put_u64(m.high_watermark);
}

Status decode(ByteBuffer& in, MetadataResp& out) {
    Status s = in.get_u32(out.num_partitions);
    if (s != Status::OK) return s;
    s = in.get_u64(out.committed_offset);
    if (s != Status::OK) return s;
    return in.get_u64(out.high_watermark);
}

Status encode_frame(MsgType type, const ByteBuffer& payload, ByteBuffer& out) {
    const std::vector<uint8_t>& p = payload.data();
    out.put_u32(1 + static_cast<uint32_t>(p.size()));  // type byte + payload
    uint8_t t = static_cast<uint8_t>(type);
    out.put_bytes(&t, 1);
    out.put_bytes(p.data(), p.size());
    return Status::OK;
}

Status read_frame(ByteBuffer& in, MsgType& type, ByteBuffer& payload) {
    uint32_t len = 0;
    Status s = in.get_u32(len);
    if (s != Status::OK) return s;
    if (len < 1) return Status::CORRUPT;
    if (in.remaining() < len) return Status::SHORT_READ;
    uint8_t t = 0;
    s = in.get_bytes(&t, 1);
    if (s != Status::OK) return s;
    type = static_cast<MsgType>(t);
    std::vector<uint8_t> body(len - 1);
    s = in.get_bytes(body.data(), body.size());
    if (s != Status::OK) return s;
    payload = ByteBuffer();
    payload.put_bytes(body.data(), body.size());
    return Status::OK;
}

}
