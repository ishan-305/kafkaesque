#pragma once
#include <cstdint>
#include <string>
#include <vector>

#include "streamlog/byte_buffer.h"
#include "streamlog/errors.h"
#include "streamlog/record.h"

namespace streamlog {

// Wire frame: [len:4][type:1][payload...]
// len counts everything after the len field (type byte + payload).
enum class MsgType : uint8_t {
    PRODUCE = 1,
    FETCH = 2,
    ACK = 3,
    ERROR = 4,
    COMMIT_OFFSET = 5,
    METADATA = 6,
};

// Strings on the wire: [len:4][bytes].

struct ProduceReq {
    std::string topic;
    int32_t partition = -1;  // -1 = route by hash(key)
    std::string key;
    std::string value;
    bool operator==(const ProduceReq&) const = default;
};

struct AckResp {
    uint64_t offset = 0;
    bool operator==(const AckResp&) const = default;
};

struct ErrorResp {
    uint32_t code = 0;  // a Status value
    std::string message;
    bool operator==(const ErrorResp&) const = default;
};

struct FetchReq {
    std::string topic;
    uint32_t partition = 0;
    uint64_t offset = 0;
    uint32_t max_bytes = 1024 * 1024;
    bool operator==(const FetchReq&) const = default;
};

struct FetchResp {
    std::vector<Record> records;
    uint64_t next_offset = 0;
    uint64_t high_watermark = 0;
    bool operator==(const FetchResp&) const = default;
};

struct CommitReq {
    std::string group;
    std::string topic;
    uint32_t partition = 0;
    uint64_t offset = 0;
    bool operator==(const CommitReq&) const = default;
};

struct MetadataReq {
    std::string topic;
    std::string group;      // used to return the committed offset
    uint32_t partition = 0;
    bool operator==(const MetadataReq&) const = default;
};

struct MetadataResp {
    uint32_t num_partitions = 0;
    uint64_t committed_offset = 0;
    uint64_t high_watermark = 0;
    bool operator==(const MetadataResp&) const = default;
};

// Payload ser/de. Pure byte transforms, no sockets.
void encode(const ProduceReq& m, ByteBuffer& out);
Status decode(ByteBuffer& in, ProduceReq& out);
void encode(const AckResp& m, ByteBuffer& out);
Status decode(ByteBuffer& in, AckResp& out);
void encode(const ErrorResp& m, ByteBuffer& out);
Status decode(ByteBuffer& in, ErrorResp& out);
void encode(const FetchReq& m, ByteBuffer& out);
Status decode(ByteBuffer& in, FetchReq& out);
void encode(const FetchResp& m, ByteBuffer& out);
Status decode(ByteBuffer& in, FetchResp& out);
void encode(const CommitReq& m, ByteBuffer& out);
Status decode(ByteBuffer& in, CommitReq& out);
void encode(const MetadataReq& m, ByteBuffer& out);
Status decode(ByteBuffer& in, MetadataReq& out);
void encode(const MetadataResp& m, ByteBuffer& out);
Status decode(ByteBuffer& in, MetadataResp& out);

// Frame the payload: [len:4][type:1][payload].
Status encode_frame(MsgType type, const ByteBuffer& payload, ByteBuffer& out);

// Parse one complete frame from `in`. Caller is responsible for having read a
// full frame off the socket (see Conn::read_frame).
Status read_frame(ByteBuffer& in, MsgType& type, ByteBuffer& payload);

}
