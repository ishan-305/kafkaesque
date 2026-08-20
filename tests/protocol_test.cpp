#include <gtest/gtest.h>
#include <streamlog/protocol.h>

using namespace streamlog;

namespace {
// Encode payload, frame it, unframe it, decode payload.
template <typename T>
T frame_round_trip(MsgType type, const T& in) {
    ByteBuffer payload;
    encode(in, payload);
    ByteBuffer frame;
    EXPECT_EQ(encode_frame(type, payload, frame), Status::OK);

    MsgType got_type{};
    ByteBuffer got_payload;
    EXPECT_EQ(read_frame(frame, got_type, got_payload), Status::OK);
    EXPECT_EQ(got_type, type);

    T out;
    EXPECT_EQ(decode(got_payload, out), Status::OK);
    return out;
}
}

TEST(Protocol, ProduceReqRoundTrip) {
    ProduceReq in{.topic = "orders", .partition = -1, .key = "k1", .value = "v1"};
    EXPECT_EQ(frame_round_trip(MsgType::PRODUCE, in), in);
}

TEST(Protocol, ProduceReqExplicitPartition) {
    ProduceReq in{.topic = "orders", .partition = 3, .key = "", .value = "payload"};
    EXPECT_EQ(frame_round_trip(MsgType::PRODUCE, in), in);
}

TEST(Protocol, AckRoundTrip) {
    AckResp in{.offset = 123456789ull};
    EXPECT_EQ(frame_round_trip(MsgType::ACK, in), in);
}

TEST(Protocol, ErrorRoundTrip) {
    ErrorResp in{.code = 4, .message = "no such topic"};
    EXPECT_EQ(frame_round_trip(MsgType::ERROR, in), in);
}

TEST(Protocol, FetchReqRoundTrip) {
    FetchReq in{.topic = "orders", .partition = 2, .offset = 42, .max_bytes = 4096};
    EXPECT_EQ(frame_round_trip(MsgType::FETCH, in), in);
}

TEST(Protocol, FetchRespRoundTrip) {
    FetchResp in;
    in.records.push_back({.timestamp = 1, .key = "a", .value = "va"});
    in.records.push_back({.timestamp = 2, .key = "b", .value = "vb"});
    in.next_offset = 44;
    in.high_watermark = 100;
    EXPECT_EQ(frame_round_trip(MsgType::FETCH, in), in);
}

TEST(Protocol, FetchRespEmpty) {
    FetchResp in{.records = {}, .next_offset = 0, .high_watermark = 0};
    EXPECT_EQ(frame_round_trip(MsgType::FETCH, in), in);
}

TEST(Protocol, CommitReqRoundTrip) {
    CommitReq in{.group = "g1", .topic = "orders", .partition = 1, .offset = 77};
    EXPECT_EQ(frame_round_trip(MsgType::COMMIT_OFFSET, in), in);
}

TEST(Protocol, MetadataRoundTrip) {
    MetadataReq req{.topic = "orders", .group = "g1", .partition = 0};
    EXPECT_EQ(frame_round_trip(MsgType::METADATA, req), req);
    MetadataResp resp{.num_partitions = 4, .committed_offset = 10, .high_watermark = 20};
    EXPECT_EQ(frame_round_trip(MsgType::METADATA, resp), resp);
}

TEST(Protocol, PartialFrameIsShortRead) {
    ByteBuffer payload;
    encode(AckResp{.offset = 1}, payload);
    ByteBuffer frame;
    ASSERT_EQ(encode_frame(MsgType::ACK, payload, frame), Status::OK);

    std::vector<uint8_t> bytes = frame.data();
    ByteBuffer partial;
    partial.put_bytes(bytes.data(), bytes.size() - 2);

    MsgType t{};
    ByteBuffer p;
    EXPECT_EQ(read_frame(partial, t, p), Status::SHORT_READ);
}
