#include <gtest/gtest.h>
#include <streamlog/record.h>

using streamlog::ByteBuffer;
using streamlog::Record;
using streamlog::Status;

TEST(Record, RoundTrip) {
    Record in{.timestamp = 1234567890123ull, .key = "user-42", .value = "hello kafka"};
    ByteBuffer buf;
    ASSERT_EQ(streamlog::serialize(in, buf), Status::OK);
    EXPECT_EQ(buf.data().size(), streamlog::encoded_size(in));

    Record out;
    ASSERT_EQ(streamlog::deserialize(buf, out), Status::OK);
    EXPECT_EQ(out, in);
}

TEST(Record, EmptyKeyAndValue) {
    Record in{.timestamp = 0, .key = "", .value = ""};
    ByteBuffer buf;
    ASSERT_EQ(streamlog::serialize(in, buf), Status::OK);
    Record out;
    ASSERT_EQ(streamlog::deserialize(buf, out), Status::OK);
    EXPECT_EQ(out, in);
}

TEST(Record, CorruptByteFailsCrc) {
    Record in{.timestamp = 99, .key = "k", .value = "some payload"};
    ByteBuffer buf;
    ASSERT_EQ(streamlog::serialize(in, buf), Status::OK);

    // Flip one payload byte (past length+crc header = byte 8+).
    std::vector<uint8_t> bytes = buf.data();
    bytes[bytes.size() - 1] ^= 0xFF;
    ByteBuffer corrupted;
    corrupted.put_bytes(bytes.data(), bytes.size());

    Record out;
    EXPECT_EQ(streamlog::deserialize(corrupted, out), Status::CRC);
}

TEST(Record, TruncatedFrameIsShortRead) {
    Record in{.timestamp = 99, .key = "k", .value = "some payload"};
    ByteBuffer buf;
    ASSERT_EQ(streamlog::serialize(in, buf), Status::OK);

    std::vector<uint8_t> bytes = buf.data();
    ByteBuffer truncated;
    truncated.put_bytes(bytes.data(), bytes.size() - 3);

    Record out;
    EXPECT_EQ(streamlog::deserialize(truncated, out), Status::SHORT_READ);
}

TEST(Record, TwoRecordsBackToBack) {
    Record a{.timestamp = 1, .key = "a", .value = "first"};
    Record b{.timestamp = 2, .key = "b", .value = "second"};
    ByteBuffer buf;
    ASSERT_EQ(streamlog::serialize(a, buf), Status::OK);
    ASSERT_EQ(streamlog::serialize(b, buf), Status::OK);

    Record out;
    ASSERT_EQ(streamlog::deserialize(buf, out), Status::OK);
    EXPECT_EQ(out, a);
    ASSERT_EQ(streamlog::deserialize(buf, out), Status::OK);
    EXPECT_EQ(out, b);
}
