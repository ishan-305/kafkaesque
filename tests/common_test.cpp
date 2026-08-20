#include <gtest/gtest.h>
#include <streamlog/crc32.h>
#include <streamlog/byte_buffer.h>

TEST(Crc32, KnownValue) {
    const char* s = "123456789";
    // CRC32 of "123456789" is a famous test vector: 0xCBF43926
    uint32_t got = streamlog::crc32(
        reinterpret_cast<const uint8_t*>(s), 9);
    EXPECT_EQ(got, 0xCBF43926u);   // stub returns 0 → FAILS → good
}


TEST(ByteBuffer, RoundTripU32) {
    streamlog::ByteBuffer b;
    b.put_u32(123456);
    uint32_t got = 0;
    ASSERT_EQ(b.get_u32(got), streamlog::Status::OK);
    EXPECT_EQ(got, 123456u);
}

TEST(ByteBuffer, SHORT_READFails) {
    streamlog::ByteBuffer b;
    b.put_u32(1);            // 4 bytes in
    uint32_t a=0,bb=0;
    ASSERT_EQ(b.get_u32(a), streamlog::Status::OK);   // consumes all 4
    EXPECT_EQ(b.get_u32(bb), streamlog::Status::SHORT_READ);  // nothing left
}