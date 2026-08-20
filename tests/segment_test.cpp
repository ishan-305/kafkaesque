#include <gtest/gtest.h>
#include <streamlog/segment.h>

#include <filesystem>
#include <fstream>

using streamlog::Record;
using streamlog::Segment;
using streamlog::Status;

namespace {
std::string temp_dir(const std::string& name) {
    auto p = std::filesystem::temp_directory_path() / ("streamlog_segment_" + name);
    std::filesystem::remove_all(p);
    std::filesystem::create_directories(p);
    return p.string();
}

Record make_record(uint64_t i) {
    return Record{.timestamp = 1000 + i,
                  .key = "key-" + std::to_string(i),
                  .value = "value-" + std::to_string(i)};
}
}

TEST(Segment, AppendFlushReopenReadAll) {
    std::string dir = temp_dir("reopen");
    {
        std::unique_ptr<Segment> seg;
        ASSERT_EQ(Segment::create(dir, 0, seg), Status::OK);
        for (uint64_t i = 0; i < 100; ++i) {
            ASSERT_EQ(seg->append(make_record(i), i), Status::OK);
        }
        ASSERT_EQ(seg->flush(), Status::OK);
        EXPECT_EQ(seg->next_offset(), 100u);
    }
    std::unique_ptr<Segment> seg;
    ASSERT_EQ(Segment::open(dir, 0, seg), Status::OK);
    EXPECT_EQ(seg->base_offset(), 0u);
    EXPECT_EQ(seg->next_offset(), 100u);
    for (uint64_t i = 0; i < 100; ++i) {
        Record out;
        ASSERT_EQ(seg->read(i, out), Status::OK) << "offset " << i;
        EXPECT_EQ(out, make_record(i));
    }
}

TEST(Segment, NonZeroBaseOffset) {
    std::string dir = temp_dir("base");
    std::unique_ptr<Segment> seg;
    ASSERT_EQ(Segment::create(dir, 500, seg), Status::OK);
    ASSERT_EQ(seg->append(make_record(500), 500), Status::OK);
    ASSERT_EQ(seg->append(make_record(501), 501), Status::OK);

    Record out;
    ASSERT_EQ(seg->read(501, out), Status::OK);
    EXPECT_EQ(out, make_record(501));
    EXPECT_EQ(seg->read(499, out), Status::NOT_FOUND);
    EXPECT_EQ(seg->read(502, out), Status::NOT_FOUND);
}

TEST(Segment, RejectsWrongOffset) {
    std::string dir = temp_dir("wrongoff");
    std::unique_ptr<Segment> seg;
    ASSERT_EQ(Segment::create(dir, 0, seg), Status::OK);
    EXPECT_EQ(seg->append(make_record(0), 5), Status::INVALID_ARGUMENT);
}

TEST(Segment, TornTailTruncatedOnOpen) {
    std::string dir = temp_dir("torn");
    {
        std::unique_ptr<Segment> seg;
        ASSERT_EQ(Segment::create(dir, 0, seg), Status::OK);
        for (uint64_t i = 0; i < 10; ++i) {
            ASSERT_EQ(seg->append(make_record(i), i), Status::OK);
        }
        ASSERT_EQ(seg->flush(), Status::OK);
    }
    // Simulate a crash mid-write: append garbage half-record to the log.
    {
        std::ofstream f(Segment::log_path(dir, 0), std::ios::binary | std::ios::app);
        const char garbage[] = {0x40, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03};
        f.write(garbage, sizeof(garbage));
    }
    std::unique_ptr<Segment> seg;
    ASSERT_EQ(Segment::open(dir, 0, seg), Status::OK);
    EXPECT_EQ(seg->next_offset(), 10u);  // torn record dropped
    for (uint64_t i = 0; i < 10; ++i) {
        Record out;
        ASSERT_EQ(seg->read(i, out), Status::OK);
        EXPECT_EQ(out, make_record(i));
    }
    // And the segment still accepts appends after recovery.
    ASSERT_EQ(seg->append(make_record(10), 10), Status::OK);
    Record out;
    ASSERT_EQ(seg->read(10, out), Status::OK);
    EXPECT_EQ(out, make_record(10));
}
