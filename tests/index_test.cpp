#include <gtest/gtest.h>
#include <streamlog/index.h>

#include <filesystem>

using streamlog::OffsetIndex;
using streamlog::Status;

namespace {
std::string temp_path(const std::string& name) {
    auto p = std::filesystem::temp_directory_path() / ("streamlog_index_" + name);
    std::filesystem::remove(p);
    return p.string();
}
}

TEST(OffsetIndex, FloorLookup) {
    OffsetIndex idx;
    ASSERT_EQ(idx.create(temp_path("floor")), Status::OK);
    ASSERT_EQ(idx.append(0, 0), Status::OK);
    ASSERT_EQ(idx.append(8, 100), Status::OK);
    ASSERT_EQ(idx.append(16, 250), Status::OK);

    uint64_t rel = 99;
    uint32_t pos = 99;
    idx.lookup(10, rel, pos);   // between 8 and 16 → floor is 8
    EXPECT_EQ(rel, 8u);
    EXPECT_EQ(pos, 100u);

    idx.lookup(16, rel, pos);   // exact hit
    EXPECT_EQ(rel, 16u);
    EXPECT_EQ(pos, 250u);

    idx.lookup(500, rel, pos);  // past the end → last entry
    EXPECT_EQ(rel, 16u);
    EXPECT_EQ(pos, 250u);

    idx.lookup(3, rel, pos);    // before second entry → first
    EXPECT_EQ(rel, 0u);
    EXPECT_EQ(pos, 0u);
}

TEST(OffsetIndex, EmptyLookupReturnsZero) {
    OffsetIndex idx;
    ASSERT_EQ(idx.create(temp_path("empty")), Status::OK);
    uint64_t rel = 99;
    uint32_t pos = 99;
    idx.lookup(42, rel, pos);
    EXPECT_EQ(rel, 0u);
    EXPECT_EQ(pos, 0u);
}

TEST(OffsetIndex, FlushAndLoadRoundTrip) {
    std::string path = temp_path("persist");
    {
        OffsetIndex idx;
        ASSERT_EQ(idx.create(path), Status::OK);
        ASSERT_EQ(idx.append(0, 0), Status::OK);
        ASSERT_EQ(idx.append(8, 123), Status::OK);
        ASSERT_EQ(idx.flush(), Status::OK);
    }
    OffsetIndex idx;
    ASSERT_EQ(idx.load(path), Status::OK);
    EXPECT_EQ(idx.entry_count(), 2u);
    uint64_t rel = 0;
    uint32_t pos = 0;
    idx.lookup(9, rel, pos);
    EXPECT_EQ(rel, 8u);
    EXPECT_EQ(pos, 123u);
}

TEST(OffsetIndex, RejectsOutOfOrderAppend) {
    OffsetIndex idx;
    ASSERT_EQ(idx.create(temp_path("order")), Status::OK);
    ASSERT_EQ(idx.append(8, 100), Status::OK);
    EXPECT_EQ(idx.append(4, 50), Status::INVALID_ARGUMENT);
}
