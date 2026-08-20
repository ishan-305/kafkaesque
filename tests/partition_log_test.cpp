#include <gtest/gtest.h>
#include <streamlog/partition_log.h>

#include <filesystem>

using streamlog::PartitionLog;
using streamlog::PartitionLogOptions;
using streamlog::Record;
using streamlog::Status;

namespace {
std::string temp_dir(const std::string& name) {
    auto p = std::filesystem::temp_directory_path() / ("streamlog_plog_" + name);
    std::filesystem::remove_all(p);
    return p.string();
}

Record make_record(uint64_t i) {
    return Record{.timestamp = i,
                  .key = "key-" + std::to_string(i),
                  .value = "payload-" + std::to_string(i)};
}
}

// THE GATE TEST: append 10k, destroy, reopen, read 0..9999 in order.
TEST(PartitionLog, GateAppend10kReopenReadAllInOrder) {
    std::string dir = temp_dir("gate");
    // Small segment limit so the run exercises rolling too.
    PartitionLogOptions opts;
    opts.max_segment_bytes = 64 * 1024;
    {
        std::unique_ptr<PartitionLog> log;
        ASSERT_EQ(PartitionLog::open(dir, log, opts), Status::OK);
        for (uint64_t i = 0; i < 10000; ++i) {
            uint64_t off = 999;
            ASSERT_EQ(log->append(make_record(i), off), Status::OK);
            ASSERT_EQ(off, i);
        }
        ASSERT_EQ(log->flush(), Status::OK);
        EXPECT_EQ(log->high_watermark(), 10000u);
    }  // destroy

    std::unique_ptr<PartitionLog> log;
    ASSERT_EQ(PartitionLog::open(dir, log, opts), Status::OK);
    EXPECT_EQ(log->high_watermark(), 10000u);
    for (uint64_t i = 0; i < 10000; ++i) {
        Record out;
        ASSERT_EQ(log->read(i, out), Status::OK) << "offset " << i;
        ASSERT_EQ(out, make_record(i)) << "offset " << i;
    }
    // Multiple segments must exist (rolling happened).
    size_t log_files = 0;
    for (const auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".log") ++log_files;
    }
    EXPECT_GT(log_files, 1u);
}

TEST(PartitionLog, AppendContinuesAfterReopen) {
    std::string dir = temp_dir("resume");
    {
        std::unique_ptr<PartitionLog> log;
        ASSERT_EQ(PartitionLog::open(dir, log), Status::OK);
        uint64_t off = 0;
        ASSERT_EQ(log->append(make_record(0), off), Status::OK);
        ASSERT_EQ(log->append(make_record(1), off), Status::OK);
        ASSERT_EQ(log->flush(), Status::OK);
    }
    std::unique_ptr<PartitionLog> log;
    ASSERT_EQ(PartitionLog::open(dir, log), Status::OK);
    uint64_t off = 0;
    ASSERT_EQ(log->append(make_record(2), off), Status::OK);
    EXPECT_EQ(off, 2u);  // offset counter recovered from disk
}

TEST(PartitionLog, ReadFromBatch) {
    std::string dir = temp_dir("batch");
    std::unique_ptr<PartitionLog> log;
    ASSERT_EQ(PartitionLog::open(dir, log), Status::OK);
    for (uint64_t i = 0; i < 20; ++i) {
        uint64_t off;
        ASSERT_EQ(log->append(make_record(i), off), Status::OK);
    }
    std::vector<Record> out;
    ASSERT_EQ(log->read_from(5, 10, out), Status::OK);
    ASSERT_EQ(out.size(), 10u);
    for (size_t i = 0; i < out.size(); ++i) {
        EXPECT_EQ(out[i], make_record(5 + i));
    }
    // Stops at high watermark.
    out.clear();
    ASSERT_EQ(log->read_from(15, 100, out), Status::OK);
    EXPECT_EQ(out.size(), 5u);
    // Reading past the end returns empty, OK.
    out.clear();
    ASSERT_EQ(log->read_from(20, 10, out), Status::OK);
    EXPECT_TRUE(out.empty());
}

TEST(PartitionLog, ReadMissingOffsetIsNotFound) {
    std::string dir = temp_dir("missing");
    std::unique_ptr<PartitionLog> log;
    ASSERT_EQ(PartitionLog::open(dir, log), Status::OK);
    Record out;
    EXPECT_EQ(log->read(0, out), Status::NOT_FOUND);
}
