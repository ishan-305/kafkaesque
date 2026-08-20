#pragma once
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "streamlog/errors.h"
#include "streamlog/record.h"
#include "streamlog/segment.h"

namespace streamlog {

struct PartitionLogOptions {
    // Roll the active segment once it grows past this many bytes.
    uint64_t max_segment_bytes = 64ull * 1024 * 1024;
    // fsync log + index on every append. Off by default; the broker flushes
    // before acking a produce instead.
    bool fsync_each_append = false;
};

// A partition: a directory of segments, active segment at the tail.
// Owns the offset counter. Thread-safe (coarse mutex, v1).
class PartitionLog {
public:
    static Status open(const std::string& dir, std::unique_ptr<PartitionLog>& out,
                       PartitionLogOptions opts = {});

    PartitionLog(const PartitionLog&) = delete;
    PartitionLog& operator=(const PartitionLog&) = delete;

    // Assign offset = next_offset++, append to the active segment,
    // roll first if the active segment is over the size limit.
    Status append(const Record& r, uint64_t& assigned_offset);

    // Read one record by absolute offset.
    Status read(uint64_t offset, Record& out);

    // Batch fetch for consumers; stops at the end of the log.
    Status read_from(uint64_t offset, size_t max_records, std::vector<Record>& out);

    // Next offset to be assigned == end of log; consumers stop here.
    uint64_t high_watermark();

    // fsync the active segment (older segments were flushed when rolled).
    Status flush();

private:
    explicit PartitionLog(std::string dir, PartitionLogOptions opts)
        : dir_(std::move(dir)), opts_(opts) {}

    Status roll_segment_locked();
    Segment* find_segment_locked(uint64_t offset);

    std::string dir_;
    PartitionLogOptions opts_;
    std::mutex mu_;
    std::vector<std::unique_ptr<Segment>> segments_;  // sorted by base_offset
    uint64_t next_offset_ = 0;
};

}
