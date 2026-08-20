#pragma once
#include <cstdint>
#include <memory>
#include <string>

#include "streamlog/errors.h"
#include "streamlog/index.h"
#include "streamlog/record.h"

namespace streamlog {

// One physical file pair: <base_offset>.log + <base_offset>.index inside a
// partition directory. Append-only; owns its sparse index. No roll logic and
// no fsync-per-append policy — the caller (PartitionLog) decides both.
class Segment {
public:
    // One index entry every N records.
    static constexpr uint64_t kIndexInterval = 8;

    // New empty segment in `dir` with the given base offset.
    static Status create(const std::string& dir, uint64_t base_offset,
                         std::unique_ptr<Segment>& out);

    // Reopen an existing segment: scan the log to recover the end position
    // and record count, truncate a torn tail record, rebuild the index.
    static Status open(const std::string& dir, uint64_t base_offset,
                       std::unique_ptr<Segment>& out);

    ~Segment();
    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;

    // `offset` must equal next_offset(); the offset is implicit on disk.
    Status append(const Record& r, uint64_t offset);

    // Read one record at a known byte position. `next_pos` is set to the
    // position of the following record.
    Status read_at(uint64_t byte_pos, Record& out, uint64_t& next_pos) const;

    // Read the record with the given absolute offset: index floor lookup,
    // then scan forward.
    Status read(uint64_t abs_offset, Record& out) const;

    // fsync log + index.
    Status flush();

    uint64_t size_bytes() const { return log_size_; }
    uint64_t base_offset() const { return base_offset_; }
    uint64_t next_offset() const { return base_offset_ + record_count_; }

    static std::string log_path(const std::string& dir, uint64_t base_offset);
    static std::string index_path(const std::string& dir, uint64_t base_offset);

private:
    Segment() = default;

    uint64_t base_offset_ = 0;
    uint64_t record_count_ = 0;
    uint64_t log_size_ = 0;
    int log_fd_ = -1;
    OffsetIndex index_;
};

}
