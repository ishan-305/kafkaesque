#pragma once
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "streamlog/errors.h"

namespace streamlog {

// Sparse offset index for one segment. On-disk format (little-endian):
//   [rel_offset:4][file_pos:4]   repeated
// rel_offset = offset - segment.base_offset. Entries are append-only and
// sorted by construction. Lookup returns the floor entry (largest <= target);
// the caller scans the log forward from there to the exact offset.
class OffsetIndex {
public:
    OffsetIndex() = default;
    ~OffsetIndex();
    OffsetIndex(const OffsetIndex&) = delete;
    OffsetIndex& operator=(const OffsetIndex&) = delete;

    // Create a new (empty) index file, truncating any existing one.
    Status create(const std::string& path);

    // Load an existing index file into memory; keeps the file open for appends.
    Status load(const std::string& path);

    // Append an entry. Must be called with non-decreasing rel_offset.
    Status append(uint64_t rel_offset, uint32_t file_pos);

    // Floor lookup: largest entry with rel_offset <= target.
    // When the index is empty (or target precedes the first entry) returns
    // {0, 0} — the first record of a segment always lives at file pos 0.
    void lookup(uint64_t target_rel, uint64_t& floor_rel, uint32_t& floor_pos) const;

    // Persist entries appended since the last flush, then fsync.
    Status flush();

    size_t entry_count() const { return entries_.size(); }

private:
    std::vector<std::pair<uint32_t, uint32_t>> entries_;  // (rel_offset, file_pos)
    size_t flushed_count_ = 0;
    int fd_ = -1;
};

}
