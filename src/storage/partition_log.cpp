#include "streamlog/partition_log.h"

#include <algorithm>
#include <charconv>
#include <filesystem>

namespace fs = std::filesystem;

namespace streamlog {

Status PartitionLog::open(const std::string& dir,
                          std::unique_ptr<PartitionLog>& out,
                          PartitionLogOptions opts) {
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) return Status::IO_ERROR;

    auto log = std::unique_ptr<PartitionLog>(new PartitionLog(dir, opts));

    std::vector<uint64_t> bases;
    for (const auto& entry : fs::directory_iterator(dir, ec)) {
        if (ec) return Status::IO_ERROR;
        if (!entry.is_regular_file() || entry.path().extension() != ".log") continue;
        std::string stem = entry.path().stem().string();
        uint64_t base = 0;
        auto [ptr, err] = std::from_chars(stem.data(), stem.data() + stem.size(), base);
        if (err != std::errc{} || ptr != stem.data() + stem.size()) continue;
        bases.push_back(base);
    }
    std::sort(bases.begin(), bases.end());

    for (uint64_t base : bases) {
        std::unique_ptr<Segment> seg;
        Status s = Segment::open(dir, base, seg);
        if (s != Status::OK) return s;
        log->segments_.push_back(std::move(seg));
    }

    if (log->segments_.empty()) {
        std::unique_ptr<Segment> seg;
        Status s = Segment::create(dir, 0, seg);
        if (s != Status::OK) return s;
        log->segments_.push_back(std::move(seg));
    }

    log->next_offset_ = log->segments_.back()->next_offset();
    out = std::move(log);
    return Status::OK;
}

Status PartitionLog::roll_segment_locked() {
    Status s = segments_.back()->flush();
    if (s != Status::OK) return s;
    std::unique_ptr<Segment> seg;
    s = Segment::create(dir_, next_offset_, seg);
    if (s != Status::OK) return s;
    segments_.push_back(std::move(seg));
    return Status::OK;
}

Status PartitionLog::append(const Record& r, uint64_t& assigned_offset) {
    std::lock_guard<std::mutex> lock(mu_);
    if (segments_.back()->size_bytes() >= opts_.max_segment_bytes) {
        Status s = roll_segment_locked();
        if (s != Status::OK) return s;
    }
    uint64_t offset = next_offset_;
    Status s = segments_.back()->append(r, offset);
    if (s != Status::OK) return s;
    next_offset_ = offset + 1;
    if (opts_.fsync_each_append) {
        s = segments_.back()->flush();
        if (s != Status::OK) return s;
    }
    assigned_offset = offset;
    return Status::OK;
}

Segment* PartitionLog::find_segment_locked(uint64_t offset) {
    // Last segment with base_offset <= offset.
    auto it = std::upper_bound(
        segments_.begin(), segments_.end(), offset,
        [](uint64_t o, const std::unique_ptr<Segment>& s) { return o < s->base_offset(); });
    if (it == segments_.begin()) return nullptr;
    return std::prev(it)->get();
}

Status PartitionLog::read(uint64_t offset, Record& out) {
    std::lock_guard<std::mutex> lock(mu_);
    if (offset >= next_offset_) return Status::NOT_FOUND;
    Segment* seg = find_segment_locked(offset);
    if (seg == nullptr) return Status::NOT_FOUND;
    return seg->read(offset, out);
}

Status PartitionLog::read_from(uint64_t offset, size_t max_records,
                               std::vector<Record>& out) {
    std::lock_guard<std::mutex> lock(mu_);
    uint64_t cur = offset;
    while (out.size() < max_records && cur < next_offset_) {
        Segment* seg = find_segment_locked(cur);
        if (seg == nullptr) return Status::NOT_FOUND;
        Record rec;
        Status s = seg->read(cur, rec);
        if (s != Status::OK) return s;
        out.push_back(std::move(rec));
        ++cur;
    }
    return Status::OK;
}

uint64_t PartitionLog::high_watermark() {
    std::lock_guard<std::mutex> lock(mu_);
    return next_offset_;
}

Status PartitionLog::flush() {
    std::lock_guard<std::mutex> lock(mu_);
    return segments_.back()->flush();
}

}
