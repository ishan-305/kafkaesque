#include "streamlog/index.h"
#include "streamlog/byte_buffer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>

namespace streamlog {

OffsetIndex::~OffsetIndex() {
    if (fd_ >= 0) ::close(fd_);
}

Status OffsetIndex::create(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return Status::IO_ERROR;
    fd_ = fd;
    entries_.clear();
    flushed_count_ = 0;
    return Status::OK;
}

Status OffsetIndex::load(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) return Status::IO_ERROR;

    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        ::close(fd);
        return Status::IO_ERROR;
    }

    std::vector<uint8_t> raw(static_cast<size_t>(st.st_size));
    size_t done = 0;
    while (done < raw.size()) {
        ssize_t n = ::pread(fd, raw.data() + done, raw.size() - done,
                            static_cast<off_t>(done));
        if (n <= 0) {
            ::close(fd);
            return Status::IO_ERROR;
        }
        done += static_cast<size_t>(n);
    }

    entries_.clear();
    ByteBuffer buf;
    buf.put_bytes(raw.data(), raw.size());
    while (buf.remaining() >= 8) {
        uint32_t rel = 0, pos = 0;
        buf.get_u32(rel);
        buf.get_u32(pos);
        entries_.emplace_back(rel, pos);
    }
    // Ignore a trailing partial entry (torn write); it is rewritten on flush.
    flushed_count_ = entries_.size();
    if (::ftruncate(fd, static_cast<off_t>(entries_.size() * 8)) != 0) {
        ::close(fd);
        return Status::IO_ERROR;
    }
    fd_ = fd;
    return Status::OK;
}

Status OffsetIndex::append(uint64_t rel_offset, uint32_t file_pos) {
    if (rel_offset > UINT32_MAX) return Status::INVALID_ARGUMENT;
    if (!entries_.empty() && rel_offset < entries_.back().first)
        return Status::INVALID_ARGUMENT;
    entries_.emplace_back(static_cast<uint32_t>(rel_offset), file_pos);
    return Status::OK;
}

void OffsetIndex::lookup(uint64_t target_rel, uint64_t& floor_rel,
                         uint32_t& floor_pos) const {
    floor_rel = 0;
    floor_pos = 0;
    // First entry with rel_offset > target, then step back one.
    auto it = std::upper_bound(
        entries_.begin(), entries_.end(), target_rel,
        [](uint64_t t, const std::pair<uint32_t, uint32_t>& e) { return t < e.first; });
    if (it == entries_.begin()) return;
    --it;
    floor_rel = it->first;
    floor_pos = it->second;
}

Status OffsetIndex::flush() {
    if (fd_ < 0) return Status::IO_ERROR;
    while (flushed_count_ < entries_.size()) {
        ByteBuffer buf;
        buf.put_u32(entries_[flushed_count_].first);
        buf.put_u32(entries_[flushed_count_].second);
        const auto& bytes = buf.data();
        ssize_t n = ::pwrite(fd_, bytes.data(), bytes.size(),
                             static_cast<off_t>(flushed_count_ * 8));
        if (n != static_cast<ssize_t>(bytes.size())) return Status::IO_ERROR;
        ++flushed_count_;
    }
    if (::fsync(fd_) != 0) return Status::IO_ERROR;
    return Status::OK;
}

}
