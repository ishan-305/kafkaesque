#include "streamlog/segment.h"
#include "streamlog/byte_buffer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cinttypes>
#include <cstdio>
#include <vector>

namespace streamlog {

namespace {
Status pread_exact(int fd, uint8_t* buf, size_t n, uint64_t pos) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = ::pread(fd, buf + done, n - done,
                            static_cast<off_t>(pos + done));
        if (r == 0) return Status::SHORT_READ;
        if (r < 0) return Status::IO_ERROR;
        done += static_cast<size_t>(r);
    }
    return Status::OK;
}
}

std::string Segment::log_path(const std::string& dir, uint64_t base_offset) {
    char name[32];
    std::snprintf(name, sizeof(name), "%020" PRIu64 ".log", base_offset);
    return dir + "/" + name;
}

std::string Segment::index_path(const std::string& dir, uint64_t base_offset) {
    char name[32];
    std::snprintf(name, sizeof(name), "%020" PRIu64 ".index", base_offset);
    return dir + "/" + name;
}

Segment::~Segment() {
    if (log_fd_ >= 0) ::close(log_fd_);
}

Status Segment::create(const std::string& dir, uint64_t base_offset,
                       std::unique_ptr<Segment>& out) {
    auto seg = std::unique_ptr<Segment>(new Segment());
    seg->base_offset_ = base_offset;

    int fd = ::open(log_path(dir, base_offset).c_str(),
                    O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return Status::IO_ERROR;
    seg->log_fd_ = fd;

    Status s = seg->index_.create(index_path(dir, base_offset));
    if (s != Status::OK) return s;

    out = std::move(seg);
    return Status::OK;
}

Status Segment::open(const std::string& dir, uint64_t base_offset,
                     std::unique_ptr<Segment>& out) {
    auto seg = std::unique_ptr<Segment>(new Segment());
    seg->base_offset_ = base_offset;

    int fd = ::open(log_path(dir, base_offset).c_str(), O_RDWR, 0644);
    if (fd < 0) return Status::IO_ERROR;
    seg->log_fd_ = fd;

    struct stat st{};
    if (::fstat(fd, &st) != 0) return Status::IO_ERROR;
    uint64_t file_size = static_cast<uint64_t>(st.st_size);

    // The index is cheap to rebuild and the log is the source of truth, so
    // recovery ignores the old index file and recreates it during the scan.
    Status s = seg->index_.create(index_path(dir, base_offset));
    if (s != Status::OK) return s;

    uint64_t pos = 0;
    uint64_t count = 0;
    while (pos < file_size) {
        Record rec;
        uint64_t next_pos = 0;
        // Bound reads to file_size so a torn tail reads as SHORT_READ.
        seg->log_size_ = file_size;
        Status rs = seg->read_at(pos, rec, next_pos);
        if (rs != Status::OK || next_pos > file_size) break;  // torn/corrupt tail
        if (count % kIndexInterval == 0) {
            seg->index_.append(count, static_cast<uint32_t>(pos));
        }
        pos = next_pos;
        ++count;
    }

    if (pos < file_size) {
        if (::ftruncate(fd, static_cast<off_t>(pos)) != 0) return Status::IO_ERROR;
    }
    seg->log_size_ = pos;
    seg->record_count_ = count;

    s = seg->index_.flush();
    if (s != Status::OK) return s;

    out = std::move(seg);
    return Status::OK;
}

Status Segment::append(const Record& r, uint64_t offset) {
    if (offset != next_offset()) return Status::INVALID_ARGUMENT;

    ByteBuffer buf;
    serialize(r, buf);
    const std::vector<uint8_t>& bytes = buf.data();

    size_t done = 0;
    while (done < bytes.size()) {
        ssize_t n = ::pwrite(log_fd_, bytes.data() + done, bytes.size() - done,
                             static_cast<off_t>(log_size_ + done));
        if (n < 0) return Status::IO_ERROR;
        done += static_cast<size_t>(n);
    }

    if (record_count_ % kIndexInterval == 0) {
        index_.append(record_count_, static_cast<uint32_t>(log_size_));
    }
    log_size_ += bytes.size();
    ++record_count_;
    return Status::OK;
}

Status Segment::read_at(uint64_t byte_pos, Record& out, uint64_t& next_pos) const {
    if (byte_pos + 4 > log_size_) return Status::SHORT_READ;

    uint8_t len_bytes[4];
    Status s = pread_exact(log_fd_, len_bytes, 4, byte_pos);
    if (s != Status::OK) return s;

    ByteBuffer len_buf;
    len_buf.put_bytes(len_bytes, 4);
    uint32_t length = 0;
    len_buf.get_u32(length);
    if (length < 20) return Status::CORRUPT;
    if (byte_pos + 4 + length > log_size_) return Status::SHORT_READ;

    std::vector<uint8_t> rest(length);
    s = pread_exact(log_fd_, rest.data(), length, byte_pos + 4);
    if (s != Status::OK) return s;

    ByteBuffer rec_buf;
    rec_buf.put_bytes(len_bytes, 4);
    rec_buf.put_bytes(rest.data(), rest.size());
    s = deserialize(rec_buf, out);
    if (s != Status::OK) return s;

    next_pos = byte_pos + 4 + length;
    return Status::OK;
}

Status Segment::read(uint64_t abs_offset, Record& out) const {
    if (abs_offset < base_offset_ || abs_offset >= next_offset())
        return Status::NOT_FOUND;
    uint64_t target_rel = abs_offset - base_offset_;

    uint64_t rel = 0;
    uint32_t pos32 = 0;
    index_.lookup(target_rel, rel, pos32);
    uint64_t pos = pos32;

    while (true) {
        Record rec;
        uint64_t next_pos = 0;
        Status s = read_at(pos, rec, next_pos);
        if (s != Status::OK) return s;
        if (rel == target_rel) {
            out = std::move(rec);
            return Status::OK;
        }
        ++rel;
        pos = next_pos;
    }
}

Status Segment::flush() {
    if (::fsync(log_fd_) != 0) return Status::IO_ERROR;
    return index_.flush();
}

}
