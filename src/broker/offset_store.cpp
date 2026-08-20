#include "streamlog/offset_store.h"
#include "streamlog/byte_buffer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <vector>

namespace streamlog {

namespace {
void put_string(ByteBuffer& out, const std::string& s) {
    out.put_u32(static_cast<uint32_t>(s.size()));
    out.put_bytes(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

Status get_string(ByteBuffer& in, std::string& s) {
    uint32_t len = 0;
    Status st = in.get_u32(len);
    if (st != Status::OK) return st;
    if (len > in.remaining()) return Status::SHORT_READ;
    s.resize(len);
    if (len > 0) {
        st = in.get_bytes(reinterpret_cast<uint8_t*>(s.data()), len);
        if (st != Status::OK) return st;
    }
    return Status::OK;
}
}

OffsetStore::~OffsetStore() {
    if (fd_ >= 0) ::close(fd_);
}

Status OffsetStore::open(const std::string& path) {
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

    ByteBuffer buf;
    buf.put_bytes(raw.data(), raw.size());
    uint64_t valid_end = 0;
    while (buf.remaining() > 0) {
        std::string group, topic;
        uint32_t partition = 0;
        uint64_t offset = 0;
        if (get_string(buf, group) != Status::OK) break;
        if (get_string(buf, topic) != Status::OK) break;
        if (buf.get_u32(partition) != Status::OK) break;
        if (buf.get_u64(offset) != Status::OK) break;
        offsets_[{group, topic, partition}] = offset;
        valid_end = raw.size() - buf.remaining();
    }
    // Drop a torn tail entry (crash mid-commit).
    if (valid_end < raw.size()) {
        if (::ftruncate(fd, static_cast<off_t>(valid_end)) != 0) {
            ::close(fd);
            return Status::IO_ERROR;
        }
    }
    fd_ = fd;
    file_size_ = valid_end;
    return Status::OK;
}

Status OffsetStore::commit(const std::string& group, const std::string& topic,
                           uint32_t partition, uint64_t offset) {
    std::lock_guard<std::mutex> lock(mu_);
    if (fd_ < 0) return Status::IO_ERROR;

    ByteBuffer buf;
    put_string(buf, group);
    put_string(buf, topic);
    buf.put_u32(partition);
    buf.put_u64(offset);

    const auto& bytes = buf.data();
    size_t done = 0;
    while (done < bytes.size()) {
        ssize_t n = ::pwrite(fd_, bytes.data() + done, bytes.size() - done,
                             static_cast<off_t>(file_size_ + done));
        if (n < 0) return Status::IO_ERROR;
        done += static_cast<size_t>(n);
    }
    if (::fsync(fd_) != 0) return Status::IO_ERROR;
    file_size_ += bytes.size();
    offsets_[{group, topic, partition}] = offset;
    return Status::OK;
}

uint64_t OffsetStore::fetch(const std::string& group, const std::string& topic,
                            uint32_t partition) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = offsets_.find({group, topic, partition});
    return it == offsets_.end() ? 0 : it->second;
}

}
