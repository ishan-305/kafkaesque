#include "streamlog/record.h"
#include "streamlog/crc32.h"

#include <vector>

namespace streamlog {

namespace {
// Bytes after the crc field: timestamp + key_len + key + value_len + value.
uint32_t body_size(const Record& r) {
    return 8 + 4 + static_cast<uint32_t>(r.key.size()) + 4 +
           static_cast<uint32_t>(r.value.size());
}
}

uint32_t encoded_size(const Record& r) {
    // length field + crc field + body
    return 4 + 4 + body_size(r);
}

Status serialize(const Record& r, ByteBuffer& out) {
    ByteBuffer body;
    body.put_u64(r.timestamp);
    body.put_u32(static_cast<uint32_t>(r.key.size()));
    body.put_bytes(reinterpret_cast<const uint8_t*>(r.key.data()), r.key.size());
    body.put_u32(static_cast<uint32_t>(r.value.size()));
    body.put_bytes(reinterpret_cast<const uint8_t*>(r.value.data()), r.value.size());

    const std::vector<uint8_t>& body_bytes = body.data();
    uint32_t crc = crc32(body_bytes.data(), body_bytes.size());

    out.put_u32(4 + static_cast<uint32_t>(body_bytes.size()));  // length after this field
    out.put_u32(crc);
    out.put_bytes(body_bytes.data(), body_bytes.size());
    return Status::OK;
}

Status deserialize(ByteBuffer& in, Record& out) {
    uint32_t length = 0;
    Status s = in.get_u32(length);
    if (s != Status::OK) return s;
    // Minimum: crc(4) + timestamp(8) + key_len(4) + value_len(4)
    if (length < 20) return Status::CORRUPT;
    if (in.remaining() < length) return Status::SHORT_READ;

    uint32_t crc = 0;
    s = in.get_u32(crc);
    if (s != Status::OK) return s;

    uint32_t body_len = length - 4;
    std::vector<uint8_t> body(body_len);
    s = in.get_bytes(body.data(), body_len);
    if (s != Status::OK) return s;

    if (crc32(body.data(), body.size()) != crc) return Status::CRC;

    ByteBuffer body_buf;
    body_buf.put_bytes(body.data(), body.size());

    Record r;
    s = body_buf.get_u64(r.timestamp);
    if (s != Status::OK) return Status::CORRUPT;

    uint32_t key_len = 0;
    s = body_buf.get_u32(key_len);
    if (s != Status::OK || key_len > body_buf.remaining()) return Status::CORRUPT;
    r.key.resize(key_len);
    if (key_len > 0) {
        s = body_buf.get_bytes(reinterpret_cast<uint8_t*>(r.key.data()), key_len);
        if (s != Status::OK) return Status::CORRUPT;
    }

    uint32_t value_len = 0;
    s = body_buf.get_u32(value_len);
    if (s != Status::OK || value_len != body_buf.remaining()) return Status::CORRUPT;
    r.value.resize(value_len);
    if (value_len > 0) {
        s = body_buf.get_bytes(reinterpret_cast<uint8_t*>(r.value.data()), value_len);
        if (s != Status::OK) return Status::CORRUPT;
    }

    out = std::move(r);
    return Status::OK;
}

}
