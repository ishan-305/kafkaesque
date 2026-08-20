#pragma once
#include <cstdint>
#include <string>
#include "streamlog/byte_buffer.h"
#include "streamlog/errors.h"

namespace streamlog {

// On-disk format:
//   [length:4][crc:4][timestamp:8][key_len:4][key][value_len:4][value]
// length = total bytes after the length field.
// crc covers every byte after the crc field.
// Offset is NOT stored — it is implicit from position in the segment.
struct Record {
    uint64_t timestamp = 0;
    std::string key;
    std::string value;

    bool operator==(const Record& other) const = default;
};

// On-disk byte size including the length field. Used by segment roll decisions.
uint32_t encoded_size(const Record& r);

// Append the framed record bytes to `out`.
Status serialize(const Record& r, ByteBuffer& out);

// Parse one record at the cursor of `in`, verify crc.
// Returns Status::CRC on checksum mismatch, SHORT_READ if the frame is
// incomplete, CORRUPT if lengths are inconsistent.
Status deserialize(ByteBuffer& in, Record& out);

}
