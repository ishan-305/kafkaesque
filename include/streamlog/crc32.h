#pragma once
#include <cstdint>
#include <cstddef>

namespace streamlog {
    uint32_t crc32(const uint8_t* data,size_t length);
}