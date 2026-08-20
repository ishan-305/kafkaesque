#include "streamlog/crc32.h"
#include <array>

namespace{
    constexpr uint32_t  POLYNOMIAL = 0xEDB88320;
    std::array<uint32_t, 256> generate_table(){
        std::array<uint32_t, 256> table{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t crc = i;
            for (int j = 0; j < 8; ++j) {
                if (crc & 1) {
                    crc = (crc >> 1) ^ POLYNOMIAL;
                } else {
                    crc >>= 1;
                }
            }
            table[i] = crc;
        }
        return table;
    }

    const std::array<uint32_t, 256>& get_table(){
        static const std::array<uint32_t, 256> table = generate_table();
        return table;
    }
}
namespace streamlog{
    uint32_t crc32(const uint8_t* data, size_t length) {
        uint32_t crc = 0xFFFFFFFF;
        const auto& table = get_table();
        for (size_t i = 0; i < length; ++i) {
            uint8_t byte = data[i];
            uint32_t index = (crc ^ byte) & 0xFF;
            crc = (crc >> 8) ^ table[index];
        }
        return ~crc;
    }
}