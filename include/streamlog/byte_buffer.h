#pragma once
#include <cstdint>
#include <cstddef>
#include <vector>
#include "streamlog/errors.h"

namespace streamlog {
    class ByteBuffer{
        public:
            void put_u32(uint32_t value);
            void put_u64(uint64_t value);
            void put_bytes(const uint8_t* data, size_t length);

            streamlog::Status get_u32(uint32_t& value);
            streamlog::Status get_u64(uint64_t& value);
            streamlog::Status get_bytes(uint8_t* data, size_t length);

            size_t remaining() const{
                return buffer_.size() - read_ptr_;
            }

            const std::vector<uint8_t>& data() const{
                return buffer_;
            }
        private:
            std::vector<uint8_t> buffer_;
            size_t read_ptr_ = 0;
    };
}
