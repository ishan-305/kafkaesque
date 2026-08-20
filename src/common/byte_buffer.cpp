#include "streamlog/byte_buffer.h"

namespace streamlog{

    void ByteBuffer::put_u32(uint32_t value){
        for(int i=0;i<4;i++){
            buffer_.push_back(static_cast<uint8_t>((value >> (i*8)) & 0xFF));
        }
    }

    void ByteBuffer::put_u64(uint64_t value){
        for(int i=0;i<8;i++){
            buffer_.push_back(static_cast<uint8_t>((value >> (i*8)) & 0xFF));
        }
    }

    void ByteBuffer::put_bytes(const uint8_t* data , size_t length){
        buffer_.insert(buffer_.end(), data, data + length);
    }

    Status ByteBuffer::get_u32(uint32_t& value){
        if (remaining() < 4) return Status::SHORT_READ;
        value = uint32_t(buffer_[read_ptr_]) |
                (uint32_t(buffer_[read_ptr_ + 1]) << 8) |
                (uint32_t(buffer_[read_ptr_ + 2]) << 16) |
                (uint32_t(buffer_[read_ptr_ + 3]) << 24);
        read_ptr_ += 4;
        return Status::OK;
    }

    Status ByteBuffer::get_u64(uint64_t& value){
        if (remaining() < 8) return Status::SHORT_READ;
        value = uint64_t(buffer_[read_ptr_]) |
                (uint64_t(buffer_[read_ptr_ + 1]) << 8) |
                (uint64_t(buffer_[read_ptr_ + 2]) << 16) |
                (uint64_t(buffer_[read_ptr_ + 3]) << 24) |
                (uint64_t(buffer_[read_ptr_ + 4]) << 32) |
                (uint64_t(buffer_[read_ptr_ + 5]) << 40) |
                (uint64_t(buffer_[read_ptr_ + 6]) << 48) |
                (uint64_t(buffer_[read_ptr_ + 7]) << 56);
        read_ptr_ += 8;
        return Status::OK;
    }

    Status ByteBuffer::get_bytes(uint8_t* value, size_t len){
        if (remaining() < len) return Status::SHORT_READ;
        std::copy(buffer_.begin() + read_ptr_, buffer_.begin() + read_ptr_ + len, value);
        read_ptr_ += len;
        return Status::OK;
    }    
}