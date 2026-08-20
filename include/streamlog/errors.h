#pragma once
namespace streamlog {
    enum class Status{
        OK,
        CRC,
        SHORT_READ,
        CORRUPT,
        NOT_FOUND,
        INVALID_ARGUMENT,
        IO_ERROR
    };
}    