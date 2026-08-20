// Tiny CLI driver for milestone validation against a live broker process.
//   smoke_client produce <port> <topic> <count>
//   smoke_client consume <port> <topic> <expected_count>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "streamlog/consumer.h"
#include "streamlog/producer.h"

using namespace streamlog;

int main(int argc, char** argv) {
    if (argc != 5) {
        std::fprintf(stderr,
                     "usage: %s produce|consume <port> <topic> <count>\n",
                     argv[0]);
        return 2;
    }
    std::string mode = argv[1];
    int port = std::atoi(argv[2]);
    std::string topic = argv[3];
    uint64_t count = std::strtoull(argv[4], nullptr, 10);

    if (mode == "produce") {
        Producer p;
        if (p.connect("127.0.0.1", port) != Status::OK) {
            std::fprintf(stderr, "connect failed\n");
            return 1;
        }
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t offset = 0;
            if (p.send(topic, "key-" + std::to_string(i),
                       "value-" + std::to_string(i), offset) != Status::OK) {
                std::fprintf(stderr, "produce failed at %llu\n",
                             static_cast<unsigned long long>(i));
                return 1;
            }
        }
        std::printf("produced %llu\n", static_cast<unsigned long long>(count));
        return 0;
    }

    if (mode == "consume") {
        Consumer c;
        if (c.connect("127.0.0.1", port) != Status::OK) {
            std::fprintf(stderr, "connect failed\n");
            return 1;
        }
        if (c.subscribe(topic, 0) != Status::OK) {
            std::fprintf(stderr, "subscribe failed\n");
            return 1;
        }
        c.seek(0);
        std::vector<Record> records;
        while (c.position() < c.high_watermark()) {
            if (c.poll(records) != Status::OK) {
                std::fprintf(stderr, "poll failed\n");
                return 1;
            }
        }
        if (records.size() != count) {
            std::fprintf(stderr, "expected %llu records, got %zu\n",
                         static_cast<unsigned long long>(count), records.size());
            return 1;
        }
        for (uint64_t i = 0; i < count; ++i) {
            if (records[i].value != "value-" + std::to_string(i)) {
                std::fprintf(stderr, "order mismatch at offset %llu\n",
                             static_cast<unsigned long long>(i));
                return 1;
            }
        }
        std::printf("consumed %zu in order\n", records.size());
        return 0;
    }

    std::fprintf(stderr, "unknown mode %s\n", mode.c_str());
    return 2;
}
