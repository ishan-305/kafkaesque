#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <semaphore>

#include "streamlog/broker.h"

namespace {
std::binary_semaphore shutdown_sem(0);
void on_signal(int) { shutdown_sem.release(); }
}

int main(int argc, char** argv) {
    int port = 9092;
    std::string data_dir = "./data";
    uint32_t partitions = 1;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (std::strcmp(argv[i], "--default-partitions") == 0 && i + 1 < argc) {
            partitions = static_cast<uint32_t>(std::atoi(argv[++i]));
        } else {
            std::fprintf(stderr,
                         "usage: %s [--port N] [--data-dir DIR] "
                         "[--default-partitions N]\n",
                         argv[0]);
            return 2;
        }
    }

    streamlog::Broker broker({.data_dir = data_dir,
                              .default_partitions = partitions});
    if (broker.start(port) != streamlog::Status::OK) {
        std::fprintf(stderr, "kafkaesque: failed to start on port %d\n", port);
        return 1;
    }
    std::printf("kafkaesque broker listening on port %d, data dir %s\n",
                broker.port(), data_dir.c_str());
    std::fflush(stdout);

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    shutdown_sem.acquire();

    std::printf("kafkaesque: shutting down\n");
    broker.stop();
    return 0;
}
