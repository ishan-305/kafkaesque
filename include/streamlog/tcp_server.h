#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "streamlog/byte_buffer.h"
#include "streamlog/errors.h"

namespace streamlog {

// One socket. Bytes in/out only — no protocol knowledge beyond the
// [len:4][rest...] framing helper.
class Conn {
public:
    explicit Conn(int fd) : fd_(fd) {}
    ~Conn();
    Conn(const Conn&) = delete;
    Conn& operator=(const Conn&) = delete;

    Status read_n(uint8_t* buf, size_t n);
    Status write_all(const uint8_t* buf, size_t n);

    // Read one complete frame: the 4-byte length prefix plus that many bytes.
    // `out` receives the whole frame (length prefix included) ready for
    // protocol read_frame().
    Status read_frame_bytes(ByteBuffer& out);

    int fd() const { return fd_; }

private:
    int fd_ = -1;
};

// v1: thread per connection. Known bottleneck; acceptable for correctness.
class TcpServer {
public:
    ~TcpServer();

    Status listen(int port);

    // Blocking accept loop; spawns one thread per connection running
    // handler(conn). Returns when stop() is called.
    void serve(std::function<void(Conn&)> handler);

    // Close the listening socket and all live connections; join threads.
    void stop();

    int port() const { return port_; }

private:
    int listen_fd_ = -1;
    int port_ = 0;
    std::atomic<bool> stopping_{false};
    std::mutex mu_;
    std::vector<std::thread> threads_;
    std::vector<int> conn_fds_;
};

}
