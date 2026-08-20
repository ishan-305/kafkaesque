#include "streamlog/tcp_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>

namespace streamlog {

Conn::~Conn() {
    if (fd_ >= 0) ::close(fd_);
}

Status Conn::read_n(uint8_t* buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = ::read(fd_, buf + done, n - done);
        if (r == 0) return Status::SHORT_READ;  // peer closed
        if (r < 0) return Status::IO_ERROR;
        done += static_cast<size_t>(r);
    }
    return Status::OK;
}

Status Conn::write_all(const uint8_t* buf, size_t n) {
    size_t done = 0;
    while (done < n) {
        ssize_t r = ::write(fd_, buf + done, n - done);
        if (r < 0) return Status::IO_ERROR;
        done += static_cast<size_t>(r);
    }
    return Status::OK;
}

Status Conn::read_frame_bytes(ByteBuffer& out) {
    uint8_t len_bytes[4];
    Status s = read_n(len_bytes, 4);
    if (s != Status::OK) return s;

    ByteBuffer len_buf;
    len_buf.put_bytes(len_bytes, 4);
    uint32_t len = 0;
    len_buf.get_u32(len);
    // 64 MB frame cap: refuse absurd lengths instead of allocating them.
    if (len == 0 || len > 64u * 1024 * 1024) return Status::CORRUPT;

    std::vector<uint8_t> body(len);
    s = read_n(body.data(), len);
    if (s != Status::OK) return s;

    out = ByteBuffer();
    out.put_bytes(len_bytes, 4);
    out.put_bytes(body.data(), body.size());
    return Status::OK;
}

TcpServer::~TcpServer() { stop(); }

Status TcpServer::listen(int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return Status::IO_ERROR;

    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return Status::IO_ERROR;
    }
    if (::listen(fd, 128) != 0) {
        ::close(fd);
        return Status::IO_ERROR;
    }

    // Recover the actual port when 0 was requested (tests).
    sockaddr_in bound{};
    socklen_t blen = sizeof(bound);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &blen) == 0) {
        port_ = ntohs(bound.sin_port);
    } else {
        port_ = port;
    }

    listen_fd_ = fd;
    stopping_ = false;
    return Status::OK;
}

void TcpServer::serve(std::function<void(Conn&)> handler) {
    while (!stopping_) {
        int cfd = ::accept(listen_fd_, nullptr, nullptr);
        if (cfd < 0) {
            if (stopping_) break;
            continue;  // transient accept error
        }
        int one = 1;
        ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        {
            std::lock_guard<std::mutex> lock(mu_);
            conn_fds_.push_back(cfd);
            threads_.emplace_back([this, handler, cfd]() {
                Conn conn(cfd);  // closes fd on destruction
                handler(conn);
                // Deregister before the fd is closed so stop() never
                // shutdown()s a number the OS may have reused.
                std::lock_guard<std::mutex> inner(mu_);
                conn_fds_.erase(
                    std::remove(conn_fds_.begin(), conn_fds_.end(), cfd),
                    conn_fds_.end());
            });
        }
    }
}

void TcpServer::stop() {
    if (stopping_.exchange(true)) {
        // Already stopped; still make sure threads are joined.
    }
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    std::vector<std::thread> threads;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (int fd : conn_fds_) ::shutdown(fd, SHUT_RDWR);
        conn_fds_.clear();
        threads.swap(threads_);
    }
    for (std::thread& t : threads) {
        if (t.joinable()) t.join();
    }
}

}
