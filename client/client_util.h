#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <memory>
#include <string>

#include "streamlog/errors.h"
#include "streamlog/protocol.h"
#include "streamlog/tcp_server.h"

namespace streamlog::client {

inline Status tcp_connect(const std::string& host, int port,
                          std::unique_ptr<Conn>& out) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return Status::IO_ERROR;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        return Status::INVALID_ARGUMENT;  // v1: numeric IPs only
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return Status::IO_ERROR;
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    out = std::make_unique<Conn>(fd);
    return Status::OK;
}

// Send one framed request, read one framed response.
inline Status request(Conn& conn, MsgType type, const ByteBuffer& payload,
                      MsgType& resp_type, ByteBuffer& resp_payload) {
    ByteBuffer frame;
    Status s = encode_frame(type, payload, frame);
    if (s != Status::OK) return s;
    s = conn.write_all(frame.data().data(), frame.data().size());
    if (s != Status::OK) return s;

    ByteBuffer resp_frame;
    s = conn.read_frame_bytes(resp_frame);
    if (s != Status::OK) return s;
    return read_frame(resp_frame, resp_type, resp_payload);
}

// Map an ERROR response back to its Status code.
inline Status error_status(ByteBuffer& payload) {
    ErrorResp err;
    if (decode(payload, err) != Status::OK) return Status::CORRUPT;
    return static_cast<Status>(err.code);
}

}
