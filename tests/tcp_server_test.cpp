#include <gtest/gtest.h>
#include <streamlog/tcp_server.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <thread>

using streamlog::ByteBuffer;
using streamlog::Conn;
using streamlog::Status;
using streamlog::TcpServer;

namespace {
int connect_local(int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(fd, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    EXPECT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    return fd;
}
}

TEST(TcpServer, EchoRoundTrip) {
    TcpServer server;
    ASSERT_EQ(server.listen(0), Status::OK);  // ephemeral port
    ASSERT_GT(server.port(), 0);

    std::thread serve_thread([&]() {
        server.serve([](Conn& conn) {
            // Echo frames back until the peer closes.
            while (true) {
                ByteBuffer frame;
                if (conn.read_frame_bytes(frame) != Status::OK) return;
                const auto& bytes = frame.data();
                if (conn.write_all(bytes.data(), bytes.size()) != Status::OK) return;
            }
        });
    });

    {
        Conn client(connect_local(server.port()));

        ByteBuffer out;
        out.put_u32(5);  // frame length: 5 bytes follow
        const uint8_t body[5] = {'h', 'e', 'l', 'l', 'o'};
        out.put_bytes(body, 5);
        ASSERT_EQ(client.write_all(out.data().data(), out.data().size()), Status::OK);

        ByteBuffer echoed;
        ASSERT_EQ(client.read_frame_bytes(echoed), Status::OK);
        EXPECT_EQ(echoed.data(), out.data());
    }  // client closes → handler exits

    server.stop();
    serve_thread.join();
}

TEST(TcpServer, MultipleConcurrentClients) {
    TcpServer server;
    ASSERT_EQ(server.listen(0), Status::OK);

    std::thread serve_thread([&]() {
        server.serve([](Conn& conn) {
            while (true) {
                ByteBuffer frame;
                if (conn.read_frame_bytes(frame) != Status::OK) return;
                const auto& bytes = frame.data();
                if (conn.write_all(bytes.data(), bytes.size()) != Status::OK) return;
            }
        });
    });

    std::vector<std::thread> clients;
    for (int c = 0; c < 4; ++c) {
        clients.emplace_back([&server, c]() {
            Conn client(connect_local(server.port()));
            for (int i = 0; i < 50; ++i) {
                ByteBuffer out;
                out.put_u32(4);
                out.put_u32(static_cast<uint32_t>(c * 1000 + i));
                ASSERT_EQ(client.write_all(out.data().data(), out.data().size()),
                          Status::OK);
                ByteBuffer echoed;
                ASSERT_EQ(client.read_frame_bytes(echoed), Status::OK);
                EXPECT_EQ(echoed.data(), out.data());
            }
        });
    }
    for (auto& t : clients) t.join();

    server.stop();
    serve_thread.join();
}
