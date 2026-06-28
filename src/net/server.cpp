#include "net/connBuff.hpp"
#include "net/protocol.hpp"
#include "storage/partition.hpp"
#include "storage/record.hpp"
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace pubsub::net;

std::unique_ptr<pubsub::storage::Partition> currPartition;

// const is not validated at compile time, constexpr is.
constexpr int MAX_EVENTS = 64;

// Handles the buffer and file descriptor for a single client
struct ClientState {
    int fd;
    ConnectionBuffer buffer;
    explicit ClientState(int socket_fd) : fd(socket_fd) {}
};

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Same logic as 2.2, just refactored into a clean function
void process_frame(ClientState *conn, std::span<uint8_t> frame) {
    std::vector<uint8_t> frame_vec(frame.begin(), frame.end());
    size_t offset = 4; // first 4 bytes are FrameLength

    try {
        RequestHeader header = deserializeRequestHeader(frame_vec, offset);

        if (header.requestType == RequestType::PRODUCE) {
            ProducePayload reqPayload = deserializeProducePayload(frame_vec, offset);
            std::cout << "[net/server] PRODUCE from " << header.clientId << " on " << reqPayload.topic << "\n";
            try {
                std::cout << "[net/server] rawRecordBatch size: " << reqPayload.rawRecordBatch.size() << "\n";
                pubsub::storage::RecordBatch recordBatch =
                    pubsub::storage::deserializeRecordBatch(reqPayload.rawRecordBatch);
                std::cout << "[net/server] recordBatch numRecords: " << recordBatch.numRecords << "\n";
                currPartition->append(recordBatch);
                std::cout << "[net/server] Appended " << recordBatch.numRecords << " records to partition\n";
                std::vector<uint8_t> response =
                    serializeResponse(header.correlationId, ErrorCode::NONE, std::vector<uint8_t>());
                send(conn->fd, response.data(), response.size(), 0);
            } catch (const std::exception &e) {
                std::cerr << "[net/server] Write failed: " << e.what() << "\n";
                auto response =
                    serializeResponse(header.correlationId, ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>());
                send(conn->fd, response.data(), response.size(), 0);
            }
        } else if (header.requestType == RequestType::FETCH) {
            FetchPayload reqPayload = deserializeFetchPayload(frame_vec, offset);
            std::cout << "[net/server] FETCH from " << header.clientId << " at offset " << reqPayload.fetchOffset
                      << "\n";
            try {
                pubsub::storage::Record record;
                if (currPartition->read(reqPayload.fetchOffset, record)) {
                    std::vector<uint8_t> serializedRecord = pubsub::storage::serializeRecord(record);
                    auto response = serializeResponse(header.correlationId, ErrorCode::NONE, serializedRecord);
                    send(conn->fd, response.data(), response.size(), 0);
                }
            } catch (const std::exception &e) {
                std::cerr << "[net/server] Fetch failed: " << e.what() << "\n";
                auto response =
                    serializeResponse(header.correlationId, ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>());
                send(conn->fd, response.data(), response.size(), 0);
            }
        }

        // // Send dummy response for now
        // std::vector<uint8_t> empty_payload;
        // auto response = serializeResponse(header.correlationId, ErrorCode::NONE, empty_payload);
        // send(conn->fd, response.data(), response.size(), 0);

    } catch (const std::exception &e) {
        std::cerr << "[net/server] Protocol error: " << e.what() << "\n";
    }
}

// The Edge-Triggered Read Loop
void handle_readable(ClientState *conn, int epoll_fd) {
    uint8_t read_buf[4096];

    while (true) {
        ssize_t bytes_read = recv(conn->fd, read_buf, sizeof(read_buf), 0);

        if (bytes_read > 0) {
            conn->buffer.append(read_buf, bytes_read);

            // Process ALL complete frames currently sitting in the buffer
            while (auto frame = conn->buffer.try_read_frame()) {
                process_frame(conn, *frame);
                conn->buffer.consume(frame->size());
            }
        } else if (bytes_read == 0) {
            // Client gracefully disconnected
            std::cout << "[epoll] Client disconnected (fd " << conn->fd << ")\n";
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->fd, nullptr);
            close(conn->fd);
            delete conn;
            return;
        } else {
            // bytes_read < 0
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // We have completely drained the kernel socket buffer.
                // Return to epoll_wait and let it notify us when MORE data arrives.
                return;
            } else {
                // A real network error occurred
                std::cerr << "[epoll] Socket error on fd " << conn->fd << "\n";
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->fd, nullptr);
                close(conn->fd);
                delete conn;
                return;
            }
        }
    }
}

int main() {
    try {
        std::cout << "[net/server]: Initialzing storage partition...\n";
        currPartition = std::make_unique<pubsub::storage::Partition>("./data_storage");
        std::cout << "[net/server]: Storage partition initialized.\n";
    } catch (const std::exception &e) {
        std::cerr << "Fatal Error: " << e.what() << '\n';
        return 1;
    }
    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(6969);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) == -1) {
        std::cerr << "[epoll] Bind failed: " << strerror(errno) << "\n";
        return 1;
    }
    listen(server_fd, SOMAXCONN);

    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::cout << "🚀 High-Performance epoll server listening on 6969...\n";

    while (true) {
        epoll_event events[MAX_EVENTS];
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);

        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == server_fd) {
                // Handle new incoming connections
                int client_fd = accept4(server_fd, nullptr, nullptr, SOCK_NONBLOCK);
                if (client_fd < 0)
                    continue;

                int flag = 1;
                setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

                // Create heap state to track this connection's buffer over time
                ClientState *conn = new ClientState(client_fd);

                epoll_event cev{};
                cev.events = EPOLLIN | EPOLLET; // Edge-triggered
                cev.data.ptr = conn;            // Attach our state struct to the event
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &cev);

                std::cout << "[epoll] New client connected (fd " << client_fd << ")\n";
            } else {
                // Handle existing client sending us data
                auto *conn = static_cast<ClientState *>(events[i].data.ptr);
                handle_readable(conn, epoll_fd);
            }
        }
    }

    close(server_fd);
    close(epoll_fd);
    return 0;
}