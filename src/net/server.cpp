#include "concurrency/workerPool.hpp"
#include "net/connBuff.hpp"
#include "net/protocol.hpp"
#include "storage/partition.hpp"
#include "storage/record.hpp"
#include <asm-generic/socket.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

using namespace pubsub::net;

std::unique_ptr<pubsub::storage::Partition> currPartition;
std::unique_ptr<pubsub::concurrency::WorkerPool> currWorkerPool;

// const is not validated at compile time, constexpr is.
constexpr int MAX_EVENTS = 32; // epoll_wait will return at most 32 events at once

// Handles the buffer and file descriptor for a single client
struct ClientState {
    int fd;
    ConnectionBuffer buffer;
    std::mutex outBufferMutex;
    std::queue<std::vector<uint8_t>> outBuffer;
    explicit ClientState(int socket_fd) : fd(socket_fd) {}
};

// void set_nonblocking(int fd) {
//     int flags = fcntl(fd, F_GETFL, 0);
//     fcntl(fd, F_SETFL, flags | O_NONBLOCK);
// }

void sendResponse(ClientState *conn, int epollFD) {
    std::lock_guard<std::mutex> lock(conn->outBufferMutex);
    while (not conn->outBuffer.empty()) {
        std::vector<uint8_t> &response = conn->outBuffer.front();
        ssize_t bytesSend = send(conn->fd, response.data(), response.size(), 0);
        if (bytesSend == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            std::cerr << "[net/server] send failed: " << strerror(errno) << "\n";
            return;
        }
        conn->outBuffer.pop();
    }
    epoll_event epollEvent;
    epollEvent.data.fd = conn->fd;
    epollEvent.events = EPOLLIN | EPOLLET;
    epoll_ctl(epollFD, EPOLL_CTL_MOD, conn->fd, &epollEvent);
}

void process_frame(ClientState *conn, std::vector<uint8_t> &frame_vec, int epollFD) {
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
        // std::vector<uint8_t> empty_payload;
        // auto response = serializeResponse(header.correlationId, ErrorCode::NONE, empty_payload);
        // send(conn->fd, response.data(), response.size(), 0);
    } catch (const std::exception &e) {
        std::cerr << "[net/server] Protocol error: " << e.what() << "\n";
    }
}

void handle_readable(ClientState *conn, int epoll_fd) {
    uint8_t read_buf[4096];
    while (true) {
        ssize_t bytes_read = recv(conn->fd, read_buf, sizeof(read_buf), 0);
        if (bytes_read > 0) {
            conn->buffer.append(read_buf, bytes_read);
            // Process ALL complete frames currently present in the buffer
            // std::cout << "[epoll] Read " << bytes_read << " bytes from client (fd " << conn->fd << ")\n";
            std::vector<uint8_t> frame;
            while (conn->buffer.read_frame(frame)) {
                process_frame(conn, frame);
            }
            // std::cout << "[epoll] Processed " << frame.size() << " bytes from client (fd " << conn->fd << ")\n";
        } else if (bytes_read == 0) {
            // client has closed the connection, so we need to remove it from epoll
            std::cout << "[epoll] Client disconnected (fd " << conn->fd << ")\n";
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->fd, nullptr);
            close(conn->fd);
            delete conn;
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // We have completely drained the kernel socket buffer.
                // Return to epoll_wait and let it notify us when MORE data arrives.
                return;
            } else {
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
        std::cerr << "[net/server]: Failed to initialize storage partition: " << e.what() << '\n';
        return 1;
    }
    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        std::cerr << "[net/server]: Failed to set SO_REUSEADDR: " << strerror(errno) << "\n";
        return 1;
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) == -1) {
        std::cerr << "[net/server]: Failed to set SO_REUSEPORT: " << strerror(errno) << "\n";
        return 1;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(6969);
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) == -1) {
        std::cerr << "[net/server]: Server bind to addr failed: " << strerror(errno) << "\n";
        return 1;
    }
    listen(server_fd, SOMAXCONN);
    // epoll better to handle multiple connections. creates a epoll instance which manages the sockets that i want to
    // monitor. and epoll_fd is fd of the epoll instance.
    int epoll_fd = epoll_create1(EPOLL_CLOEXEC); // refer man epoll
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
        std::cerr << "[net/server]: epoll_ctl failed: " << strerror(errno) << "\n";
        return 1;
    }
    std::cout << "Epoll server listening on 6969...\n";
    while (true) {
        epoll_event events[MAX_EVENTS];
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < n; i++) {
            // it the event is on the listening socket, that means a new connection is ready to be accepted
            if (events[i].data.fd == server_fd) {
                int client_fd = accept4(server_fd, nullptr, nullptr, SOCK_NONBLOCK);
                if (client_fd < 0)
                    continue;
                int flag = 1;
                // disable Nagle's algorithm to reduce latency
                // (https://medium.com/@elouadinouhaila566/the-nagle-algorithm-a-simple-solution-to-a-complex-problem-0c66715663dc)
                setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
                ClientState *conn = new ClientState(client_fd);
                epoll_event cev{};
                cev.events = EPOLLIN | EPOLLET; // Edge-triggered (level-triggered)
                cev.data.ptr = conn;            // Attach our state struct to the event
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &cev);
                std::cout << "[net/server]: New client connected (fd " << client_fd << ")\n";
            } else {
                ClientState *conn = static_cast<ClientState *>(events[i].data.ptr);
                handle_readable(conn, epoll_fd);
            }
        }
    }
    close(server_fd);
    close(epoll_fd);
    return 0;
}