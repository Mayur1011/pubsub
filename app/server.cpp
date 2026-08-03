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
#include <utility>
#include <vector>

#include "concurrency/diskRequest.hpp"
#include "concurrency/groupCoord.hpp"
#include "concurrency/workerPool.hpp"
#include "net/connBuff.hpp"
#include "net/protocol.hpp"
#include "storage/partition.hpp"
#include "storage/record.hpp"
#include "storage/topicManager.hpp"

using namespace pubsub::net;

std::unique_ptr<pubsub::storage::TopicManager> currTopicManager;
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
    while (!conn->outBuffer.empty()) {
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
    epollEvent.data.ptr = conn;
    epollEvent.events = EPOLLIN | EPOLLET;
    epoll_ctl(epollFD, EPOLL_CTL_MOD, conn->fd, &epollEvent);
}
void process_frame(ClientState *conn, std::vector<uint8_t> &frame_vec, int epollFD) {
    size_t offset = 4; // first 4 bytes are FrameLength
    try {
        RequestHeader header = deserializeRequestHeader(frame_vec, offset);
        pubsub::concurrency::DiskRequest diskRequest;
        diskRequest.partitionID = 0; // hardcoded for now
        diskRequest.correlationID = header.correlationId;
        diskRequest.clientFD = conn->fd; // dont need to send this as we are now using callback (sending for debugging)
        if (header.requestType == RequestType::PRODUCE) {
            ProducePayload reqPayload = deserializeProducePayload(frame_vec, offset);
            std::cout << "[net/server] PRODUCE from " << header.clientId << " on " << reqPayload.topic << "\n";
            diskRequest.type = pubsub::concurrency::TaskType::PRODUCE;
            diskRequest.topicName = reqPayload.topic;
            diskRequest.partitionID = reqPayload.partitionID;
            diskRequest.produceBatch = pubsub::storage::deserializeRecordBatch(reqPayload.rawRecordBatch);
        } else if (header.requestType == RequestType::FETCH) {
            FetchPayload reqPayload = deserializeFetchPayload(frame_vec, offset);
            std::cout << "[net/server] FETCH from " << header.clientId << " on " << reqPayload.topic << "\n";
            diskRequest.type = pubsub::concurrency::TaskType::FETCH;
            diskRequest.topicName = reqPayload.topic;
            diskRequest.partitionID = reqPayload.partitionID;
            diskRequest.fetchOffset = reqPayload.fetchOffset;
        } else if (header.requestType == RequestType::COMMIT_LOG_OFFSET) {
            OffsetCommitPayload reqPayload = deserializeOffsetCommitPayload(frame_vec, offset);
            std::cout << "[net/server] OFFSET_COMMIT from " << header.clientId << " for group " << reqPayload.groupID
                      << "\n";
            diskRequest.type = pubsub::concurrency::TaskType::COMMIT_LOG_OFFSET;
            diskRequest.groupID = reqPayload.groupID;
            diskRequest.topicName = reqPayload.topic;
            diskRequest.partitionID = reqPayload.partitionID;
            diskRequest.committedLogOffset = reqPayload.committedLogOffset;
        } else if (header.requestType == RequestType::FETCH_LOG_OFFSET) {
            OffsetFetchPayload reqPayload = deserializeOffsetFetchPayload(frame_vec, offset);
            std::cout << "[net/server] OFFSET_FETCH from " << header.clientId << " for group " << reqPayload.groupID
                      << "\n";
            diskRequest.type = pubsub::concurrency::TaskType::FETCH_LOG_OFFSET;
            diskRequest.groupID = reqPayload.groupID;
            diskRequest.topicName = reqPayload.topic;
            diskRequest.partitionID = reqPayload.partitionID;
        } else if (header.requestType == RequestType::CONSUMER_HEARTBEAT) {
            HeartBeatPayload reqPayload = deserializeHeartBeatPayload(frame_vec, offset);
            diskRequest.type = pubsub::concurrency::TaskType::CONSUMER_HEARTBEAT;
            diskRequest.groupID = reqPayload.groupID;
            diskRequest.memberID = reqPayload.memberID;
            diskRequest.generationID = reqPayload.generationID;
        }
        if (currTopicManager->getPartition(diskRequest.topicName, diskRequest.partitionID) == nullptr) {
            std::cerr << "[net/server] Rejecting request: Topic/Partition target not found: " << diskRequest.topicName
                      << ":" << diskRequest.partitionID << "\n";
            auto response = serializeResponse(header.correlationId, ErrorCode::UNKNOWN_SERVER_ERROR, {});
            send(conn->fd, response.data(), response.size(), 0);
            return;
        }
        // std::vector<uint8_t> empty_payload;
        // auto response = serializeResponse(header.correlationId, ErrorCode::NONE, empty_payload);
        // send(conn->fd, response.data(), response.size(), 0);
        diskRequest.sendResponse = [conn, epollFD](std::vector<uint8_t> responseData) {
            std::lock_guard<std::mutex> lock(conn->outBufferMutex);
            // std::cerr << "[net/server]: in callback Sending response of size " << responseData.size() << "\n";
            conn->outBuffer.push(std::move(responseData));
            // now notify the event loop to wake as we have data to write
            epoll_event epollEvent;
            epollEvent.events = EPOLLIN | EPOLLOUT | EPOLLET;
            epollEvent.data.ptr = conn;
            epoll_ctl(epollFD, EPOLL_CTL_MOD, conn->fd, &epollEvent);
        };
        // std::cerr << "[net/server] routing request to worker pool\n";
        currWorkerPool->routeRequest(std::move(diskRequest));
    } catch (const std::exception &e) {
        std::cerr << "[net/server] Protocol error: " << e.what() << "\n";
    }
}
void handleEpollReads(ClientState *conn, int epollFD) {
    uint8_t read_buf[4096];
    while (true) {
        ssize_t bytes_read = recv(conn->fd, read_buf, sizeof(read_buf), 0);
        if (bytes_read > 0) {
            conn->buffer.append(read_buf, bytes_read);
            // Process ALL complete frames currently present in the buffer
            // std::cout << "[epoll] Read " << bytes_read << " bytes from client (fd " << conn->fd << ")\n";
            std::vector<uint8_t> frame;
            while (conn->buffer.read_frame(frame)) {
                process_frame(conn, frame, epollFD);
            }
            // std::cout << "[epoll] Processed " << frame.size() << " bytes from client (fd " << conn->fd << ")\n";
        } else if (bytes_read == 0) {
            // client has closed the connection, so we need to remove it from epoll
            std::cout << "[epoll] Client disconnected (fd " << conn->fd << ")\n";
            epoll_ctl(epollFD, EPOLL_CTL_DEL, conn->fd, nullptr);
            close(conn->fd);
            // If a worker thread is still processing a request for this client,
            // the pointer 'conn' in the lambda will become dangling here.
            // Proper cleanup requires cancelling the task or using shared_ptr.
            delete conn;
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // We have completely drained the kernel socket buffer.
                // Return to epoll_wait and let it notify us when MORE data arrives.
                return;
            } else {
                std::cerr << "[epoll] Socket error on fd " << conn->fd << "\n";
                epoll_ctl(epollFD, EPOLL_CTL_DEL, conn->fd, nullptr);
                close(conn->fd);
                delete conn;
                return;
            }
        }
    }
}
int main() {
    try {
        std::cout << "[net/server]: Initialzing topic manager...\n";
        currTopicManager = std::make_unique<pubsub::storage::TopicManager>("./data");
        std::cout << "[net/server]: Topic manager initialized.\n";
    } catch (const std::exception &e) {
        std::cerr << "[net/server]: Failed to initialize topic manager: " << e.what() << '\n';
        return 1;
    }
    auto currGroupCoordinator = std::make_unique<pubsub::concurrency::GroupCoord>();
    currWorkerPool =
        std::make_unique<pubsub::concurrency::WorkerPool>(5, currTopicManager.get(), currGroupCoordinator.get());
    currWorkerPool->start();
    std::cout << "[net/server]: Worker pool initialized.\n";
    std::cout << "[net/server]: Recovering partitions...\n";
    std::vector<pubsub::storage::RecoveryInfo> recoveryInfo = currTopicManager->recoverTopics();
    currTopicManager->initializeOffsetPartition();
    currTopicManager->recoverCommitLogOffset();
    if (recoveryInfo.empty()) {
        std::cout << "[net/server] No early partitions recovered. Making dummy partitions...\n";
        currTopicManager->createTopic("orders", 1);
        currWorkerPool->registerPartition("orders", 0, currTopicManager->getPartition("orders", 0));
        currTopicManager->createTopic("payments", 1);
        currWorkerPool->registerPartition("payments", 0, currTopicManager->getPartition("payments", 0));
    } else {
        for (const pubsub::storage::RecoveryInfo &info : recoveryInfo) {
            currWorkerPool->registerPartition(info.topicName, info.partitionId, info.partition);
        }
    }
    std::cout << "[net/server]: Registering offset partitions to workerpool.\n";
    for (uint32_t i = 0; i < 50; i++) {
        currWorkerPool->registerPartition("consumer_offsets", i, currTopicManager->getPartition("consumer_offsets", i));
    }

    int serverFD = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    int opt = 1;
    if (setsockopt(serverFD, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        std::cerr << "[net/server]: Failed to set SO_REUSEADDR: " << strerror(errno) << "\n";
        return 1;
    }
    if (setsockopt(serverFD, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) == -1) {
        std::cerr << "[net/server]: Failed to set SO_REUSEPORT: " << strerror(errno) << "\n";
        return 1;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(6969);
    if (bind(serverFD, (struct sockaddr *)&address, sizeof(address)) == -1) {
        std::cerr << "[net/server]: Server bind to addr failed: " << strerror(errno) << "\n";
        return 1;
    }
    listen(serverFD, SOMAXCONN);
    // epoll better to handle multiple connections. creates a epoll instance which manages the sockets that i want to
    // monitor. and epoll_fd is fd of the epoll instance.
    int epollFD = epoll_create1(EPOLL_CLOEXEC); // refer man epoll
    epoll_event epollEvent{};
    epollEvent.events = EPOLLIN;
    epollEvent.data.fd = serverFD;
    if (epoll_ctl(epollFD, EPOLL_CTL_ADD, serverFD, &epollEvent) == -1) {
        std::cerr << "[net/server]: epoll_ctl failed: " << strerror(errno) << "\n";
        return 1;
    }
    std::cout << "Epoll server listening on 6969...\n";
    while (true) {
        epoll_event events[MAX_EVENTS];
        int n = epoll_wait(epollFD, events, MAX_EVENTS, -1);
        for (int i = 0; i < n; i++) {
            // it the event is on the listening socket, that means a new connection is ready to be accepted
            if (events[i].data.fd == serverFD) {
                int client_fd = accept4(serverFD, nullptr, nullptr, SOCK_NONBLOCK);
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
                epoll_ctl(epollFD, EPOLL_CTL_ADD, client_fd, &cev);
                std::cout << "[net/server]: New client connected (fd " << client_fd << ")\n";
            } else {
                ClientState *conn = static_cast<ClientState *>(events[i].data.ptr);
                if (events[i].events & EPOLLOUT) {
                    std::cerr << "[net/server]: EPOLLOUT event received\n";
                    sendResponse(conn, epollFD);
                } else
                    handleEpollReads(conn, epollFD);
            }
        }
    }
    close(serverFD);
    close(epollFD);
    return 0;
}