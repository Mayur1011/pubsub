#include "net/protocol.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace pubsub::net;

const int NUM_PRODUCERS = 2;
const int NUM_CONSUMERS = 2;
const int MESSAGES_PER_CLIENT = 10;

template <typename T> void append_bytes(std::vector<uint8_t> &buf, const T &v) {
    const uint8_t *p = reinterpret_cast<const uint8_t *>(&v);
    buf.insert(buf.end(), p, p + sizeof(T));
}

void append_string_u16(std::vector<uint8_t> &buf, const std::string &s) {
    uint16_t len = static_cast<uint16_t>(s.size());
    append_bytes<uint16_t>(buf, len);
    buf.insert(buf.end(), s.begin(), s.end());
}

bool send_all(int sock, const uint8_t *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sock, data + sent, len - sent, 0);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return false;
        }
        if (n == 0)
            return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool recv_exact(int sock, uint8_t *dst, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = recv(sock, dst + got, len - got, 0);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return false;
        }
        if (n == 0)
            return false; // peer closed
        got += static_cast<size_t>(n);
    }
    return true;
}

// Build RecordBatch bytes matching your storage deserializeRecordBatch layout:
// [batchStart:u8][baseOffset:u64][batchLen:u32][timeStamp:u64][numRecords:u32]
// repeated record: [recordOffsetDelta:u32][keyLen:u32][key][valLen:u32][value]
std::vector<uint8_t> build_record_batch_bytes(const std::string &key, const std::string &value) {
    std::vector<uint8_t> batch;
    append_bytes<uint8_t>(batch, 0xAA); // batchStart
    append_bytes<uint64_t>(batch, 0);   // baseOffset (server overwrites)
    append_bytes<uint32_t>(batch, 0);   // batchLen placeholder

    uint64_t ts = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    append_bytes<uint64_t>(batch, ts);
    append_bytes<uint32_t>(batch, 1); // numRecords

    append_bytes<uint32_t>(batch, 0); // recordOffsetDelta
    append_bytes<uint32_t>(batch, static_cast<uint32_t>(key.size()));
    batch.insert(batch.end(), key.begin(), key.end());
    append_bytes<uint32_t>(batch, static_cast<uint32_t>(value.size()));
    batch.insert(batch.end(), value.begin(), value.end());

    // batchLen = bytes after batchLen field
    uint32_t batchLen = static_cast<uint32_t>(batch.size() - 13);
    std::memcpy(batch.data() + 9, &batchLen, sizeof(batchLen));
    return batch;
}

std::vector<uint8_t> build_produce_request(uint32_t corrIDelationId, const std::string &clientId,
                                           const std::string &topic, uint32_t partitionId, int8_t acks,
                                           const std::vector<uint8_t> &rawBatch) {
    std::vector<uint8_t> frame;
    append_bytes<uint32_t>(frame, 0); // frameLen placeholder

    append_bytes<uint8_t>(frame, static_cast<uint8_t>(RequestType::PRODUCE));
    append_bytes<uint32_t>(frame, corrIDelationId);
    append_string_u16(frame, clientId);

    append_string_u16(frame, topic);
    append_bytes<uint32_t>(frame, partitionId);
    append_bytes<int8_t>(frame, acks);
    frame.insert(frame.end(), rawBatch.begin(), rawBatch.end());

    uint32_t frameLen = static_cast<uint32_t>(frame.size() - 4);
    std::memcpy(frame.data(), &frameLen, sizeof(frameLen));
    return frame;
}

std::vector<uint8_t> build_fetch_request(uint32_t corrIDelationId, const std::string &clientId,
                                         const std::string &topic, uint32_t partitionId, uint64_t fetchOffset,
                                         uint32_t maxBytes) {
    std::vector<uint8_t> frame;
    append_bytes<uint32_t>(frame, 0); // frameLen placeholder

    append_bytes<uint8_t>(frame, static_cast<uint8_t>(RequestType::FETCH));
    append_bytes<uint32_t>(frame, corrIDelationId);
    append_string_u16(frame, clientId);

    append_string_u16(frame, topic);
    append_bytes<uint32_t>(frame, partitionId);
    append_bytes<uint64_t>(frame, fetchOffset);
    append_bytes<uint32_t>(frame, maxBytes);

    uint32_t frameLen = static_cast<uint32_t>(frame.size() - 4);
    std::memcpy(frame.data(), &frameLen, sizeof(frameLen));
    return frame;
}

bool recv_response_frame(int sock, uint32_t &corrIDId, uint8_t &errorCode, std::vector<uint8_t> &payloadOut) {
    uint32_t frameLen = 0;
    if (!recv_exact(sock, reinterpret_cast<uint8_t *>(&frameLen), sizeof(frameLen))) {
        return false;
    }

    std::vector<uint8_t> body(frameLen);
    if (!recv_exact(sock, body.data(), body.size())) {
        return false;
    }
    if (body.size() < 5) {
        return false; // must contain corrIDelationId(4) + errorCode(1)
    }
    std::memcpy(&corrIDId, body.data(), sizeof(uint32_t));
    errorCode = body[4];
    payloadOut.assign(body.begin() + 5, body.end());
    return true;
}

int connect_to_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
        return -1;
    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(6969);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

void producer_task(int client_id) {
    int sock = connect_to_server();
    if (sock < 0)
        return;
    std::string client = "producer-" + std::to_string(client_id);
    std::string topic = "test_topic";
    for (int i = 0; i < MESSAGES_PER_CLIENT; ++i) {
        uint32_t corrID = static_cast<uint32_t>(client_id * MESSAGES_PER_CLIENT + i);
        std::string key = "k-" + std::to_string(client_id) + "-" + std::to_string(i);
        std::string value = "msg-" + std::to_string(client_id) + "-" + std::to_string(i);
        auto rawBatch = build_record_batch_bytes(key, value);

        auto req = build_produce_request(corrID, client, topic, 0, 1, rawBatch);

        if (!send_all(sock, req.data(), req.size())) {
            break;
        }

        uint32_t respcorrID = 0;
        uint8_t err = 0xFF;
        std::vector<uint8_t> payload;
        if (!recv_response_frame(sock, respcorrID, err, payload)) {
            break;
        }
    }

    close(sock);
}

void consumer_task(int client_id) {
    int sock = connect_to_server();
    if (sock < 0)
        return;

    std::string client = "consumer-" + std::to_string(client_id);
    std::string topic = "test_topic";

    for (int i = 0; i < MESSAGES_PER_CLIENT; ++i) {
        uint32_t corrID = static_cast<uint32_t>(100000000 + client_id * MESSAGES_PER_CLIENT + i);
        uint64_t fetchOffset = static_cast<uint64_t>(i);

        auto req = build_fetch_request(corrID, client, topic, 0, fetchOffset, 4096);

        if (!send_all(sock, req.data(), req.size())) {
            break;
        }

        uint32_t respcorrID = 0;
        uint8_t err = 0xFF;
        std::vector<uint8_t> payload;
        if (!recv_response_frame(sock, respcorrID, err, payload)) {
            break;
        }
    }

    close(sock);
}

int main() {
    std::cout << "Starting Stress Test: " << NUM_PRODUCERS + NUM_CONSUMERS << " concurrent clients...\n";
    std::vector<std::thread> threads;
    threads.reserve(NUM_PRODUCERS + NUM_CONSUMERS);

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_PRODUCERS; ++i) {
        threads.emplace_back(producer_task, i);
    }
    for (int i = 0; i < NUM_CONSUMERS; ++i) {
        threads.emplace_back(consumer_task, i);
    }

    for (auto &t : threads) {
        t.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end_time - start_time;

    std::cout << "Stress Test Complete in " << diff.count() << " seconds.\n";
    return 0;
}
