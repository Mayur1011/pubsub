#include "net/protocol.hpp"
#include "storage/partition.hpp"
#include "storage/record.hpp"
#include <arpa/inet.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

using namespace pubsub::net;

int connect_to_broker() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(6969);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Failed to connect to broker. Is it running?\n";
        exit(1);
    }
    return sock;
}
bool recv_exact(int fd, uint8_t *dst, size_t n) {
    size_t got = 0;
    while (got < n) {
        ssize_t r = recv(fd, dst + got, n - got, 0);
        if (r <= 0)
            return false;
        got += static_cast<size_t>(r);
    }
    return true;
}
void send_produce(int sock, const std::string &topic, uint32_t partition, const std::string &value, uint32_t corr_id) {
    // send_produce(sock, "orders", 0, "Order #1001: Shoes", 101);
    // recordbatch
    pubsub::storage::Record record{0, "k", value};

    pubsub::storage::RecordBatch batch{};
    batch.batchStart = 0xAA;
    batch.baseOffset = 0; // server overwrites
    batch.batchLen = 0;   // serializeRecordBatch computes it
    batch.timeStamp = 1719324000;
    batch.numRecords = 1;
    batch.records.push_back(record);

    std::vector<uint8_t> rawBatch = pubsub::storage::serializeRecordBatch(batch);
    // Full frame = [frameLen:u32][body...], frameLen excludes first 4 bytes

    RequestHeader header{};
    header.frameLen = 0;
    header.requestType = RequestType::PRODUCE;
    std::cout << "[Client] Sending " << static_cast<int>(header.requestType) << " " << topic << "-" << partition
              << " corrId=" << corr_id << "\n";
    header.clientId = "test_cleint";
    header.correlationId = corr_id;
    ProducePayload payload{};
    payload.topic = topic;
    payload.partitionID = partition;
    payload.acks = 1;
    payload.rawRecordBatch = rawBatch;
    std::vector<uint8_t> frame = serializeProduceRequest(header, payload);
    if (send(sock, frame.data(), frame.size(), 0) <= 0) {
        std::cerr << "[Client] Failed to send PRODUCE on " << topic << "-" << partition << "\n";
        return;
    }
    // ---------- Read framed ACK ----------
    uint32_t resp_len = 0;
    if (!recv_exact(sock, reinterpret_cast<uint8_t *>(&resp_len), sizeof(resp_len))) {
        std::cerr << "[Client] Failed to read ACK length\n";
        return;
    }

    std::vector<uint8_t> resp(resp_len);
    if (!recv_exact(sock, resp.data(), resp.size())) {
        std::cerr << "[Client] Failed to read ACK payload\n";
        return;
    }

    if (resp.size() >= 5) {
        uint8_t err = resp[0];
        uint32_t ack_corr = 0;
        std::memcpy(&ack_corr, resp.data() + 1, sizeof(uint32_t));

        std::cout << "[Client] ACK PRODUCE " << topic << ":" << partition << " corrId=" << ack_corr
                  << " err=" << static_cast<int>(err) << "\n";
    } else {
        std::cerr << "[Client] Invalid ACK frame\n";
    }
}

int main() {
    int sock = connect_to_broker();

    std::cout << "--- Step 1: Testing Multi-Topic Routing ---\n";
    // Send message to orders partition 0
    send_produce(sock, "orders", 0, "Order #1001: Shoes", 101);
    // Send message to payments partition 0 (Should map to a different folder entirely!)
    send_produce(sock, "payments", 0, "Payment #5501: Success", 102);

    std::cout << "\n--- Step 2: Testing Log Rotation ---\n";
    std::cout << "Blasting messages to 'orders' partition 0 to force a file split...\n";

    // Generate long string data to quickly hit our temporary 5KB limit
    std::string huge_payload(1024, 'A'); // 1KB string
    for (int i = 0; i < 6; ++i) {
        send_produce(sock, "orders", 0, "Huge Data Burst " + std::to_string(i) + " " + huge_payload, 200 + i);
    }

    close(sock);
    std::cout << "\nTest client execution complete. Check your broker state!\n";
    return 0;
}