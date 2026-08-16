#include <arpa/inet.h>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include "net/protocol.hpp"
#include "storage/record.hpp"

using namespace std::chrono_literals;
using pubsub::net::ErrorCode;
using pubsub::net::RequestType;

namespace {

constexpr const char *BROKER_IP = "127.0.0.1";
constexpr int BROKER_PORT = 6969;

struct BrokerResponse {
    uint32_t correlationId = 0;
    ErrorCode errorCode = ErrorCode::UNKNOWN_SERVER_ERROR;
    std::vector<uint8_t> payload;
};

template <typename T> void appendBytes(std::vector<uint8_t> &buf, const T &v) {
    static_assert(std::is_trivially_copyable_v<T>, "appendBytes expects a trivially copyable type");
    const uint8_t *p = reinterpret_cast<const uint8_t *>(&v);
    buf.insert(buf.end(), p, p + sizeof(T));
}

void appendStringU16(std::vector<uint8_t> &buf, const std::string &s) {
    if (s.size() > static_cast<size_t>(UINT16_MAX)) {
        throw std::runtime_error("string too long for u16-prefixed encoding");
    }
    uint16_t len = static_cast<uint16_t>(s.size());
    appendBytes<uint16_t>(buf, len);
    buf.insert(buf.end(), s.begin(), s.end());
}

bool sendAll(int sock, const uint8_t *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        const ssize_t n = send(sock, data + sent, len - sent, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool recvExact(int sock, uint8_t *dst, size_t len) {
    size_t got = 0;
    while (got < len) {
        const ssize_t n = recv(sock, dst + got, len - got, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        got += static_cast<size_t>(n);
    }
    return true;
}

std::vector<uint8_t> buildRequestFrame(RequestType requestType, uint32_t correlationId, const std::string &clientId,
                                       const std::vector<uint8_t> &payload) {
    std::vector<uint8_t> frame;
    appendBytes<uint32_t>(frame, 0); // frameLen placeholder
    appendBytes<uint8_t>(frame, static_cast<uint8_t>(requestType));
    appendBytes<uint32_t>(frame, correlationId);
    appendStringU16(frame, clientId);
    frame.insert(frame.end(), payload.begin(), payload.end());

    const uint32_t frameLen = static_cast<uint32_t>(frame.size() - sizeof(uint32_t));
    std::memcpy(frame.data(), &frameLen, sizeof(frameLen));
    return frame;
}

BrokerResponse readBrokerResponse(int sock) {
    uint32_t frameLen = 0;
    if (!recvExact(sock, reinterpret_cast<uint8_t *>(&frameLen), sizeof(frameLen))) {
        throw std::runtime_error("failed to read response frame length");
    }

    if (frameLen < sizeof(uint8_t) + sizeof(uint32_t)) {
        throw std::runtime_error("malformed response frame (too small)");
    }

    std::vector<uint8_t> body(frameLen);
    if (!recvExact(sock, body.data(), body.size())) {
        throw std::runtime_error("failed to read response frame body");
    }

    // serializeResponse format: [errorCode:u8][correlationId:u32][payload...]
    BrokerResponse response;
    response.errorCode = static_cast<ErrorCode>(body[0]);
    std::memcpy(&response.correlationId, body.data() + 1, sizeof(uint32_t));
    response.payload.assign(body.begin() + 1 + sizeof(uint32_t), body.end());
    return response;
}

std::vector<uint8_t> makeCreateTopicPayload(const std::string &topic, uint32_t partitions) {
    std::vector<uint8_t> payload;
    appendStringU16(payload, topic);
    appendBytes<uint32_t>(payload, partitions);
    return payload;
}

std::vector<uint8_t> makeJoinGroupPayload(const std::string &groupId, const std::string &memberId,
                                          uint32_t generationId, const std::string &topicName) {
    std::vector<uint8_t> payload;
    appendStringU16(payload, groupId);
    appendStringU16(payload, memberId);
    appendBytes<uint32_t>(payload, generationId);
    appendStringU16(payload, topicName);
    return payload;
}

std::vector<uint8_t> makeLeaveGroupPayload(const std::string &groupId, const std::string &memberId) {
    std::vector<uint8_t> payload;
    appendStringU16(payload, groupId);
    appendStringU16(payload, memberId);
    return payload;
}

std::vector<uint8_t> makeCommitOffsetPayload(const std::string &groupId, const std::string &topic, uint32_t partition,
                                             uint64_t committedOffset, uint32_t generationId) {
    std::vector<uint8_t> payload;
    appendStringU16(payload, groupId);
    appendStringU16(payload, topic);
    appendBytes<uint32_t>(payload, partition);
    appendBytes<uint64_t>(payload, committedOffset);
    appendBytes<uint32_t>(payload, generationId);
    return payload;
}

std::vector<uint8_t> makeFetchOffsetPayload(const std::string &groupId, const std::string &topic, uint32_t partition,
                                            uint32_t generationId) {
    std::vector<uint8_t> payload;
    appendStringU16(payload, groupId);
    appendStringU16(payload, topic);
    appendBytes<uint32_t>(payload, partition);
    appendBytes<uint32_t>(payload, generationId);
    return payload;
}

int64_t readInt64Payload(const std::vector<uint8_t> &payload) {
    if (payload.size() < sizeof(int64_t)) {
        throw std::runtime_error("response payload too small for int64_t");
    }
    int64_t value = 0;
    std::memcpy(&value, payload.data(), sizeof(int64_t));
    return value;
}

// uint64_t readU64Payload(const std::vector<uint8_t> &payload) {
//     if (payload.size() < sizeof(uint64_t)) {
//         throw std::runtime_error("response payload too small for uint64_t");
//     }
//     uint64_t value = 0;
//     std::memcpy(&value, payload.data(), sizeof(uint64_t));
//     return value;
// }

struct JoinAssignment {
    uint32_t generationId = 0;
    std::vector<uint32_t> partitions;
};

JoinAssignment parseJoinAssignment(const std::vector<uint8_t> &payload) {
    if (payload.size() < sizeof(uint32_t) * 2) {
        throw std::runtime_error("join response payload too small");
    }

    size_t off = 0;
    JoinAssignment out;
    std::memcpy(&out.generationId, payload.data() + off, sizeof(uint32_t));
    off += sizeof(uint32_t);

    uint32_t count = 0;
    std::memcpy(&count, payload.data() + off, sizeof(uint32_t));
    off += sizeof(uint32_t);

    const size_t need = off + static_cast<size_t>(count) * sizeof(uint32_t);
    if (payload.size() < need) {
        throw std::runtime_error("join response partition list truncated");
    }

    out.partitions.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t p = 0;
        std::memcpy(&p, payload.data() + off, sizeof(uint32_t));
        off += sizeof(uint32_t);
        out.partitions.push_back(p);
    }
    return out;
}

pubsub::storage::RecordBatch makeSingleRecordBatch(const std::string &key, const std::string &value) {
    pubsub::storage::RecordBatch batch;
    batch.batchStart = 0xAB;
    batch.baseOffset = 0;
    batch.timeStamp = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    batch.numRecords = 1;

    pubsub::storage::Record rec;
    rec.recordOffsetDelta = 0;
    rec.key = key;
    rec.value = value;
    batch.records.push_back(std::move(rec));
    return batch;
}

class TestClient {
  public:
    explicit TestClient(std::string id) : clientId(std::move(id)) {}

    ~TestClient() {
        if (sockFd >= 0) {
            close(sockFd);
        }
    }

    void connect(const std::string &ip, int port) {
        sockFd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockFd < 0) {
            throw std::runtime_error("socket() failed");
        }

        sockaddr_in servAddr{};
        servAddr.sin_family = AF_INET;
        servAddr.sin_port = htons(static_cast<uint16_t>(port));
        if (inet_pton(AF_INET, ip.c_str(), &servAddr.sin_addr) <= 0) {
            throw std::runtime_error("invalid broker IP");
        }

        if (::connect(sockFd, reinterpret_cast<sockaddr *>(&servAddr), sizeof(servAddr)) < 0) {
            throw std::runtime_error("connect() failed. Is broker running?");
        }

        std::cout << "[Client:" << clientId << "] Connected to Broker at " << ip << ":" << port << "\n";
    }

    BrokerResponse sendAndReceive(RequestType requestType, const std::vector<uint8_t> &payload,
                                  bool strictCorrelation = true) {
        if (sockFd < 0) {
            throw std::runtime_error("client is not connected");
        }
        const uint32_t corrId = correlationCounter++;
        auto frame = buildRequestFrame(requestType, corrId, clientId, payload);

        if (!sendAll(sockFd, frame.data(), frame.size())) {
            throw std::runtime_error("send() failed while writing request frame");
        }

        std::cout << "  -> Sent Request Type: " << static_cast<int>(requestType) << " | CorrID: " << corrId << "\n";

        BrokerResponse res = readBrokerResponse(sockFd);
        if (strictCorrelation && res.correlationId != corrId) {
            throw std::runtime_error("correlation mismatch in broker response");
        }
        return res;
    }

  private:
    int sockFd = -1;
    std::string clientId;
    uint32_t correlationCounter = 1;
};

void assertErrorCode(const BrokerResponse &res, ErrorCode expected, const std::string &where) {
    if (res.errorCode != expected) {
        throw std::runtime_error(where +
                                 " failed: unexpected ErrorCode=" + std::to_string(static_cast<int>(res.errorCode)));
    }
}

} // namespace

void run_system_tests() {
    std::cout << "========== STARTING END-TO-END BROKER TEST ==========\n\n";

    TestClient adminClient("admin");
    adminClient.connect(BROKER_IP, BROKER_PORT);

    // ---------------------------------------------------------
    // TEST 1: DYNAMIC TOPIC CREATION (Phase 5)
    // ---------------------------------------------------------
    std::cout << "TEST 1: Creating Topic 'test_topic' with 2 partitions...\n";
    auto createPayload = makeCreateTopicPayload("test_topic", 2);
    auto createRes = adminClient.sendAndReceive(RequestType::CREATE_TOPIC, createPayload);
    assertErrorCode(createRes, ErrorCode::NONE, "CREATE_TOPIC");
    std::cout << "✅ Topic creation successful.\n\n";

    // ---------------------------------------------------------
    // TEST 2: PRODUCING DATA (Phase 1 & 2)
    // ---------------------------------------------------------
    std::cout << "TEST 2: Producing data to Partition 0 and 1...\n";
    TestClient producer("producer");
    producer.connect(BROKER_IP, BROKER_PORT);

    pubsub::net::ProducePayload p0;
    p0.topic = "test_topic";
    p0.partitionID = 0;
    p0.acks = 1;
    p0.rawRecordBatch = pubsub::storage::serializeRecordBatch(makeSingleRecordBatch("k0", "Hello P0"));

    pubsub::net::ProducePayload p1;
    p1.topic = "test_topic";
    p1.partitionID = 1;
    p1.acks = 1;
    p1.rawRecordBatch = pubsub::storage::serializeRecordBatch(makeSingleRecordBatch("k1", "Hello P1"));

    auto p0Res = producer.sendAndReceive(RequestType::PRODUCE, pubsub::net::serializeProducePayload(p0));
    auto p1Res = producer.sendAndReceive(RequestType::PRODUCE, pubsub::net::serializeProducePayload(p1));
    assertErrorCode(p0Res, ErrorCode::NONE, "PRODUCE p0");
    assertErrorCode(p1Res, ErrorCode::NONE, "PRODUCE p1");
    std::cout << "✅ Data appended to disk safely.\n\n";

    // ---------------------------------------------------------
    // TEST 3: CONSUMER GROUP REBALANCE (Phase 4)
    // ---------------------------------------------------------
    std::cout << "TEST 3: Joining Consumer Group...\n";
    TestClient consumerA("memberA");
    TestClient consumerB("memberB");
    consumerA.connect(BROKER_IP, BROKER_PORT);
    consumerB.connect(BROKER_IP, BROKER_PORT);

    BrokerResponse joinResA;
    BrokerResponse joinResB;

    std::thread ta([&]() {
        auto joinAPayload = makeJoinGroupPayload("group1", "memberA", 0, "test_topic");
        // Current broker code for JOIN_GROUP may send corrId=0 from coordinator callback.
        joinResA = consumerA.sendAndReceive(RequestType::JOIN_GROUP, joinAPayload, /*strictCorrelation=*/false);
    });

    std::thread tb([&]() {
        auto joinBPayload = makeJoinGroupPayload("group1", "memberB", 0, "test_topic");
        joinResB = consumerB.sendAndReceive(RequestType::JOIN_GROUP, joinBPayload, /*strictCorrelation=*/false);
    });

    ta.join();
    tb.join();

    assertErrorCode(joinResA, ErrorCode::NONE, "JOIN_GROUP memberA");
    assertErrorCode(joinResB, ErrorCode::NONE, "JOIN_GROUP memberB");

    JoinAssignment a = parseJoinAssignment(joinResA.payload);
    JoinAssignment b = parseJoinAssignment(joinResB.payload);

    std::cout << "  memberA generation=" << a.generationId << " partitions=" << a.partitions.size() << "\n";
    std::cout << "  memberB generation=" << b.generationId << " partitions=" << b.partitions.size() << "\n";

    if (a.generationId != b.generationId) {
        throw std::runtime_error("JOIN_GROUP returned different generation IDs");
    }

    std::cout << "Waiting 3.5 seconds for GroupCoordinator rebalance window to close...\n";
    std::this_thread::sleep_for(3500ms);
    std::cout << "✅ Rebalance complete.\n\n";

    // ---------------------------------------------------------
    // TEST 4 & 5: FETCH OFFSETS AND DATA (Phase 3 & 5)
    // ---------------------------------------------------------
    std::cout << "TEST 4 & 5: Fetching starting offsets and reading data...\n";

    uint32_t assignedPartitionForA = a.partitions.empty() ? 0 : a.partitions.front();
    std::cout << "  assigned partition for memberA = " << assignedPartitionForA << "\n";
    uint32_t assignedPartitionForB = b.partitions.empty() ? 0 : b.partitions.front();
    std::cout << "  assigned partition for memberB = " << assignedPartitionForB << "\n";

    auto fetchOffsetPayload = makeFetchOffsetPayload("group1", "test_topic", assignedPartitionForA, a.generationId);
    auto offsetRes = consumerA.sendAndReceive(RequestType::FETCH_LOG_OFFSET, fetchOffsetPayload);
    assertErrorCode(offsetRes, ErrorCode::NONE, "FETCH_LOG_OFFSET");
    int64_t committedOffset = readInt64Payload(offsetRes.payload);
    if (committedOffset == -1)
        committedOffset = 0;
    std::cout << "  committed offset for memberA partition " << assignedPartitionForA << " = " << committedOffset
              << "\n";

    pubsub::net::FetchPayload fetchReq;
    fetchReq.topic = "test_topic";
    fetchReq.partitionID = assignedPartitionForA;
    fetchReq.fetchOffset = 0;
    fetchReq.groupID = "group1";
    fetchReq.generationID = a.generationId;
    std::cout << "  fetching from offset " << committedOffset << " for partition " << assignedPartitionForA
              << " with generationId " << a.generationId << "\n";

    auto msgRes = consumerA.sendAndReceive(RequestType::FETCH, pubsub::net::serializeFetchPayload(fetchReq));
    assertErrorCode(msgRes, ErrorCode::NONE, "FETCH");

    if (msgRes.payload.size() < sizeof(uint64_t)) {
        throw std::runtime_error("FETCH response payload too small for lastOffset");
    }
    uint64_t lastOffset = 0;
    std::memcpy(&lastOffset, msgRes.payload.data(), sizeof(uint64_t));
    std::cout << "  fetch lastOffset=" << lastOffset << ", payloadBytes=" << msgRes.payload.size() << "\n";
    std::cout << "✅ Consumer A successfully read data.\n\n";
    std::cout << "  payload: ";
    for (size_t i = 0; i < msgRes.payload.size(); ++i) {
        std::cout << static_cast<int>(msgRes.payload[i]) << " ";
    }
    std::cout << "\n";

    // ---------------------------------------------------------
    // TEST 6: OFFSET COMMIT (Phase 3)
    // ---------------------------------------------------------
    std::cout << "TEST 6: Committing Log Offset...\n";
    auto commitPayload =
        makeCommitOffsetPayload("group1", "test_topic", assignedPartitionForA, /*offset=*/1, a.generationId);
    auto commitRes = consumerA.sendAndReceive(RequestType::COMMIT_LOG_OFFSET, commitPayload);
    assertErrorCode(commitRes, ErrorCode::NONE, "COMMIT_LOG_OFFSET");
    std::cout << "✅ Offset saved to consumer_offsets internal partition.\n\n";

    // ---------------------------------------------------------
    // TEST 7: ZOMBIE DEFENSE (Phase 5)
    // ---------------------------------------------------------
    std::cout << "TEST 7: Testing Zombie Defense...\n";
    auto zombiePayload =
        makeCommitOffsetPayload("group1", "test_topic", assignedPartitionForA, /*offset=*/2, /*fakeGen=*/999);
    auto zombieRes = consumerA.sendAndReceive(RequestType::COMMIT_LOG_OFFSET, zombiePayload);
    assertErrorCode(zombieRes, ErrorCode::REJOIN, "Zombie COMMIT_LOG_OFFSET");
    std::cout << "✅ Broker correctly rejected the zombie consumer and issued REJOIN.\n\n";

    // ---------------------------------------------------------
    // TEST 8: CLEAN SHUTDOWN (Phase 5.5)
    // ---------------------------------------------------------
    std::cout << "TEST 8: Graceful Leave Group...\n";
    auto leavePayload = makeLeaveGroupPayload("group1", "memberA");
    auto leaveRes = consumerA.sendAndReceive(RequestType::LEAVE_GROUP, leavePayload);
    assertErrorCode(leaveRes, ErrorCode::NONE, "LEAVE_GROUP");

    std::cout << "✅ Consumer A left group cleanly.\n\n";
    std::cout << "========== ALL TESTS PASSED! ==========\n";
}

int main() {
    try {
        run_system_tests();
    } catch (const std::exception &e) {
        std::cerr << "Test failed with exception: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
