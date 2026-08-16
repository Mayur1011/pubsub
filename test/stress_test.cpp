#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "net/protocol.hpp"
#include "storage/record.hpp"

namespace {

constexpr const char *kBrokerIp = "127.0.0.1";
constexpr int kBrokerPort = 6969;
constexpr int kDefaultThreads = 10;
constexpr uint64_t kDefaultMessages = 10000;
constexpr uint32_t kDefaultPartitions = 2;

struct BrokerResponse {
    uint8_t errorCode = 0;
    uint32_t correlationId = 0;
    std::vector<uint8_t> payload;
};

struct WorkerStats {
    uint64_t attempted = 0;
    uint64_t succeeded = 0;
    uint64_t failed = 0;
    uint64_t connectionFailures = 0;
    uint64_t ioFailures = 0;
    std::vector<uint64_t> latenciesUs;
    std::unordered_map<uint8_t, uint64_t> errorHistogram;
};

template <typename T> void appendBytes(std::vector<uint8_t> &buf, const T &value) {
    static_assert(std::is_trivially_copyable_v<T>, "appendBytes requires trivially copyable type");
    const auto *ptr = reinterpret_cast<const uint8_t *>(&value);
    buf.insert(buf.end(), ptr, ptr + sizeof(T));
}

void appendStringU16(std::vector<uint8_t> &buf, const std::string &s) {
    if (s.size() > static_cast<size_t>(UINT16_MAX)) {
        throw std::runtime_error("string too long for u16 length prefix");
    }
    const uint16_t len = static_cast<uint16_t>(s.size());
    appendBytes<uint16_t>(buf, len);
    buf.insert(buf.end(), s.begin(), s.end());
}

std::vector<uint8_t> makeCreateTopicPayload(const std::string &topic, uint32_t partitions) {
    std::vector<uint8_t> payload;
    appendStringU16(payload, topic);
    appendBytes<uint32_t>(payload, partitions);
    return payload;
}

std::vector<uint8_t> buildRequestFrame(pubsub::net::RequestType requestType, uint32_t correlationId,
                                       const std::string &clientId, const std::vector<uint8_t> &payload) {
    std::vector<uint8_t> frame;
    frame.reserve(sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint16_t) + clientId.size() +
                  payload.size());

    appendBytes<uint32_t>(frame, 0U); // frameLen placeholder
    appendBytes<uint8_t>(frame, static_cast<uint8_t>(requestType));
    appendBytes<uint32_t>(frame, correlationId);
    appendStringU16(frame, clientId);
    frame.insert(frame.end(), payload.begin(), payload.end());

    const uint32_t frameLen = static_cast<uint32_t>(frame.size() - sizeof(uint32_t));
    std::memcpy(frame.data(), &frameLen, sizeof(frameLen));
    return frame;
}

bool sendAll(int sock, const uint8_t *data, size_t len) {
    size_t totalSent = 0;
    while (totalSent < len) {
        const ssize_t n = ::send(sock, data + totalSent, len - totalSent, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        totalSent += static_cast<size_t>(n);
    }
    return true;
}

bool recvExact(int sock, uint8_t *dst, size_t len) {
    size_t totalRead = 0;
    while (totalRead < len) {
        const ssize_t n = ::recv(sock, dst + totalRead, len - totalRead, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        totalRead += static_cast<size_t>(n);
    }
    return true;
}

bool readBrokerResponse(int sock, BrokerResponse &out) {
    uint32_t frameLen = 0;
    if (!recvExact(sock, reinterpret_cast<uint8_t *>(&frameLen), sizeof(frameLen))) {
        return false;
    }
    if (frameLen < (sizeof(uint8_t) + sizeof(uint32_t))) {
        return false;
    }

    std::vector<uint8_t> body(frameLen);
    if (!recvExact(sock, body.data(), body.size())) {
        return false;
    }

    // serializeResponse format in your codebase: [errorCode:u8][correlationId:u32][payload...]
    out.errorCode = body[0];
    std::memcpy(&out.correlationId, body.data() + 1, sizeof(uint32_t));
    out.payload.assign(body.begin() + 1 + sizeof(uint32_t), body.end());
    return true;
}

int connectToBroker() {
    const int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        return -1;
    }

    int one = 1;
    (void)setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(kBrokerPort));
    if (inet_pton(AF_INET, kBrokerIp, &addr.sin_addr) <= 0) {
        ::close(sock);
        return -1;
    }

    if (::connect(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        ::close(sock);
        return -1;
    }
    return sock;
}

bool createTopicOnce(const std::string &topic, uint32_t partitions) {
    const int sock = connectToBroker();
    if (sock < 0) {
        std::cerr << "CREATE_TOPIC: connection failed\n";
        return false;
    }

    const auto payload = makeCreateTopicPayload(topic, partitions);
    const auto frame = buildRequestFrame(pubsub::net::RequestType::CREATE_TOPIC, 1, "stress_admin", payload);

    if (!sendAll(sock, frame.data(), frame.size())) {
        std::cerr << "CREATE_TOPIC: send failed\n";
        ::close(sock);
        return false;
    }

    BrokerResponse res;
    if (!readBrokerResponse(sock, res)) {
        std::cerr << "CREATE_TOPIC: recv failed\n";
        ::close(sock);
        return false;
    }

    ::close(sock);

    if (res.correlationId != 1) {
        std::cerr << "CREATE_TOPIC: correlation mismatch (got " << res.correlationId << ", expected 1)\n";
        return false;
    }

    if (res.errorCode != static_cast<uint8_t>(pubsub::net::ErrorCode::NONE)) {
        std::cerr << "CREATE_TOPIC: broker returned errorCode=" << static_cast<int>(res.errorCode) << "\n";
        return false;
    }

    return true;
}

pubsub::storage::RecordBatch makeSingleRecordBatch(const std::string &key, const std::string &value) {
    pubsub::storage::RecordBatch batch{};
    batch.batchStart = 0xAB;
    batch.baseOffset = 0;
    batch.batchLen = 0;
    batch.timeStamp = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
    batch.numRecords = 1;

    pubsub::storage::Record rec{};
    rec.recordOffsetDelta = 0;
    rec.key = key;
    rec.value = value;
    batch.records.push_back(std::move(rec));
    return batch;
}

WorkerStats runWorker(size_t workerId, uint64_t messageCount, uint32_t partitions, const std::string &topic) {
    WorkerStats stats{};

    const int sock = connectToBroker();
    if (sock < 0) {
        stats.connectionFailures += 1;
        stats.attempted += messageCount;
        stats.failed += messageCount;
        return stats;
    }

    const uint32_t partitionId = static_cast<uint32_t>(workerId % partitions);

    pubsub::net::ProducePayload producePayload{};
    producePayload.topic = topic;
    producePayload.partitionID = partitionId;
    producePayload.acks = 1;
    producePayload.rawRecordBatch =
        pubsub::storage::serializeRecordBatch(makeSingleRecordBatch("k" + std::to_string(workerId), "stress_value"));

    // Pre-serialized once per worker
    const std::vector<uint8_t> serializedProducePayload = pubsub::net::serializeProducePayload(producePayload);

    for (uint64_t i = 0; i < messageCount; ++i) {
        stats.attempted += 1;
        const uint32_t correlationId = static_cast<uint32_t>((workerId << 20U) ^ (i + 1U));
        const auto frame = buildRequestFrame(pubsub::net::RequestType::PRODUCE, correlationId,
                                             "producer_" + std::to_string(workerId), serializedProducePayload);

        const auto t0 = std::chrono::steady_clock::now();

        if (!sendAll(sock, frame.data(), frame.size())) {
            stats.ioFailures += 1;
            stats.failed += 1;
            const uint64_t remaining = messageCount - i - 1;
            stats.attempted += remaining;
            stats.failed += remaining;
            break;
        }

        BrokerResponse res;
        if (!readBrokerResponse(sock, res)) {
            stats.ioFailures += 1;
            stats.failed += 1;
            const uint64_t remaining = messageCount - i - 1;
            stats.attempted += remaining;
            stats.failed += remaining;
            break;
        }

        const auto t1 = std::chrono::steady_clock::now();
        stats.latenciesUs.push_back(
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count()));

        if (res.correlationId != correlationId) {
            stats.ioFailures += 1;
            stats.failed += 1;
            continue;
        }

        stats.errorHistogram[res.errorCode] += 1;
        if (res.errorCode == static_cast<uint8_t>(pubsub::net::ErrorCode::NONE)) {
            stats.succeeded += 1;
        } else {
            stats.failed += 1;
        }
    }

    ::close(sock);
    return stats;
}

uint64_t percentileUs(std::vector<uint64_t> values, double p) {
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    const size_t idx = static_cast<size_t>(p * static_cast<double>(values.size() - 1));
    return values[idx];
}

bool parseU64Arg(const char *arg, uint64_t &out) {
    if (arg == nullptr || *arg == '\0') {
        return false;
    }
    char *end = nullptr;
    errno = 0;
    const unsigned long long v = std::strtoull(arg, &end, 10);
    if (errno != 0 || end == arg || *end != '\0') {
        return false;
    }
    out = static_cast<uint64_t>(v);
    return true;
}

bool parseU32Arg(const char *arg, uint32_t &out) {
    uint64_t temp = 0;
    if (!parseU64Arg(arg, temp)) {
        return false;
    }
    if (temp > static_cast<uint64_t>(UINT32_MAX)) {
        return false;
    }
    out = static_cast<uint32_t>(temp);
    return true;
}

void printUsage(const char *prog) {
    std::cout << "Usage: " << prog << " [threads] [totalMessages] [partitions] [topic]\n"
              << "Defaults: threads=" << kDefaultThreads << " totalMessages=" << kDefaultMessages
              << " partitions=" << kDefaultPartitions << " topic=stress_topic\n"
              << "Example: " << prog << " 20 200000 8 orders\n";
}

} // namespace

int main(int argc, char **argv) {
    uint32_t threads = kDefaultThreads;
    uint64_t totalMessages = kDefaultMessages;
    uint32_t partitions = kDefaultPartitions;
    std::string topic = "stress_topic";

    if (argc > 1 && (std::string(argv[1]) == "-h" || std::string(argv[1]) == "--help")) {
        printUsage(argv[0]);
        return 0;
    }

    if (argc > 1) {
        uint32_t v = 0;
        if (!parseU32Arg(argv[1], v) || v == 0) {
            std::cerr << "Invalid threads argument\n";
            printUsage(argv[0]);
            return 1;
        }
        threads = v;
    }
    if (argc > 2) {
        uint64_t v = 0;
        if (!parseU64Arg(argv[2], v) || v == 0) {
            std::cerr << "Invalid totalMessages argument\n";
            printUsage(argv[0]);
            return 1;
        }
        totalMessages = v;
    }
    if (argc > 3) {
        uint32_t v = 0;
        if (!parseU32Arg(argv[3], v) || v == 0) {
            std::cerr << "Invalid partitions argument\n";
            printUsage(argv[0]);
            return 1;
        }
        partitions = v;
    }
    if (argc > 4) {
        topic = argv[4];
        if (topic.empty()) {
            std::cerr << "Invalid topic argument\n";
            return 1;
        }
    }

    std::cout << "[producer_stress] broker=" << kBrokerIp << ":" << kBrokerPort << " threads=" << threads
              << " totalMessages=" << totalMessages << " partitions=" << partitions << " topic=" << topic << "\n";

    if (!createTopicOnce(topic, partitions)) {
        std::cerr << "Failed to create topic '" << topic << "' with partitions=" << partitions << "\n";
        return 1;
    }

    std::vector<std::thread> workerThreads;
    workerThreads.reserve(threads);
    std::vector<WorkerStats> results(threads);

    const uint64_t base = totalMessages / threads;
    const uint64_t rem = totalMessages % threads;

    const auto start = std::chrono::steady_clock::now();

    for (uint32_t i = 0; i < threads; ++i) {
        const uint64_t count = base + (i < rem ? 1ULL : 0ULL);
        workerThreads.emplace_back([&, i, count]() { results[i] = runWorker(i, count, partitions, topic); });
    }

    for (auto &t : workerThreads) {
        t.join();
    }

    const auto end = std::chrono::steady_clock::now();
    const double elapsedSec = std::chrono::duration<double>(end - start).count();

    WorkerStats agg{};
    for (const auto &s : results) {
        agg.attempted += s.attempted;
        agg.succeeded += s.succeeded;
        agg.failed += s.failed;
        agg.connectionFailures += s.connectionFailures;
        agg.ioFailures += s.ioFailures;
        agg.latenciesUs.insert(agg.latenciesUs.end(), s.latenciesUs.begin(), s.latenciesUs.end());
        for (const auto &[code, count] : s.errorHistogram) {
            agg.errorHistogram[code] += count;
        }
    }

    const double throughput = elapsedSec > 0.0 ? static_cast<double>(agg.succeeded) / elapsedSec : 0.0;
    const uint64_t p50 = percentileUs(agg.latenciesUs, 0.50);
    const uint64_t p95 = percentileUs(agg.latenciesUs, 0.95);
    const uint64_t p99 = percentileUs(agg.latenciesUs, 0.99);

    std::vector<std::pair<uint8_t, uint64_t>> hist(agg.errorHistogram.begin(), agg.errorHistogram.end());
    std::sort(hist.begin(), hist.end(), [](const auto &a, const auto &b) { return a.first < b.first; });

    std::cout << "\n========== PRODUCER STRESS RESULTS ==========\n";
    std::cout << "attempted            : " << agg.attempted << "\n";
    std::cout << "succeeded            : " << agg.succeeded << "\n";
    std::cout << "failed               : " << agg.failed << "\n";
    std::cout << "connection failures  : " << agg.connectionFailures << "\n";
    std::cout << "io failures          : " << agg.ioFailures << "\n";
    std::cout << "duration sec         : " << elapsedSec << "\n";
    std::cout << "throughput msg/sec   : " << throughput << "\n";
    std::cout << "latency p50 (us)     : " << p50 << "\n";
    std::cout << "latency p95 (us)     : " << p95 << "\n";
    std::cout << "latency p99 (us)     : " << p99 << "\n";
    std::cout << "error-code histogram:\n";
    bool any = false;
    for (const auto &[code, count] : hist) {
        if (count == 0) {
            continue;
        }
        any = true;
        std::cout << "  code=" << static_cast<int>(code) << " count=" << count << "\n";
    }
    if (!any) {
        std::cout << "  (none)\n";
    }
    std::cout << "============================================\n";

    return agg.failed == 0 ? 0 : 2;
}
