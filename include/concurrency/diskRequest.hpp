#pragma once

#include "storage/record.hpp"
#include <cstdint>
#include <functional>
#include <vector>

namespace pubsub::concurrency {
// this struct represent how to request related to disk for the worker thread will look like (this is the struct that
// will be pushed into diskqueue by epoll n/w theread)
enum class TaskType {
    PRODUCE = 0x01,
    FETCH = 0x02,
    COMMIT_LOG_OFFSET = 0x03,
    FETCH_LOG_OFFSET = 0x04,
    CONSUMER_HEARTBEAT = 0x05,
    JOIN_GROUP = 0x06,
    LEAVE_GROUP = 0x07,
    CREATE_TOPIC = 0x08
};
struct DiskRequest {
    TaskType type;
    int clientFD;
    std::string topicName;
    uint32_t partitionID;
    uint32_t correlationID;

    std::string groupID;
    pubsub::storage::RecordBatch produceBatch;
    uint64_t fetchOffset;
    uint64_t committedLogOffset;

    std::string memberID;
    uint32_t generationID;

    uint32_t numPartitions;

    std::function<void(std::vector<uint8_t>)> sendResponse;
    // routing key only used by produce and fetch reqs.
    std::string getRoutingKey() const { return topicName + "-" + std::to_string(partitionID); }
};
} // namespace pubsub::concurrency