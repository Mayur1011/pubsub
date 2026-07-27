#pragma once

#include "storage/record.hpp"
#include <cstdint>
#include <functional>
#include <vector>

namespace pubsub::concurrency {
// this struct represent how to request related to disk for the worker thread will look like (this is the struct that
// will be pushed into diskqueue by epoll n/w theread)
enum class TaskType { PRODUCE, FETCH, COMMIT_LOG_OFFSET, FETCH_LOG_OFFSET };
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

    std::function<void(std::vector<uint8_t>)> sendResponse;
    std::string getRoutingKey() const { return topicName + "-" + std::to_string(partitionID); }
};
} // namespace pubsub::concurrency