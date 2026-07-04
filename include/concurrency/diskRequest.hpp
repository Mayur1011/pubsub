#pragma once

#include "storage/record.hpp"
#include <cstdint>
#include <functional>
#include <vector>

namespace pubsub::concurrency {
// this struct represent how to request related to disk for the worker thread will look like (this is the struct that
// will be pushed into diskqueue by epoll n/w theread)
enum class TaskType { PRODUCE, FETCH };
struct DiskRequest {
    TaskType type;
    int clientFD;
    uint32_t partitionID;
    uint32_t correlationID;

    pubsub::storage::RecordBatch produceBatch;
    uint64_t fetchOffset;

    std::function<void(std::vector<uint8_t>)> sendResponse;
};
} // namespace pubsub::concurrency