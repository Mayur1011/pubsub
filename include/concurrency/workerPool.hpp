#pragma once

#include <cstdint>
#include <functional>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unordered_map>

#include "concurrency/diskQueue.hpp"
#include "concurrency/diskRequest.hpp"
#include "net/protocol.hpp"
#include "storage/partition.hpp"
#include "storage/record.hpp"

namespace pubsub::concurrency {
// a worker thread will managed fixed number of partitions. so no two worker threads will manage the same partition.
class WorkerThread {
    std::jthread thread;
    DiskQueue<DiskRequest> workerQueue; // this is where the epoll thread pushes requests to be processed by this worker
    std::unordered_map<std::string, pubsub::storage::Partition *>
        assignedPartitions; // one thread handling one/more partitions
    void dispatchRequest(const DiskRequest &request);

  public:
    WorkerThread() = default;
    void assignPartition(pubsub::storage::Partition *partition, const std::string &routingKey);
    void start();
    void submitRequest(DiskRequest diskRequest);
    void shutDown();
};

class WorkerPool {
  private:
    std::vector<std::unique_ptr<WorkerThread>> workers;

  public:
    explicit WorkerPool(size_t numWorkers) {
        for (size_t i = 0; i < numWorkers; ++i) {
            auto worker = std::make_unique<WorkerThread>();
            workers.push_back(std::move(worker));
        }
    }
    void start();
    void registerPartition(const std::string &topicName, uint32_t partitionID, void *partitionPtr);
    void routeRequest(DiskRequest diskRequest);
    void shutDown();
};
} // namespace pubsub::concurrency