#pragma once

#include <cstdint>
#include <functional>
#include <iostream>
#include <shared_mutex>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unordered_map>

#include "concurrency/diskQueue.hpp"
#include "concurrency/diskRequest.hpp"
#include "concurrency/groupCoord.hpp"
#include "net/protocol.hpp"
#include "storage/partition.hpp"
#include "storage/record.hpp"
#include "storage/topicManager.hpp"

namespace pubsub::concurrency {
class WorkerPool;

// a worker thread will managed fixed number of partitions. so no two worker threads will manage the same partition.
class WorkerThread {
    std::jthread thread;
    DiskQueue<DiskRequest> workerQueue; // this is where the epoll thread pushes requests to be processed by this worker
    std::unordered_map<std::string, pubsub::storage::Partition *>
        assignedPartitions; // one thread handling one/more partitions
    std::shared_mutex assignedPartitionsMutex;
    pubsub::storage::TopicManager *topicManager{nullptr};
    void dispatchRequest(const DiskRequest &request);
    pubsub::concurrency::GroupCoord *groupCoord{nullptr};
    WorkerPool *workerPool{nullptr}; // needed this to create new topic req

  public:
    WorkerThread() = default;
    explicit WorkerThread(pubsub::storage::TopicManager *tm, pubsub::concurrency::GroupCoord *gc, WorkerPool *wp)
        : topicManager(tm), groupCoord(gc), workerPool(wp) {}
    void assignPartition(pubsub::storage::Partition *partition, const std::string &routingKey);
    void start();
    void submitRequest(DiskRequest diskRequest);
    void shutDown();
};

class WorkerPool {
  private:
    std::vector<std::unique_ptr<WorkerThread>> workers;
    pubsub::storage::TopicManager *topicManager{nullptr};
    pubsub::concurrency::GroupCoord *groupCoord{nullptr};

  public:
    explicit WorkerPool(size_t numWorkers, pubsub::storage::TopicManager *tm, pubsub::concurrency::GroupCoord *gc)
        : topicManager(tm), groupCoord(gc) {
        for (size_t i = 0; i < numWorkers; ++i) {
            auto worker = std::make_unique<WorkerThread>(topicManager, gc, this);
            workers.push_back(std::move(worker));
        }
    }
    void start();
    void registerPartition(const std::string &topicName, uint32_t partitionID, void *partitionPtr);
    void routeRequest(DiskRequest diskRequest);
    void shutDown();
};
} // namespace pubsub::concurrency