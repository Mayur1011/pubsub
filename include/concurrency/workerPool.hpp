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

    void dispatchRequest(const DiskRequest &request) {
        // std::cerr << "[Worker] Dispatching request " << static_cast<int>(request.type) << " " <<
        // request.getRoutingKey()
        // << " corrId=" << request.correlationID << "\n";
        auto it = assignedPartitions.find(request.getRoutingKey());
        if (it == assignedPartitions.end()) {
            request.sendResponse(pubsub::net::serializeResponse(
                request.correlationID, net::ErrorCode::PARTITION_NOT_FOUND, std::vector<uint8_t>()));
            return;
        }
        auto *partition = it->second;
        if (request.type == TaskType::PRODUCE) {
            try {
                partition->append(request.produceBatch);
                std::vector<uint8_t> serializedResponse =
                    pubsub::net::serializeResponse(request.correlationID, net::ErrorCode::NONE, std::vector<uint8_t>());
                request.sendResponse(serializedResponse);
            } catch (const std::exception &e) {
                std::cerr << "[concurrency/workerPool]: Failed to append to partition " << request.getRoutingKey()
                          << ": " << e.what() << "\n";
                request.sendResponse(pubsub::net::serializeResponse(
                    request.correlationID, net::ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>()));
            }
        } else if (request.type == TaskType::FETCH) {
            try {
                storage::Record record;
                if (partition->read(request.fetchOffset, record)) {
                    request.sendResponse(pubsub::net::serializeResponse(request.correlationID, net::ErrorCode::NONE,
                                                                        storage::serializeRecord(record)));
                } else {
                    request.sendResponse(pubsub::net::serializeResponse(
                        request.correlationID, net::ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>()));
                }
            } catch (const std::exception &e) {
                std::cerr << "[concurrency/workerPool]: Failed to fetch from partition " << request.getRoutingKey()
                          << ": " << e.what() << "\n";
                request.sendResponse(pubsub::net::serializeResponse(
                    request.correlationID, net::ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>()));
            }
        }
    }

  public:
    WorkerThread() = default;
    void assignPartition(pubsub::storage::Partition *partition, const std::string &routingKey) {
        assignedPartitions[routingKey] = partition;
    }
    void start() {
        thread = std::jthread([this](std::stop_token st) {
            while (!st.stop_requested()) {
                DiskRequest diskRequest;
                if (!workerQueue.pop(diskRequest)) {
                    break;
                }
                dispatchRequest(diskRequest);
            }
        });
    }
    void submitRequest(DiskRequest diskRequest) { workerQueue.push(std::move(diskRequest)); }
    void shutDown() { workerQueue.closeQueue(); }
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

    void start() {
        for (auto &worker : workers) {
            worker->start();
        }
    }

    void registerPartition(const std::string &topicName, uint32_t partitionID, void *partitionPtr) {
        uint32_t workerID = (std::hash<std::string>{}(topicName) + partitionID) % workers.size();
        std::string routingKey = topicName + "-" + std::to_string(partitionID);
        workers[workerID]->assignPartition((storage::Partition *)partitionPtr, routingKey);
        std::cout << "[concurreny/WorkerPool] Assigned Partition " << partitionID << " to Worker Thread " << workerID
                  << " (routing key: " << routingKey << ")\n";
    }

    // epoll thread will call this to drop requests onto the correct worker
    void routeRequest(DiskRequest diskRequest) {
        uint32_t workerID =
            (std::hash<std::string>{}(diskRequest.topicName) + diskRequest.partitionID) % workers.size();
        std::cerr << "[concurreny/WorkerPool] routing request to worker " << workerID
                  << " (routing key: " << diskRequest.getRoutingKey() << ")\n";
        workers[workerID]->submitRequest(std::move(diskRequest));
    }

    void shutDown() {
        for (auto &worker : workers) {
            worker->shutDown();
        }
    }
};
} // namespace pubsub::concurrency