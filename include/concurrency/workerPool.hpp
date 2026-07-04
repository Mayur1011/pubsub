#pragma once

#include "concurrency/diskQueue.hpp"
#include "concurrency/diskRequest.hpp"
#include "net/protocol.hpp"
#include "storage/partition.hpp"
#include "storage/record.hpp"
#include <cstdint>
#include <iostream>
#include <sys/socket.h>
#include <thread>
#include <unordered_map>
namespace pubsub::concurrency {

// a worker thread will managed fixed number of partitions. so no two worker threads will manage the same partition.
class WorkerThread {
    std::jthread thread;
    DiskQueue<DiskRequest> workerQueue; // this is where the epoll thread pushes requests to be processed by this worker
    std::unordered_map<uint32_t, pubsub::storage::Partition *>
        assignedPartitions; // one thread handling one/more partitions
    void dispatchRequest(const DiskRequest &request) {
        auto it = assignedPartitions.find(request.partitionID);
        if (it == assignedPartitions.end()) {
            std::cerr << "[concurrency/workerPool]: Partition " << request.partitionID << " not found\n";
            request.sendResponse(pubsub::net::serializeResponse(
                request.correlationID, net::ErrorCode::PARTITION_NOT_FOUND, std::vector<uint8_t>()));
            return;
        }
        auto *partition = it->second;
        if (request.type == TaskType::PRODUCE) {
            try {
                partition->append(request.produceBatch);
                request.sendResponse(pubsub::net::serializeResponse(request.correlationID, net::ErrorCode::NONE,
                                                                    std::vector<uint8_t>()));
            } catch (const std::exception &e) {
                std::cerr << "[concurrency/workerPool]: Failed to append to partition " << request.partitionID << ": "
                          << e.what() << "\n";
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
                std::cerr << "[concurrency/workerPool]: Failed to fetch from partition " << request.partitionID << ": "
                          << e.what() << "\n";
                request.sendResponse(pubsub::net::serializeResponse(
                    request.correlationID, net::ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>()));
            }
        }
    }

  public:
    WorkerThread() = default;
    void assignPartition(uint32_t partitionID, pubsub::storage::Partition *partition) {
        assignedPartitions[partitionID] = partition;
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
    explicit WorkerPool(size_t num_workers) {
        for (size_t i = 0; i < num_workers; ++i) {
            auto worker = std::make_unique<WorkerThread>();
            workers.push_back(std::move(worker));
        }
    }

    void start() {
        for (auto &worker : workers) {
            worker->start();
        }
    }

    void registerPartition(uint32_t partition_id, void *partition_ptr) {
        uint32_t worker_id = partition_id % workers.size();
        workers[worker_id]->assignPartition(partition_id, static_cast<pubsub::storage::Partition *>(partition_ptr));
        std::cout << "[concurreny/WorkerPool] Assigned Partition " << partition_id << " to Worker Thread " << worker_id
                  << "\n";
    }

    // epoll thread will call this to drop requests onto the correct worker
    void routeRequest(DiskRequest req) {
        uint32_t worker_id = req.partitionID % workers.size();
        workers[worker_id]->submitRequest(std::move(req));
    }

    void shutDown() {
        for (auto &worker : workers) {
            worker->shutDown();
        }
    }
};
} // namespace pubsub::concurrency