#include "concurrency/workerPool.hpp"

namespace pubsub::concurrency {
void WorkerThread::assignPartition(pubsub::storage::Partition *partition, const std::string &routingKey) {
    assignedPartitions[routingKey] = partition;
}
void WorkerThread::dispatchRequest(const DiskRequest &request) {
    {
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
}
void WorkerThread::start() {
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
void WorkerThread::submitRequest(DiskRequest diskRequest) { workerQueue.push(std::move(diskRequest)); }
void WorkerThread::shutDown() { workerQueue.closeQueue(); }

/*-------------------------------------------------------------------------------- */
void WorkerPool::start() {
    for (auto &worker : workers) {
        worker->start();
    }
}
void WorkerPool::registerPartition(const std::string &topicName, uint32_t partitionID, void *partitionPtr) {
    uint32_t workerID = (std::hash<std::string>{}(topicName) + partitionID) % workers.size();
    std::string routingKey = topicName + "-" + std::to_string(partitionID);
    workers[workerID]->assignPartition((storage::Partition *)partitionPtr, routingKey);
    std::cout << "[concurreny/WorkerPool] Assigned Partition " << partitionID << " to Worker Thread " << workerID
              << " (routing key: " << routingKey << ")\n";
}
// epoll thread will call this to drop requests onto the correct worker
void WorkerPool::routeRequest(DiskRequest diskRequest) {
    uint32_t workerID = (std::hash<std::string>{}(diskRequest.topicName) + diskRequest.partitionID) % workers.size();
    std::cerr << "[concurreny/WorkerPool] routing request to worker " << workerID
              << " (routing key: " << diskRequest.getRoutingKey() << ")\n";
    workers[workerID]->submitRequest(std::move(diskRequest));
}
void WorkerPool::shutDown() {
    for (auto &worker : workers) {
        worker->shutDown();
    }
}
} // namespace pubsub::concurrency