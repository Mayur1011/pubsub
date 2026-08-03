#include "concurrency/workerPool.hpp"
#include "net/protocol.hpp"
#include "storage/record.hpp"
#include "storage/topicManager.hpp"
#include <cstdint>
#include <cstring>

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
                uint64_t tmpLastOffset = partition->getNextOffset();
                storage::RecordBatch recordBatch;
                if (partition->read(request.fetchOffset, recordBatch) and recordBatch.numRecords != 0) {
                    request.sendResponse(pubsub::net::serializeResponse(
                        request.correlationID, net::ErrorCode::NONE,
                        net::serializeFetchResponse(tmpLastOffset, storage::serializeRecordBatch(recordBatch))));
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
        } else if (request.type == TaskType::COMMIT_LOG_OFFSET) {
            try {
                std::string offsetKey = net::make_offset_key(request.groupID, request.topicName, request.partitionID);
                uint32_t internalPartitionID = pubsub::net::fnv1aHash(offsetKey) % 50;
                pubsub::storage::Record offsetRecord;
                offsetRecord.key = offsetKey;
                offsetRecord.value.resize(sizeof(uint64_t));
                std::memcpy(offsetRecord.value.data(), &request.committedLogOffset, sizeof(uint64_t));
                pubsub::storage::RecordBatch internalBatch;
                internalBatch.records.push_back(std::move(offsetRecord));
                internalBatch.numRecords = 1;
                pubsub::storage::Partition *internalPart =
                    topicManager->getPartition("consumer_offsets", internalPartitionID);
                if (!internalPart) {
                    request.sendResponse(pubsub::net::serializeResponse(
                        request.correlationID, pubsub::net::ErrorCode::UNKNOWN_SERVER_ERROR, {}));
                    return;
                }
                internalPart->append(internalBatch);
                topicManager->updateCommitLogOffset(offsetKey, request.committedLogOffset);
                request.sendResponse(
                    pubsub::net::serializeResponse(request.correlationID, pubsub::net::ErrorCode::NONE, {}));
            } catch (const std::exception &e) {
                std::cerr << "[concurrency/workerPool]: Failed to commit log offset: " << e.what() << "\n";
                request.sendResponse(pubsub::net::serializeResponse(
                    request.correlationID, net::ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>()));
            }
        } else if (request.type == TaskType::FETCH_LOG_OFFSET) {
            try {
                std::string offsetKey = net::make_offset_key(request.groupID, request.topicName, request.partitionID);
                uint64_t commitedLogOffset = topicManager->getCommitLogOffset(offsetKey);
                std::vector<uint8_t> response_payload(sizeof(int64_t));
                std::memcpy(response_payload.data(), &commitedLogOffset, sizeof(int64_t));
                request.sendResponse(pubsub::net::serializeResponse(request.correlationID, pubsub::net::ErrorCode::NONE,
                                                                    response_payload));

            } catch (const std::exception &e) {
                std::cerr << "[concurrency/workerPool]: Failed to fetch log offset: " << e.what() << "\n";
                request.sendResponse(pubsub::net::serializeResponse(
                    request.correlationID, net::ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>()));
            }
        } else if (request.type == TaskType::CONSUMER_HEARTBEAT) {
            groupCoord->registerHeartBeat(request.groupID, request.memberID);
            request.sendResponse(pubsub::net::serializeResponse(request.correlationID, pubsub::net::ErrorCode::NONE,
                                                                std::vector<uint8_t>()));
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