#include "concurrency/diskRequest.hpp"
#include "concurrency/workerPool.hpp"
#include "net/protocol.hpp"
#include "storage/record.hpp"
#include "storage/topicManager.hpp"
#include <cstdint>
#include <cstring>
#include <mutex>
#include <shared_mutex>

namespace pubsub::concurrency {
void WorkerThread::assignPartition(pubsub::storage::Partition *partition, const std::string &routingKey) {
    std::unique_lock<std::shared_mutex> lock(assignedPartitionsMutex);
    assignedPartitions[routingKey] = partition;
}
void WorkerThread::dispatchRequest(const DiskRequest &request) {
    {
        std::cerr << "[Worker] Dispatching request " << static_cast<int>(request.type) << " " << request.getRoutingKey()
                  << " corrId=" << request.correlationID << "\n";

        if (request.type == TaskType::CREATE_TOPIC) {
            std::cout << "[concurrency/workerPool] Creating new topic: " << request.topicName << " with "
                      << request.numPartitions << " partitions.\n";
            topicManager->createTopic(request.topicName, request.numPartitions);

            // registering the partitions to the worker threds
            for (uint32_t i = 0; i < request.numPartitions; ++i) {
                pubsub::storage::Partition *newPart = topicManager->getPartition(request.topicName, i);
                if (newPart) {
                    workerPool->registerPartition(request.topicName, i, newPart);
                }
            }
            request.sendResponse(
                pubsub::net::serializeResponse(request.correlationID, pubsub::net::ErrorCode::NONE, {}));
        } else if (request.type == TaskType::JOIN_GROUP) {
            std::cout << "[concurrency/workerPool] Joining group: " << request.groupID << "\n";
            groupCoord->joinGroup(request.groupID, request.memberID, request.topicName, request.sendResponse);
        } else if (request.type == TaskType::LEAVE_GROUP) {
            std::cout << "[concurrency/workerPool] Leaving group: " << request.groupID << "\n";
            try {
                groupCoord->leaveGroup(request.groupID, request.memberID);
                request.sendResponse(
                    pubsub::net::serializeResponse(request.correlationID, pubsub::net::ErrorCode::NONE, {}));
            } catch (const std::exception &e) {
                std::cerr << "[concurrency/workerPool]: Failed to process LEAVE_GROUP: " << e.what() << "\n";
                request.sendResponse(pubsub::net::serializeResponse(
                    request.correlationID, net::ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>()));
            }
        } else if (request.type == TaskType::CONSUMER_HEARTBEAT) {
            // checking if the consumer that has send the heartbeat was already kicked or not.
            if (not groupCoord->validateGeneration(request.groupID, request.generationID)) {
                // i need to tell the consumer to rejoin and now it can get some difference partition
                request.sendResponse(net::serializeResponse(request.correlationID, pubsub::net::ErrorCode::REJOIN,
                                                            std::vector<uint8_t>()));
                return;
            }
            groupCoord->registerHeartBeat(request.groupID, request.memberID);
            request.sendResponse(pubsub::net::serializeResponse(request.correlationID, pubsub::net::ErrorCode::NONE,
                                                                std::vector<uint8_t>()));
        } else if (request.type == TaskType::COMMIT_LOG_OFFSET) {
            if (not groupCoord->validateGeneration(request.groupID, request.generationID)) {
                request.sendResponse(pubsub::net::serializeResponse(request.correlationID, net::ErrorCode::REJOIN,
                                                                    std::vector<uint8_t>()));
                return;
            }
            try {
                // groupId:topicName:partitionID
                std::string offsetKey = net::makeOffsetKey(request.groupID, request.topicName, request.partitionID);
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
                std::cout << "[concurrency/workerPool]: Committing log offset " << request.committedLogOffset << " for "
                          << offsetKey << "\n";
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
            if (not groupCoord->validateGeneration(request.groupID, request.generationID)) {
                request.sendResponse(pubsub::net::serializeResponse(request.correlationID, net::ErrorCode::REJOIN,
                                                                    std::vector<uint8_t>()));
                return;
            }
            try {
                std::string offsetKey = net::makeOffsetKey(request.groupID, request.topicName, request.partitionID);
                int64_t commitedLogOffset = topicManager->getCommitLogOffset(offsetKey);
                std::vector<uint8_t> response_payload(sizeof(int64_t));
                std::memcpy(response_payload.data(), &commitedLogOffset, sizeof(int64_t));
                std::cout << "[concurrency/workerPool]: Fetched log offset " << commitedLogOffset << " for "
                          << offsetKey << "\n";
                request.sendResponse(pubsub::net::serializeResponse(request.correlationID, pubsub::net::ErrorCode::NONE,
                                                                    response_payload));

            } catch (const std::exception &e) {
                std::cerr << "[concurrency/workerPool]: Failed to fetch log offset: " << e.what() << "\n";
                request.sendResponse(pubsub::net::serializeResponse(
                    request.correlationID, net::ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>()));
            }
        } else if (request.type == TaskType::PRODUCE || request.type == TaskType::FETCH) {
            pubsub::storage::Partition *partition = nullptr;
            {
                std::shared_lock<std::shared_mutex> lock(assignedPartitionsMutex);
                auto it = assignedPartitions.find(request.getRoutingKey());
                if (it == assignedPartitions.end()) {
                    request.sendResponse(
                        pubsub::net::serializeResponse(request.correlationID, net::ErrorCode::PARTITION_NOT_FOUND, {}));
                    return;
                }
                partition = it->second;
            }
            if (request.type == TaskType::PRODUCE) {
                try {
                    partition->append(request.produceBatch);
                    std::vector<uint8_t> serializedResponse = pubsub::net::serializeResponse(
                        request.correlationID, net::ErrorCode::NONE, std::vector<uint8_t>());
                    request.sendResponse(serializedResponse);
                } catch (const std::exception &e) {
                    std::cerr << "[concurrency/workerPool]: Failed to append to partition " << request.getRoutingKey()
                              << ": " << e.what() << "\n";
                    request.sendResponse(pubsub::net::serializeResponse(
                        request.correlationID, net::ErrorCode::UNKNOWN_SERVER_ERROR, std::vector<uint8_t>()));
                }
            } else if (request.type == TaskType::FETCH) {
                std::cout << "[concurrency/workerPool]: Fetching log offset " << request.fetchOffset << " for "
                          << request.getRoutingKey() << " generation " << request.generationID << "\n";
                if (not groupCoord->validateGeneration(request.groupID, request.generationID)) {
                    request.sendResponse(pubsub::net::serializeResponse(request.correlationID, net::ErrorCode::REJOIN,
                                                                        std::vector<uint8_t>()));
                    return;
                }
                try {
                    uint64_t tmpLastOffset = partition->getNextOffset();
                    storage::RecordBatch recordBatch;
                    if (partition->read(request.fetchOffset, recordBatch) and recordBatch.numRecords != 0) {

                        std::cout << "Sent response for fetch request: lastOffset=" << tmpLastOffset
                                  << ", numRecords=" << recordBatch.numRecords << "\n";
                        for (size_t i = 0; i < recordBatch.records.size(); ++i) {
                            std::cout << "  record " << i << ": offset=" << recordBatch.records[i].recordOffsetDelta
                                      << " key=" << recordBatch.records[i].key
                                      << " value=" << recordBatch.records[i].value << "\n";
                        }
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
    uint32_t workerID = 0;
    // all the reqs related to data goes through this block
    if (diskRequest.type == TaskType::PRODUCE || diskRequest.type == TaskType::FETCH) {
        std::cerr << "[concurrency/WorkerPool] routing DATA request to worker " << workerID
                  << " (routing key: " << diskRequest.getRoutingKey() << ")\n";
        workerID = (std::hash<std::string>{}(diskRequest.topicName) + diskRequest.partitionID) % workers.size();
    }
    // all the reqs related to group coordinator goes through this block
    else if (diskRequest.type == TaskType::JOIN_GROUP || diskRequest.type == TaskType::LEAVE_GROUP ||
             diskRequest.type == TaskType::CONSUMER_HEARTBEAT || diskRequest.type == TaskType::COMMIT_LOG_OFFSET ||
             diskRequest.type == TaskType::FETCH_LOG_OFFSET) {
        std::cerr << "[concurrency/WorkerPool] routing GROUP request to worker " << workerID
                  << " (group: " << diskRequest.groupID << ")\n";
        workerID = std::hash<std::string>{}(diskRequest.groupID) % workers.size();
    }
    // all the reqs related to admin goes through this block
    else if (diskRequest.type == TaskType::CREATE_TOPIC) {
        std::cerr << "[concurrency/WorkerPool] routing ADMIN request to worker " << workerID << "\n";
        workerID = std::hash<std::string>{}(diskRequest.topicName) % workers.size();
    }
    workers[workerID]->submitRequest(std::move(diskRequest));
}
void WorkerPool::shutDown() {
    for (auto &worker : workers) {
        worker->shutDown();
    }
}
} // namespace pubsub::concurrency