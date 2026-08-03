#pragma once

#include "storage/partition.hpp"
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace pubsub::storage {
struct RecoveryInfo {
    std::string topicName;
    uint32_t partitionId;
    Partition *partition;
};

class TopicManager {
    std::unordered_map<std::string, std::unordered_map<uint32_t, std::unique_ptr<Partition>>>
        topicPartitionMap;         // topic -> partitionID -> Partition
    std::filesystem::path baseDir; // base directory for storing topic partitions
    std::shared_mutex topicManagerMU;

    // hashmap to store topic - commit log offset of consumers
    std::unordered_map<std::string, uint64_t> commitLogOffsetMap;
    std::shared_mutex commitLogOffsetMapMU;

  public:
    explicit TopicManager(std::filesystem::path baseDir) : baseDir(std::move(baseDir)) {
        std::filesystem::create_directories(baseDir / "topics");
    }
    std::vector<RecoveryInfo> recoverTopics() {
        std::unique_lock<std::shared_mutex> lock(topicManagerMU);
        std::vector<RecoveryInfo> recoveryInfo;
        std::filesystem::path topicsPath = baseDir / "topics";
        if (not std::filesystem::exists(topicsPath))
            return recoveryInfo;
        for (const auto &topicDir : std::filesystem::directory_iterator(topicsPath)) {
            if (not topicDir.is_directory())
                continue;
            std::string topicName = topicDir.path().filename().string();
            for (const auto &partitionDir : std::filesystem::directory_iterator(topicDir)) {
                if (not partitionDir.is_directory())
                    continue;
                std::string dirName = partitionDir.path().filename().string();
                if (dirName.rfind("partition_", 0) != 0)
                    continue;
                try {
                    uint32_t partitionId = std::stoul(dirName.substr(10));
                    topicPartitionMap[topicName][partitionId] =
                        std::make_unique<Partition>(partitionDir.path().string());
                    recoveryInfo.push_back({topicName, partitionId, topicPartitionMap[topicName][partitionId].get()});
                    std::cout << "[storage/TopicManager] Recovered Topic: " << topicName
                              << ", Partition: " << partitionId << "\n";
                } catch (const std::exception &e) {
                    std::cerr << "[storage/topicManager] Failed to recover partition: " << dirName << " " << e.what()
                              << std::endl;
                }
            }
        }
        // recoverCommitLogOffset();
        return recoveryInfo;
    }
    void createTopic(const std::string &topicName, uint32_t numPartitions) {
        std::unique_lock<std::shared_mutex> lock(topicManagerMU);
        for (uint32_t partitionId = 0; partitionId < numPartitions; partitionId++) {
            if (topicPartitionMap.find(topicName) != topicPartitionMap.end() &&
                topicPartitionMap[topicName].find(partitionId) != topicPartitionMap[topicName].end()) {
                continue;
            }
            // data/topics/<topicName>/partition_<partitionId>
            std::filesystem::path partitionPath =
                baseDir / "topics" / topicName / ("partition_" + std::to_string(partitionId));
            std::filesystem::create_directories(partitionPath);
            topicPartitionMap[topicName][partitionId] = std::make_unique<Partition>(partitionPath);
            std::cout << "[storage/TopicManager] Created/Loaded topic '" << topicName << "' partition " << partitionId
                      << " at " << partitionPath << "\n";
        }
    }
    Partition *getPartition(const std::string &topicName, uint32_t partitionId) {
        std::shared_lock<std::shared_mutex> lock(topicManagerMU);
        auto topicPtr = topicPartitionMap.find(topicName);
        if (topicPtr == topicPartitionMap.end())
            return nullptr;
        auto partitionPtr = topicPtr->second.find(partitionId);
        if (partitionPtr == topicPtr->second.end())
            return nullptr;
        return partitionPtr->second.get();
    }
    uint32_t getPartitionCount(const std::string &topicName) {
        std::shared_lock<std::shared_mutex> lock(topicManagerMU);
        auto topicPtr = topicPartitionMap.find(topicName);
        if (topicPtr == topicPartitionMap.end())
            return 0;
        return topicPtr->second.size();
    }

    // helper for log map
    void createPartitionCommitLog(const std::string &topicName, uint32_t paritionID) {
        std::unique_lock<std::shared_mutex> lock(topicManagerMU);
        auto topicIt = topicPartitionMap.find(topicName);
        if (topicIt != topicPartitionMap.end()) {
            if (topicIt->second.find(paritionID) != topicIt->second.end()) {
                return;
            }
        }
        std::filesystem::path partitionDir =
            baseDir / "topics" / topicName / ("partition_" + std::to_string(paritionID));
        std::filesystem::create_directories(partitionDir);
        topicPartitionMap[topicName][paritionID] = std::make_unique<Partition>(partitionDir.string());
        std::cout << "[storage/topicManager] Initialized partition: " << topicName << ":" << paritionID << "\n";
    }
    void initializeOffsetPartition() {
        for (int i = 0; i < 50; i++) {
            createPartitionCommitLog("consumer_offsets", i);
        }
    }
    void updateCommitLogOffset(const std::string &key, uint64_t offset) {
        std::unique_lock<std::shared_mutex> lock(commitLogOffsetMapMU);
        commitLogOffsetMap[key] = offset;
    }
    int64_t getCommitLogOffset(const std::string &key) {
        std::shared_lock<std::shared_mutex> lock(commitLogOffsetMapMU);
        auto it = commitLogOffsetMap.find(key);
        if (it != commitLogOffsetMap.end()) {
            return it->second;
        }
        return -1;
    }
    void recoverCommitLogOffset() {
        std::cout << "[storage/topicManager] Rebuilding consumer_offsets log to build memory cache...\n";

        for (int i = 0; i < 50; ++i) {
            pubsub::storage::Partition *partition = topicPartitionMap["consumer_offsets"][i].get();
            uint64_t scanOffset = 0;
            pubsub::storage::RecordBatch recordBatch;
            // Loop and read sequentially through the internal partition until we run out of batches
            while (partition->read(scanOffset, recordBatch)) {
                for (const auto &record : recordBatch.records) {
                    uint64_t offsetVal;
                    std::memcpy(&offsetVal, record.value.data(), sizeof(uint64_t));
                    // Record.key matches "group_id:topic:partition"
                    // Naturally overwrites old entries with the newest sequence position!
                    commitLogOffsetMap[record.key] = offsetVal;
                }
                // Advance past this batch to scan the next one
                scanOffset = recordBatch.baseOffset + recordBatch.numRecords;
            }
        }
        std::cout << "[storage/topicManager] Offset cache recovery complete.\n";
    }
};
} // namespace pubsub::storage