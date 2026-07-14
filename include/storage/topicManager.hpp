#pragma once

#include "storage/partition.hpp"
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <unordered_map>

namespace pubsub::storage {
class TopicManager {
    std::unordered_map<std::string, std::unordered_map<uint32_t, std::unique_ptr<Partition>>>
        topicPartitionMap; // topic -> partitionID -> Partition
    std::string baseDir;   // base directory for storing topic partitions
  public:
    explicit TopicManager(std::string baseDir) : baseDir(std::move(baseDir)) {
        std::filesystem::create_directories(baseDir);
    }
    void createTopic(const std::string &topicName, uint32_t numPartitions) {
        for (uint32_t partitionId = 0; partitionId < numPartitions; partitionId++) {
            // data/topics/<topicName>/partition_<partitionId>
            std::string partitionPath = baseDir + "/" + topicName + "/partition_" + std::to_string(partitionId);
            std::filesystem::create_directories(partitionPath);
            topicPartitionMap[topicName][partitionId] = std::make_unique<Partition>(partitionPath);
            std::cout << "[storage/TopicManager] Created/Loaded topic '" << topicName << "' partition " << partitionId
                      << " at " << partitionPath << "\n";
        }
    }
    Partition *getPartition(std::string &topicName, uint32_t partitionId) {
        auto topicPtr = topicPartitionMap.find(topicName);
        if (topicPtr == topicPartitionMap.end())
            return nullptr;
        auto partitionPtr = topicPtr->second.find(partitionId);
        if (partitionPtr == topicPtr->second.end())
            return nullptr;
        return partitionPtr->second.get();
    }
};
} // namespace pubsub::storage