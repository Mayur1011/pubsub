#pragma once

#include "storage/partition.hpp"
#include <cstdint>
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
    std::shared_mutex mu;

  public:
    explicit TopicManager(std::filesystem::path baseDir) : baseDir(std::move(baseDir)) {
        std::filesystem::create_directories(baseDir / "topics");
    }
    std::vector<RecoveryInfo> recoverTopics() {
        std::unique_lock<std::shared_mutex> lock(mu);
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
        return recoveryInfo;
    }

    void createTopic(const std::string &topicName, uint32_t numPartitions) {
        std::unique_lock<std::shared_mutex> lock(mu);
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
        std::shared_lock<std::shared_mutex> lock(mu);
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