#pragma once

#include "net/protocol.hpp"
#include "storage/topicManager.hpp"
#include <chrono>
#include <functional>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
namespace pubsub::concurrency {

// this struct will store teh info related to each consumer/member of the group
struct MemberInfo {
    std::chrono::steady_clock::time_point lastHeartBeat;
    std::vector<uint32_t> assignedPartitions;
};

struct JoinRequest {
    std::string memberID;
    std::string topicName;
    std::function<void(std::vector<uint8_t>)> sendResponse;
};

struct GroupInfo {
    uint32_t generationID = 0;
    std::unordered_map<std::string, MemberInfo> consumerInfo;
    bool isRebalancing = false; // we will set this to true when a rebalance is in progress
    std::vector<JoinRequest> joinRequests;
    std::chrono::steady_clock::time_point rebalanceDeadLine;
};

class GroupCoord {
    // this map will store which consumer groups are active and which members are part of each group along with thier
    // last heartbeat time
    std::unordered_map<std::string, GroupInfo> activeConsumerGroups;
    std::shared_mutex mu;
    std::jthread thread; // this is background thread that checks for consumer timeouts
    bool running = true;
    pubsub::storage::TopicManager *topicManager;

    const int sessionTimeOutTime = 30000; // 30 seconds
    const int checkTime = 3000;           // 3 seconds
    const int rebalanceTime = 3000; // we will not immediately rebalance (assign paritions ot new joined consumer or
                                    // already present once, we will wait for 3 secs)

    // this is the background function that checks for consumer timeouts
    void checkSessions() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(checkTime));
            auto now = std::chrono::steady_clock::now();
            std::unique_lock<std::shared_mutex> lock(mu);
            for (auto &[groupID, group] : activeConsumerGroups) {
                for (auto it = group.consumerInfo.begin(); it != group.consumerInfo.end();) {
                    auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second.lastHeartBeat);
                    if (dur.count() > sessionTimeOutTime) {
                        std::cout << "[concurrency/groupCoord] cosumer " << it->first << " in group " << groupID
                                  << " has timed out" << std::endl;
                        it = group.consumerInfo.erase(it);
                        if (not group.isRebalancing) {
                            group.isRebalancing = true;
                            group.rebalanceDeadLine =
                                std::chrono::steady_clock::now() + std::chrono::milliseconds(rebalanceTime);
                        }
                    } else {
                        ++it;
                    }
                }
                if (group.isRebalancing and now >= group.rebalanceDeadLine) {
                    doRebalancing(groupID, group);
                }
            }
        }
    }

    void doRebalancing(const std::string &groupID, GroupInfo &group) {
        group.generationID++; // increment the generation ID to signal a new rebalance, so the old requests are ignored
        group.isRebalancing = false;
        if (group.joinRequests.empty())
            return;
        std::string currTopicName = group.joinRequests.front().topicName;
        uint32_t numPartitions = topicManager->getPartitionCount(currTopicName);
        std::cout << "[Coordinator] Rebalancing group " << groupID << " for topic " << currTopicName << " (Generation "
                  << group.generationID << ")\n";
        std::vector<std::string> allMembers;
        for (const auto &joinReq : group.joinRequests) {
            group.consumerInfo[joinReq.memberID] = MemberInfo{std::chrono::steady_clock::now(), {}};
        }
        for (const auto &[memberID, info] : group.consumerInfo) {
            allMembers.push_back(memberID);
        }
        for (uint32_t p = 0; p < numPartitions; p++) {
            std::string assignedMember = allMembers[p % allMembers.size()];
            group.consumerInfo[assignedMember].assignedPartitions.push_back(p);
        }

        std::cout << "[Coordinator] Rebalanced group " << groupID << " for topic " << currTopicName << " (Generation "
                  << group.generationID << ")\n";
        // we need to send the latest generation ID back to consumer so the next time they do any fetch/log request then
        // they wull have a valid genID.
        for (auto &joinReq : group.joinRequests) {
            std::vector<uint32_t> &parts = group.consumerInfo[joinReq.memberID].assignedPartitions;
            std::vector<uint8_t> payload;
            payload.resize(sizeof(uint32_t) + sizeof(uint32_t) + (parts.size() * sizeof(uint32_t)));
            uint8_t *ptr = payload.data();
            std::memcpy(ptr, &group.generationID, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            uint32_t count = parts.size();
            std::memcpy(ptr, &count, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            for (uint32_t p : parts) {
                std::memcpy(ptr, &p, sizeof(uint32_t));
                ptr += sizeof(uint32_t);
            }
            joinReq.sendResponse(pubsub::net::serializeResponse(0, pubsub::net::ErrorCode::NONE, payload));
        }
        group.joinRequests.clear();
    }

  public:
    GroupCoord(pubsub::storage::TopicManager *topicManager) : topicManager(topicManager) {
        thread = std::jthread(&GroupCoord::checkSessions, this);
    }
    ~GroupCoord() { running = false; }
    // each cosumer should call this at interval of 30 secs.
    void registerHeartBeat(const std::string &groupID, const std::string &consumerID) {
        std::unique_lock<std::shared_mutex> lock(mu);
        // activeConsumerGroups[groupID][consumerID].lastHeartBeat = std::chrono::steady_clock::now();
        activeConsumerGroups[groupID].consumerInfo[consumerID].lastHeartBeat = std::chrono::steady_clock::now();
    }
    // Called by WorkerThread when a JOIN_GROUP arrives
    void joinGroup(const std::string &groupID, const std::string &memberID, const std::string &topicName,
                   std::function<void(std::vector<uint8_t>)> cb) {
        std::unique_lock<std::shared_mutex> lock(mu);
        GroupInfo &group = activeConsumerGroups[groupID];

        group.joinRequests.push_back({memberID, topicName, std::move(cb)});
        if (not group.isRebalancing) {
            group.isRebalancing = true;
            group.rebalanceDeadLine = std::chrono::steady_clock::now() + std::chrono::milliseconds(rebalanceTime);
        }
    }
    void leaveGroup(const std::string &groupID, const std::string &memberID) {
        std::unique_lock<std::shared_mutex> lock(mu);
        auto group = activeConsumerGroups.find(groupID);
        if (group == activeConsumerGroups.end()) {
            return;
        }
        GroupInfo &groupInfo = group->second;
        auto it = groupInfo.consumerInfo.find(memberID);
        if (it != groupInfo.consumerInfo.end()) {
            std::cout << "[concurrency/groupCoord] leaving group " << groupID << " member " << memberID << std::endl;
            groupInfo.consumerInfo.erase(it);
            if (not groupInfo.isRebalancing) {
                groupInfo.isRebalancing = true;
                groupInfo.rebalanceDeadLine =
                    std::chrono::steady_clock::now() + std::chrono::milliseconds(rebalanceTime);
            }
        }
    }

    // Validate generation IDs for Heartbeats and Commits
    bool validateGeneration(const std::string &groupID, uint32_t requestGeneration) {
        std::shared_lock<std::shared_mutex> lock(mu);
        std::cout << "[concurrency/groupCoord] validateGeneration: groupID=" << groupID
                  << " requestGeneration=" << requestGeneration << "\n";
        auto it = activeConsumerGroups.find(groupID);
        if (it != activeConsumerGroups.end()) {
            return requestGeneration == it->second.generationID;
        }
        return false;
    }
};
} // namespace pubsub::concurrency