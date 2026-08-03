#pragma once

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

    void doRebalancing(const std::string &groupID, GroupInfo &group) {}

  public:
    GroupCoord() { thread = std::jthread(&GroupCoord::checkSessions, this); }
    ~GroupCoord() { running = false; }
    // each cosumer should call this at interval of 30 secs.
    void registerHeartBeat(const std::string &groupID, const std::string &consumerID) {
        std::unique_lock<std::shared_mutex> lock(mu);
        activeConsumerGroups[groupID][consumerID].lastHeartBeat = std::chrono::steady_clock::now();
    }
};
} // namespace pubsub::concurrency