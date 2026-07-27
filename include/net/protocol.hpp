#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace pubsub::net {
enum class RequestType : uint8_t {
    PRODUCE = 0x01,
    FETCH = 0x02,
    COMMIT_LOG_OFFSET = 0x03,
    FETCH_LOG_OFFSET = 0x04,
};
enum class ErrorCode : uint8_t {
    NONE = 0x00,
    UNKNOWN_SERVER_ERROR = 0x01,
    TOPIC_NOT_FOUND = 0x02,
    PARTITION_NOT_FOUND = 0x03,
    INVALID_OFFSET = 0x04,
    CORRUPTED_MESSAGE_BATCH = 0x05,
};
struct RequestHeader {
    uint32_t frameLen; // total bytes excluding this field
    RequestType requestType;
    uint32_t correlationId; // correlation id for the request
    std::string clientId;
};
struct ResponseHeader {
    uint32_t frameLen; // total bytes excluding this field (header + payload)
    ErrorCode errorCode;
    uint32_t correlationId; // correlation id for the request
};
struct ProducePayload {
    std::string topic;
    uint32_t partitionID;
    int8_t acks;
    std::vector<uint8_t> rawRecordBatch;
};
struct FetchPayload {
    std::string topic;
    uint32_t partitionID;
    uint64_t fetchOffset; // offset to start fetching from
};
struct FetchResponse {
    uint64_t lastOffset; // highest offset in log file
    std::vector<uint8_t> rawRecordBatch;
};

// Serialization
std::vector<uint8_t> serializeRequestHeader(const RequestHeader &reqHeader);
std::vector<uint8_t> serializeProducePayload(const ProducePayload &payload);
std::vector<uint8_t> serializeProduceRequest(const RequestHeader &reqHeader, const ProducePayload &payload);
std::vector<uint8_t> serializeFetchPayload(const FetchPayload &payload);
std::vector<uint8_t> serializeFetchRequest(const RequestHeader &reqHeader, const FetchPayload &payload);
std::vector<uint8_t> serializeResponse(uint32_t correlationId, ErrorCode errorCode,
                                       const std::vector<uint8_t> &payload);
std::vector<uint8_t> serializeFetchResponse(uint64_t lastOffset, const std::vector<uint8_t> &payload);
// Deserialization
RequestHeader deserializeRequestHeader(const std::vector<uint8_t> &buffer, size_t &offset);
ProducePayload deserializeProducePayload(const std::vector<uint8_t> &buffer, size_t &offset);
FetchPayload deserializeFetchPayload(const std::vector<uint8_t> &buffer, size_t &offset);

// standard FNV-1a hash function
inline uint32_t fnv1aHash(const std::string &str) {
    uint32_t hash = 2166136261u;
    for (char c : str) {
        hash ^= static_cast<uint32_t>(c);
        hash *= 16777619u;
    }
    return hash;
}

inline std::string make_offset_key(const std::string &group_id, const std::string &topic, uint32_t partition) {
    return group_id + ":" + topic + ":" + std::to_string(partition);
}

} // namespace pubsub::net