#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "net/protocol.hpp"

namespace pubsub::net {
template <typename T> T readFromNetBuffer(const std::vector<uint8_t> &buffer, size_t &offset) {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    if (offset + sizeof(T) > buffer.size()) {
        throw std::out_of_range("[Protocol]: Buffer read overflow");
    }
    T value;
    std::memcpy(&value, buffer.data() + offset, sizeof(T));
    offset += sizeof(T);
    return value;
}
std::string readStringFromNetBuffer(const std::vector<uint8_t> &buffer, size_t &offset) {
    uint16_t len = readFromNetBuffer<uint16_t>(buffer, offset);
    if (offset + len > buffer.size()) {
        throw std::out_of_range("[net/protocol]: Buffer read overflow while reading string");
    }
    std::string value(reinterpret_cast<const char *>(buffer.data() + offset), len);
    offset += len;
    return value;
}
template <typename T> void writeToNetBufferCopyable(std::vector<uint8_t> &buffer, const T &value) {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    const uint8_t *ptr = reinterpret_cast<const uint8_t *>(&value);
    buffer.insert(buffer.end(), ptr, ptr + sizeof(T));
}

void writeToNetBufferString(std::vector<uint8_t> &buffer, const std::string &s) {
    uint16_t len = static_cast<uint16_t>(s.size());
    writeToNetBufferCopyable(buffer, len);
    buffer.insert(buffer.end(), s.begin(), s.end());
}
void writeToNetBufferBytes(std::vector<uint8_t> &buffer, const std::vector<uint8_t> &bytes) {
    buffer.insert(buffer.end(), bytes.begin(), bytes.end());
}

std::vector<uint8_t> serializeRequestHeader(const RequestHeader &reqHeader) {
    std::vector<uint8_t> serializedBuffer;
    writeToNetBufferCopyable(serializedBuffer, reqHeader.frameLen);
    writeToNetBufferCopyable(serializedBuffer, static_cast<uint8_t>(reqHeader.requestType));
    writeToNetBufferCopyable(serializedBuffer, reqHeader.correlationId);
    writeToNetBufferString(serializedBuffer, reqHeader.clientId);
    return serializedBuffer;
}
std::vector<uint8_t> serializeProducePayload(const ProducePayload &payload) {
    std::vector<uint8_t> serializedBuffer;
    writeToNetBufferString(serializedBuffer, payload.topic);
    writeToNetBufferCopyable(serializedBuffer, payload.partitionID);
    writeToNetBufferCopyable(serializedBuffer, payload.acks);
    writeToNetBufferBytes(serializedBuffer, payload.rawRecordBatch);
    return serializedBuffer;
}
std::vector<uint8_t> serializeProduceRequest(const RequestHeader &reqHeader, const ProducePayload &payload) {
    std::vector<uint8_t> headerSerializedBuffer = serializeRequestHeader(reqHeader);
    std::vector<uint8_t> payloadSerializedBuffer = serializeProducePayload(payload);
    // now first 4 bytes (uint32_t) of the header should be the total size of the request
    uint32_t frameLen =
        static_cast<uint32_t>(headerSerializedBuffer.size() + payloadSerializedBuffer.size() - sizeof(uint32_t));
    std::memcpy(headerSerializedBuffer.data(), &frameLen, sizeof(frameLen));
    headerSerializedBuffer.insert(headerSerializedBuffer.end(), payloadSerializedBuffer.begin(),
                                  payloadSerializedBuffer.end());
    return headerSerializedBuffer;
}
std::vector<uint8_t> serializeFetchPayload(const FetchPayload &payload) {
    std::vector<uint8_t> serializedBuffer;
    writeToNetBufferString(serializedBuffer, payload.topic);
    writeToNetBufferCopyable<uint32_t>(serializedBuffer, payload.partitionID);
    writeToNetBufferCopyable<uint64_t>(serializedBuffer, payload.fetchOffset);
    return serializedBuffer;
}
std::vector<uint8_t> serializeFetchRequest(const RequestHeader &reqHeader, const FetchPayload &payload) {
    std::vector<uint8_t> headerSerializedBuffer = serializeRequestHeader(reqHeader);
    std::vector<uint8_t> payloadSerializedBuffer = serializeFetchPayload(payload);
    uint32_t frameLen =
        static_cast<uint32_t>(headerSerializedBuffer.size() + payloadSerializedBuffer.size() - sizeof(uint32_t));
    std::memcpy(headerSerializedBuffer.data(), &frameLen, sizeof(frameLen));
    headerSerializedBuffer.insert(headerSerializedBuffer.end(), payloadSerializedBuffer.begin(),
                                  payloadSerializedBuffer.end());
    return headerSerializedBuffer;
}

std::vector<uint8_t> serializeResponse(uint32_t correlationId, ErrorCode errorCode,
                                       const std::vector<uint8_t> &payload) {
    std::vector<uint8_t> responseBuffer;
    writeToNetBufferCopyable<uint32_t>(responseBuffer, 0);
    writeToNetBufferCopyable<uint8_t>(responseBuffer, static_cast<uint8_t>(errorCode));
    writeToNetBufferCopyable<uint32_t>(responseBuffer, correlationId);
    // Sending the response payload immediately after the header
    if (!payload.empty()) {
        responseBuffer.insert(responseBuffer.end(), payload.begin(), payload.end());
    }
    uint32_t totalFrameLength = static_cast<uint32_t>(responseBuffer.size() - sizeof(uint32_t));
    std::memcpy(responseBuffer.data(), &totalFrameLength, sizeof(uint32_t));
    return responseBuffer;
}
std::vector<uint8_t> serializeFetchResponse(uint64_t lastOffset, const std::vector<uint8_t> &payload) {
    std::vector<uint8_t> responseBuffer;
    writeToNetBufferCopyable<uint64_t>(responseBuffer, lastOffset);
    writeToNetBufferBytes(responseBuffer, payload);
    return responseBuffer;
}

OffsetCommitPayload deserializeOffsetCommitPayload(const std::vector<uint8_t> &buffer, size_t &offset) {
    OffsetCommitPayload payload;
    payload.groupID = readStringFromNetBuffer(buffer, offset);
    payload.topic = readStringFromNetBuffer(buffer, offset);
    payload.partitionID = readFromNetBuffer<uint32_t>(buffer, offset);
    payload.committedLogOffset = readFromNetBuffer<uint64_t>(buffer, offset);
    payload.generationID = readFromNetBuffer<uint32_t>(buffer, offset);
    return payload;
}
OffsetFetchPayload deserializeOffsetFetchPayload(const std::vector<uint8_t> &buffer, size_t &offset) {
    OffsetFetchPayload payload;
    payload.groupID = readStringFromNetBuffer(buffer, offset);
    payload.topic = readStringFromNetBuffer(buffer, offset);
    payload.partitionID = readFromNetBuffer<uint32_t>(buffer, offset);
    return payload;
}
HeartBeatPayload deserializeHeartBeatPayload(const std::vector<uint8_t> &buffer, size_t &offset) {
    HeartBeatPayload payload;
    payload.groupID = readStringFromNetBuffer(buffer, offset);
    payload.memberID = readStringFromNetBuffer(buffer, offset);
    payload.generationID = readFromNetBuffer<uint32_t>(buffer, offset);
    return payload;
}
JoinGroupPayload deserializeJoinGroupPayload(const std::vector<uint8_t> &buffer, size_t &offset) {
    JoinGroupPayload payload;
    payload.groupID = readStringFromNetBuffer(buffer, offset);
    payload.memberID = readStringFromNetBuffer(buffer, offset);
    payload.generationID = readFromNetBuffer<uint32_t>(buffer, offset);
    return payload;
}

// this func will get a stream buffer from net buffer and deserialize it into a RequestHeader
RequestHeader deserializeRequestHeader(const std::vector<uint8_t> &buffer, size_t &offset) {
    RequestHeader header;
    // Note: FrameLength was already read by the network socket to allocate this buffer,
    // but we track its logical position or extract it directly if present.

    std::cerr << "[net/protocol] deserializeRequestHeader: frameLen=" << buffer.size() << " offset=" << offset
              << std::endl;
    header.frameLen = static_cast<uint32_t>(buffer.size() - 4);
    header.requestType = static_cast<RequestType>(readFromNetBuffer<uint8_t>(buffer, offset));
    std::cerr << "[net/protocol] deserializeRequestHeader: requestType=" << static_cast<int>(header.requestType)
              << " offset=" << offset << std::endl;
    header.correlationId = readFromNetBuffer<uint32_t>(buffer, offset);
    std::cerr << "[net/protocol] deserializeRequestHeader: correlationId=" << header.correlationId
              << " offset=" << offset << std::endl;
    // the net buffer contains a 2-byte length prefix for the clientId
    uint16_t clientIdLen = readFromNetBuffer<uint16_t>(buffer, offset);
    if (offset + clientIdLen > buffer.size()) {
        throw std::runtime_error("[net/protocol]: Corrupted ClientId length boundary");
    }
    header.clientId.assign(reinterpret_cast<const char *>(buffer.data() + offset), clientIdLen);
    offset += clientIdLen;
    return header;
}
ProducePayload deserializeProducePayload(const std::vector<uint8_t> &buffer, size_t &offset) {
    ProducePayload payload;
    // same like above the net buffer contains a 2-byte length prefix for the topic name
    uint16_t topicLen = readFromNetBuffer<uint16_t>(buffer, offset);
    // std::cout << "[net/protocol] topicLen: " << topicLen << "\n";
    if (offset + topicLen > buffer.size()) {
        throw std::runtime_error("[net/protocol]: Corrupted Topic length in Produce payload");
    }
    payload.topic.assign(reinterpret_cast<const char *>(buffer.data() + offset), topicLen);
    offset += topicLen;
    payload.partitionID = readFromNetBuffer<uint32_t>(buffer, offset);
    payload.acks = readFromNetBuffer<int8_t>(buffer, offset);
    // The rest of the payload frame is our raw binary RecordBatch like in storage
    if (offset < buffer.size()) {
        payload.rawRecordBatch.assign(buffer.begin() + offset, buffer.end());
        offset = buffer.size();
    }
    return payload;
}
FetchPayload deserializeFetchPayload(const std::vector<uint8_t> &buffer, size_t &offset) {
    FetchPayload payload;
    uint16_t topicLen = readFromNetBuffer<uint16_t>(buffer, offset);
    if (offset + topicLen > buffer.size()) {
        throw std::runtime_error("[net/protocol]: Corrupted Topic length in Fetch payload");
    }
    payload.topic.assign(reinterpret_cast<const char *>(buffer.data() + offset), topicLen);
    offset += topicLen;
    payload.partitionID = readFromNetBuffer<uint32_t>(buffer, offset);
    payload.fetchOffset = readFromNetBuffer<uint64_t>(buffer, offset);
    return payload;
}

} // namespace pubsub::net