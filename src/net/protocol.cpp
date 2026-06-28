#include "net/protocol.hpp"
#include <cstring>
#include <iostream>
#include <stdexcept>

namespace pubsub::net {
template <typename T> T readFromNetBuffer(const std::vector<uint8_t> &buffer, size_t &offset) {
    if (offset + sizeof(T) > buffer.size()) {
        throw std::out_of_range("[Protocol]: Buffer read overflow");
    }
    T value;
    std::memcpy(&value, buffer.data() + offset, sizeof(T));
    offset += sizeof(T);
    return value;
}
template <typename T> void writeToNetBuffer(std::vector<uint8_t> &buffer, const T &value) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t *>(&value);
    buffer.insert(buffer.end(), ptr, ptr + sizeof(T));
}
// this func will get a stream buffer from net buffer and deserialize it into a RequestHeader
RequestHeader deserializeRequestHeader(const std::vector<uint8_t> &buffer, size_t &offset) {
    RequestHeader header;
    // Note: FrameLength was already read by the network socket to allocate this buffer,
    // but we track its logical position or extract it directly if present.

    std::cerr << "[net/protocol] deserializeRequestHeader: frameLen=" << buffer.size() << " offset=" << offset
              << std::endl;
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
    std::cout << "[net/protocol] topicLen: " << topicLen << "\n";
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
    payload.maxBytes = readFromNetBuffer<uint32_t>(buffer, offset);
    return payload;
}
std::vector<uint8_t> serializeResponse(uint32_t correlationId, ErrorCode errorCode,
                                       const std::vector<uint8_t> &payload) {
    std::vector<uint8_t> responseBuffer;
    writeToNetBuffer<uint32_t>(responseBuffer, 0);
    writeToNetBuffer<uint32_t>(responseBuffer, correlationId);
    writeToNetBuffer<uint16_t>(responseBuffer, static_cast<uint16_t>(errorCode));
    // Sending the response payload immediately after the header
    if (!payload.empty()) {
        responseBuffer.insert(responseBuffer.end(), payload.begin(), payload.end());
    }
    uint32_t totalFrameLength = static_cast<uint32_t>(responseBuffer.size() - sizeof(uint32_t));
    std::memcpy(responseBuffer.data(), &totalFrameLength, sizeof(uint32_t));
    return responseBuffer;
}

} // namespace pubsub::net