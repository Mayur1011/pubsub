#include "../../include/storage/record.hpp"
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace pubsub::storage {
/*
 * This function will write the data of type T to the serialized buffer.
 * eg. if T is int, it will write 4 bytes to the buffer.
 */
template <typename T> void writeToBuffer(std::vector<uint8_t> &serializedBuffer, const T &data) {
    // reinterpret_cast tell the compiler to treat the data as sequence of bytes (8 bits)
    // [https://en.cppreference.com/cpp/language/reinterpret_cast]
    const uint8_t *dataPtr = reinterpret_cast<const uint8_t *>(&data);
    serializedBuffer.insert(serializedBuffer.end(), dataPtr, dataPtr + sizeof(T));
}

/*
 * Thid func will read size of datatype T bytes from the serialized buff starting from the offset.
 * eg. if T is int, it will read 4 bytes from the buffer starting from the offset.
 */
template <typename T> T readFromBuffer(const std::vector<uint8_t> &serializedBuffer, size_t &offset) {
    if (offset + sizeof(T) > serializedBuffer.size())
        throw std::out_of_range("[storage/record]: Offset out of range");
    T data;
    // https://en.cppreference.com/cpp/iterator/data
    std::memcpy(&data, serializedBuffer.data() + offset, sizeof(T));
    offset += sizeof(T);
    return data;
}
std::vector<uint8_t> serializeRecord(const Record &record) {
    std::vector<uint8_t> serializedBuffer;
    writeToBuffer(serializedBuffer, record.recordOffsetDelta);
    writeToBuffer(serializedBuffer, static_cast<uint32_t>(record.key.size()));
    serializedBuffer.insert(serializedBuffer.end(), record.key.begin(), record.key.end());
    writeToBuffer(serializedBuffer, static_cast<uint32_t>(record.value.size()));
    serializedBuffer.insert(serializedBuffer.end(), record.value.begin(), record.value.end());
    return serializedBuffer;
}

std::vector<uint8_t> serializeRecordBatch(const RecordBatch &batch) {
    std::vector<uint8_t> serializedBuffer;
    writeToBuffer(serializedBuffer, batch.batchStart);
    writeToBuffer(serializedBuffer, batch.baseOffset);
    size_t batch_len_offset = serializedBuffer.size(); // put 32 0 bits as placeholder
    writeToBuffer(serializedBuffer, static_cast<uint32_t>(0));
    writeToBuffer(serializedBuffer, batch.timeStamp);
    writeToBuffer(serializedBuffer, batch.numRecords);
    for (auto record : batch.records) {
        std::vector<uint8_t> serializedRecord = serializeRecord(record);
        serializedBuffer.insert(serializedBuffer.end(), serializedRecord.begin(), serializedRecord.end());
    }
    uint32_t batchLen = static_cast<uint32_t>(serializedBuffer.size() - (batch_len_offset + sizeof(uint32_t)));
    std::memcpy(serializedBuffer.data() + batch_len_offset, &batchLen, sizeof(uint32_t));
    return serializedBuffer;
}

Record deserializeRecord(const std::vector<uint8_t> &serializedBuffer, size_t &currOffset) {
    Record record;
    record.recordOffsetDelta = readFromBuffer<uint32_t>(serializedBuffer, currOffset);
    uint32_t keyLen = readFromBuffer<uint32_t>(serializedBuffer, currOffset);
    if (currOffset + keyLen > serializedBuffer.size())
        throw std::out_of_range("[storage/record]: Key length out of range");
    record.key.assign(serializedBuffer.begin() + currOffset, serializedBuffer.begin() + currOffset + keyLen);
    currOffset += keyLen;
    uint32_t valueLen = readFromBuffer<uint32_t>(serializedBuffer, currOffset);
    if (currOffset + valueLen > serializedBuffer.size())
        throw std::out_of_range("[storage/record]: Value length out of range");
    record.value.assign(serializedBuffer.begin() + currOffset, serializedBuffer.begin() + currOffset + valueLen);
    currOffset += valueLen;
    return record;
}
RecordBatch deserializeRecordBatch(const std::vector<uint8_t> &serializedBuffer) {
    RecordBatch batch;
    size_t currOffset = 0;
    batch.batchStart = readFromBuffer<uint8_t>(serializedBuffer, currOffset);
    batch.baseOffset = readFromBuffer<uint64_t>(serializedBuffer, currOffset);
    batch.batchLen = readFromBuffer<uint32_t>(serializedBuffer, currOffset);
    batch.timeStamp = readFromBuffer<uint64_t>(serializedBuffer, currOffset);
    batch.numRecords = readFromBuffer<uint32_t>(serializedBuffer, currOffset);
    for (uint32_t i = 0; i < batch.numRecords; i++) {
        Record currRecord = deserializeRecord(serializedBuffer, currOffset);
        batch.records.push_back(currRecord);
    }
    return batch;
}
} // namespace pubsub::storage