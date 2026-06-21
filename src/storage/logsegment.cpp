#include "storage/logsegment.hpp"
#include "storage/record.hpp"

#include <algorithm>
#include <fcntl.h>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace pubsub::storage {
LogSegment::LogSegment(uint64_t _firstRecordOffset, uint32_t _flushInterval, const std::string &_segmentDir) {
    firstRecordOffset = _firstRecordOffset;
    flushInterval = _flushInterval;
    bytesWrittenSinceLastIdxEntry = INDEX_INTERVAL;
    std::string logFilePath = _segmentDir + "/" + getFileName(firstRecordOffset, "log");
    std::string indexFilePath = _segmentDir + "/" + getFileName(firstRecordOffset, "index");
    logFD = open(logFilePath.c_str(), O_RDWR | O_CREAT, 0644);
    indexFD = open(indexFilePath.c_str(), O_RDWR | O_CREAT, 0644);
    if (logFD == -1 || indexFD == -1) {
        throw std::runtime_error("[logsegment]: Failed to open log or index file");
    }
    struct stat st;
    if (fstat(logFD, &st) == -1 || fstat(indexFD, &st) == -1) {
        throw std::runtime_error("[logsegment]: Failed to get file size");
    }
    logFileSize = st.st_size;
    indexFileSize = st.st_size;
}

LogSegment::~LogSegment() {
    if (logFD != -1) {
        fdatasync(logFD);
        close(logFD);
    }
    if (indexFD != -1) {
        fdatasync(indexFD);
        close(indexFD);
    }
}

std::string LogSegment::getFileName(uint64_t offset, const std::string &fileExtension) {
    std::stringstream ss;
    ss << std::setw(10) << std::setfill('0') << offset << "." << fileExtension;
    return ss.str();
}

void LogSegment::appendToLog(RecordBatch &recordBatch) {
    std::vector<uint8_t> serializedBatch = serializeRecordBatch(recordBatch);
    ssize_t bytesWrittenLOG = pwrite(logFD, &serializedBatch, serializedBatch.size(), logFileSize);
    if (bytesWrittenLOG != (ssize_t)serializedBatch.size()) {
        throw std::runtime_error("[logsegment]: Failed to write record batch to log file");
    }
    if (bytesWrittenSinceLastIdxEntry >= INDEX_INTERVAL) {
        SparseIndex entry;
        entry.baseOffset = recordBatch.baseOffset;
        entry.byteOffset = logFileSize;
        ssize_t bytesWrittenIDX = pwrite(indexFD, &entry, sizeof(entry), indexFileSize);
        if (bytesWrittenIDX != sizeof(SparseIndex))
            throw std::runtime_error("[logsegment]: Failed to write entry in .index file");
        indexFileSize += sizeof(SparseIndex);
        bytesWrittenSinceLastIdxEntry = 0;
    }
    bytesWrittenSinceLastIdxEntry += serializedBatch.size();
    batchesWrittenSinceLastFlush++;
    logFileSize += serializedBatch.size();
    if (batchesWrittenSinceLastFlush >= flushInterval) {
        fdatasync(logFD);
        batchesWrittenSinceLastFlush = 0;
    }
}

/*
 * Read the record at given offset from the log file
 */
bool LogSegment::readFromLog(uint64_t offset, Record &outRecord) {
    if (indexFileSize == 0)
        return false;
    void *mmapIdxPtr = mmap(nullptr, indexFileSize, PROT_READ, MAP_SHARED, indexFD, 0);
    if (mmapIdxPtr == MAP_FAILED)
        throw std::runtime_error("[logsegment]: Failed to mmap .index file");
    SparseIndex *sparseIndex = reinterpret_cast<SparseIndex *>(mmapIdxPtr);
    size_t numEntries = indexFileSize / sizeof(SparseIndex);

    auto it = std::upper_bound(sparseIndex, sparseIndex + numEntries, offset,
                               [](uint64_t offset, const SparseIndex &currval) { return currval.baseOffset < offset; });
    if (it != sparseIndex)
        --it;
    else {
        munmap(mmapIdxPtr, indexFileSize);
        return false;
    }
    uint64_t currPtr = it->byteOffset;
    munmap(mmapIdxPtr, indexFileSize);
    while (currPtr < logFileSize) {
        uint32_t currBatchLen;
        // first 9 bytes are batchStart and baseOffset
        if (pread(logFD, &currBatchLen, sizeof(uint32_t), currPtr + 9) != sizeof(uint32_t))
            return false;
        size_t totalBatchSize = currBatchLen + 13;
        std::vector<uint8_t> batchBuffer(totalBatchSize);
        if (pread(logFD, batchBuffer.data(), totalBatchSize, currPtr) != (ssize_t)totalBatchSize)
            return false;
        RecordBatch batch = deserializeRecordBatch(batchBuffer);
        if (offset >= batch.baseOffset and offset < (batch.baseOffset + batch.numRecords)) {
            for (Record record : batch.records) {
                if (batch.baseOffset + record.recordOffsetDelta == offset) {
                    outRecord = record;
                    return true;
                }
            }
        }
        currPtr += totalBatchSize;
    }
    return false;
}

} // namespace pubsub::storage