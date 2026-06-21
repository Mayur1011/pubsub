#include "storage/indexFile.hpp"
#include "storage/logsegment.hpp"
#include "storage/record.hpp"

#include <fcntl.h>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace pubsub::storage {
LogSegment::LogSegment(uint64_t _firstRecordOffset, uint32_t _flushInterval, const std::string &_segmentDir)
    : logFD(-1), idxFile(_segmentDir + "/" + getFileName(_firstRecordOffset, "index")),
      firstRecordOffset(_firstRecordOffset), logFileSize(0), indexFileSize(0),
      bytesWrittenSinceLastIdxEntry(INDEX_INTERVAL), flushInterval(_flushInterval) {
    std::string logFilePath = _segmentDir + "/" + getFileName(firstRecordOffset, "log");
    logFD = open(logFilePath.c_str(), O_RDWR | O_CREAT, 0644);
    if (logFD == -1) {
        throw std::runtime_error("[logsegment]: Failed to open log file");
    }
    struct stat st;
    if (fstat(logFD, &st) == -1) {
        throw std::runtime_error("[logsegment]: Failed to get file size");
    }
    logFileSize = st.st_size;
}

LogSegment::~LogSegment() {
    if (logFD != -1) {
        fdatasync(logFD);
        close(logFD);
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
        idxFile.append(recordBatch.baseOffset, logFileSize);
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
    int64_t currPtr = idxFile.lookup(offset);
    if (currPtr == -1)
        return false;
    while ((uint64_t)currPtr < logFileSize) {
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

uint64_t LogSegment::size() { return logFileSize; }
} // namespace pubsub::storage