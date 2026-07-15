#include "storage/indexFile.hpp"
#include "storage/logSegment.hpp"
#include "storage/record.hpp"

#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace pubsub::storage {
LogSegment::LogSegment(uint64_t _firstRecordOffset, uint32_t _flushInterval, const std::string &_segmentDir)
    : logFD(-1), idxFile(_segmentDir + "/" + getFileName(_firstRecordOffset, "index")),
      firstRecordOffset(_firstRecordOffset), logFileSize(0), indexFileSize(0),
      bytesWrittenSinceLastIdxEntry(INDEX_INTERVAL), flushInterval(_flushInterval), batchesWrittenSinceLastFlush(0) {
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

// here i dont need to check for segment size because the partition code will handle it.
void LogSegment::appendToLog(RecordBatch &recordBatch) {
    std::vector<uint8_t> serializedBatch = serializeRecordBatch(recordBatch);
    // why not &serializedBatch as second arg: &serializedBatch` is a pointer to the std::vector object, not to its byte
    // buffer. So you're writing vector internals to disk, and later reading it so we will get garbage value
    ssize_t bytesWrittenLOG = pwrite(logFD, serializedBatch.data(), serializedBatch.size(), logFileSize);
    if (bytesWrittenLOG != (ssize_t)serializedBatch.size()) {
        throw std::runtime_error("[logsegment]: Failed to write record batch to log file");
    }
    if (bytesWrittenSinceLastIdxEntry >= INDEX_INTERVAL) {
        idxFile.append(recordBatch.baseOffset, logFileSize);
        bytesWrittenSinceLastIdxEntry = 0;
        indexFileSize = idxFile.getIdxFileSize();
    } else if (indexFileSize == 0) {
        idxFile.append(recordBatch.baseOffset, logFileSize);
        indexFileSize = idxFile.getIdxFileSize();
    }
    batchesWrittenSinceLastFlush++;
    logFileSize += serializedBatch.size();
    bytesWrittenSinceLastIdxEntry += serializedBatch.size();
    // std::cerr << "[logsegment]: currentBatchSize= " << serializedBatch.size() << " logFileSize= " << logFileSize
    //           << std::endl;
    // pwrite usually writes to page cache, fdatasync flushes to disk
    if (batchesWrittenSinceLastFlush >= flushInterval) {
        fdatasync(logFD);
        batchesWrittenSinceLastFlush = 0;
    }
}

uint64_t LogSegment::getFirstRecordOffset() { return firstRecordOffset; }

uint64_t LogSegment::getLogFileSize() { return logFileSize; }

IndexEntry LogSegment::getLastIndexEntry() {
    IndexEntry indexEntry;
    if (not idxFile.getLastEntry(&indexEntry))
        return {};
    return indexEntry;
}

/*
 * Read the record at given offset from the log file
 */
bool LogSegment::readFromLog(uint64_t offset, Record &outRecord) {
    indexFileSize = idxFile.getIdxFileSize();
    std::cerr << "[logsegment]: readFromLog offset= " << offset << " logFileSize= " << logFileSize
              << " indexFileSize= " << indexFileSize << std::endl;
    if (indexFileSize == 0)
        return false;
    int64_t currPtr = idxFile.lookup(offset); // currPtr is the byte position in the log file
    if (currPtr == -1)
        return false;
    // std::cerr << "[logsegment]: currPtr= " << currPtr << " logFileSize= " << logFileSize << std::endl;
    while ((uint64_t)currPtr < logFileSize) {
        uint32_t currBatchLen;
        // first 9 bytes are batchStart and baseOffset
        if (pread(logFD, &currBatchLen, sizeof(uint32_t), currPtr + 9) != sizeof(uint32_t))
            return false;
        std::cerr << "[logsegment]: currPtr= " << currPtr << " currBatchLen= " << currBatchLen << std::endl;
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
    std::cerr << "[logsegment]: Failed to find record at offset " << offset << "\n";
    return false;
}

uint64_t LogSegment::size() { return logFileSize; }
} // namespace pubsub::storage