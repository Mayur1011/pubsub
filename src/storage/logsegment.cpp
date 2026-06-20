#include "storage/logsegment.hpp"
#include "storage/record.hpp"

#include <fcntl.h>
#include <iomanip>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace pubsub::storage {
LogSegment::LogSegment(uint64_t _firstRecordOffset, uint32_t _flushInterval, const std::string &_segmentDir) {
    firstRecordOffset = _firstRecordOffset;
    flushInterval = _flushInterval;
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

    ssize_t bytesWritten = pwrite(logFD, &serializedBatch, serializedBatch.size(), logFileSize);
    if (bytesWritten != (ssize_t)serializedBatch.size()) {
    }
}
} // namespace pubsub::storage