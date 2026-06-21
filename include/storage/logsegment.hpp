#pragma once

#include "storage/record.hpp"
#include <cstdint>
#include <string>

namespace pubsub::storage {
class LogSegment {
    int logFD;                              // file descriptor for .log file
    int indexFD;                            // file descriptor for .index file for the .log file
    uint64_t firstRecordOffset;             // offset or first batch in this .log file
    uint64_t logFileSize;                   // size of the .log file in bytes
    uint64_t indexFileSize;                 // size of the .index file in bytes
    uint64_t bytesWrittenSinceLastIdxEntry; // number of bytes written to the .index file till now
    uint32_t flushInterval;                 // after how many records we should flush the .log file
                                            // on disk (initially it is stored in page cache of RAM)
    uint32_t batchesWrittenSinceLastFlush;

    std::string getFileName(uint64_t offset, const std::string &fileExtension);

  public:
    static const uint32_t INDEX_INTERVAL = 4096;
    /*
     * firstRecordOffset is the offset of the first batch in this .log file
     */
    LogSegment(uint64_t _firstRecordOffset, uint32_t _flushInterval, const std::string &_segmentDir);
    ~LogSegment();
    void appendToLog(RecordBatch &recordBatch);
    bool readFromLog(uint64_t recordOffset, Record &record);
};
} // namespace pubsub::storage