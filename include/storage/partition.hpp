#pragma once
// this mangages the segments for a single partition

#include "storage/logSegment.hpp"
#include "storage/record.hpp"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

namespace pubsub::storage {

class Partition {
    std::vector<std::unique_ptr<LogSegment>> segments;
    LogSegment *currentSegment;       // pointer to the current log segment
    uint64_t nextRecordOffset;        // offset for the next record to be created
    std::filesystem::path segmentDir; // directory where the segment files are stored
    void recoverAndInitLogSegments();
    void createNewSegment(uint64_t newSegmentOffset);
    void checkNewSegment(uint64_t recordBatchSize);

  public:
    static const uint64_t SEGMENT_SIZE_LIMIT = 16 * 1024 * 1024;
    Partition(const std::string &baseDir);
    ~Partition() = default;
    void append(const RecordBatch &recordBatch);
    bool read(uint64_t offset, RecordBatch &recordBatch);
    uint64_t getNextOffset() const; // this function returns the offset for the next record to be created
};
} // namespace pubsub::storage