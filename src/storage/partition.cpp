#include "storage/indexFile.hpp"
#include "storage/partition.hpp"
#include "storage/record.hpp"
#include <algorithm>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace pubsub::storage {
Partition::Partition(const std::string &baseDir) : currentSegment(nullptr), nextRecordOffset(0), segmentDir(baseDir) {
    if (not std::filesystem::exists(baseDir))
        std::filesystem::create_directories(baseDir);
    recoverAndInitLogSegments();
}
void Partition::recoverAndInitLogSegments() {
    std::vector<uint64_t> baseOffsets;
    for (const auto &file : std::filesystem::directory_iterator(segmentDir)) {
        if (file.is_regular_file() and file.path().extension() == ".log") {
            uint64_t offset = std::stoull(file.path().stem().string());
            baseOffsets.push_back(offset);
        }
    }
    std::sort(baseOffsets.begin(), baseOffsets.end());
    if (baseOffsets.empty()) {
        createNewSegment(0);
        return;
    }
    for (auto offset : baseOffsets)
        segments.push_back(std::make_unique<LogSegment>(offset, 10, segmentDir.string()));
    currentSegment = segments.back().get();
    IndexEntry lastIdxEntry = currentSegment->getLastIndexEntry();
    // if there is no last index entry, the getLastIndexEntry() return {}.
    uint64_t seekPosition = 0;
    nextRecordOffset = currentSegment->getFirstRecordOffset(); // record batch offset
    if (lastIdxEntry.baseOffset != 0) {
        seekPosition = lastIdxEntry.bytePosition; // seek position will be the byte position of the last index entry.
        nextRecordOffset = lastIdxEntry.baseOffset;
    }
    uint64_t currentSegmentSize = currentSegment->getLogFileSize();
    std::string activeLogPath = segmentDir / LogSegment::getFileName(currentSegment->getFirstRecordOffset(), "log");
    int fd = open(activeLogPath.c_str(), O_RDONLY);
    if (fd >= 0) {
        auto lastValidPosition = seekPosition; // this is the byte position of the last valid record batch
        while (seekPosition < currentSegmentSize) {
            uint32_t batch_len = 0;
            // read the current batch len. pread read from given offset but doesnt move the file ptr.
            if (pread(fd, &batch_len, sizeof(batch_len), seekPosition + 9) != sizeof(batch_len)) {
                break;
            }
            size_t total_batch_size = 13 + batch_len;
            std::vector<uint8_t> batch_buf(total_batch_size);
            if (pread(fd, batch_buf.data(), total_batch_size, seekPosition) != static_cast<ssize_t>(total_batch_size)) {
                break; // TODO: last RecordBatch was written incompletely (need to handle this)
            }
            try {
                RecordBatch batch = deserializeRecordBatch(batch_buf);
                nextRecordOffset = batch.baseOffset + batch.numRecords;
                seekPosition += total_batch_size;
                lastValidPosition = seekPosition;
            } catch (...) {
                break;
            }
        }
        close(fd);
        if (lastValidPosition < currentSegmentSize) {
            std::cout << "[Partition]: Remove corrupted bytes from " << activeLogPath
                      << " at pos: " << lastValidPosition << "\n";
            int write_fd = open(activeLogPath.c_str(), O_RDWR);
            if (write_fd >= 0) {
                // this syscall truncates the file to the specified size
                ftruncate(write_fd, lastValidPosition);
                close(write_fd);
            }
        }
    } else {
        std::cout << "[Partition]: No active log file found or Error while opening " << activeLogPath << ".\n";
    }
}

void Partition::createNewSegment(uint64_t newBaseOffset) {
    auto new_seg = std::make_unique<LogSegment>(newBaseOffset, 10, segmentDir.string());
    currentSegment = new_seg.get();
    segments.push_back(std::move(new_seg));
}

void Partition::checkNewSegment(uint64_t recordBatchSize) {
    if (currentSegment == nullptr || currentSegment->getLogFileSize() + recordBatchSize > SEGMENT_SIZE_LIMIT) {
        createNewSegment(nextRecordOffset);
    }
}

uint64_t Partition::getNextOffset() const { return nextRecordOffset; }

void Partition::append(const RecordBatch &recordBatch) {
    // std::cout << "[Partition]: Appending batch of " << recordBatch.numRecords << " records starting at offset " <<
    // nextRecordOffset << "\n";
    RecordBatch recordBatchCopy = recordBatch;
    std::vector<uint8_t> serializedRecordBatch = serializeRecordBatch(recordBatchCopy);
    checkNewSegment(serializedRecordBatch.size());
    recordBatchCopy.baseOffset = nextRecordOffset;
    nextRecordOffset += recordBatchCopy.numRecords;
    currentSegment->appendToLog(recordBatchCopy);
}
// This function well read a single recordbatch at the given offset
bool Partition::read(uint64_t offset, RecordBatch &recordBatch) {
    if (segments.empty() || offset >= nextRecordOffset) {
        return false;
    }
    auto it = std::upper_bound(
        segments.begin(), segments.end(), offset,
        [](uint64_t offset, const std::unique_ptr<LogSegment> &seg) { return offset < seg->getFirstRecordOffset(); });
    if (it == segments.begin()) {
        return false;
    }
    --it;
    // std::cout << "[partition]: Reading from segment starting at offset " << (*it)->getFirstRecordOffset() << "\n";
    return (*it)->readFromLog(offset, recordBatch);
}
} // namespace pubsub::storage