#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace pubsub::storage {

struct Record {
    uint32_t recordOffsetDelta; // this will be added with record batch offset
                                // to get the actual record offset
    std::string key;            // key of the record
    std::string value;          // value of the record
};

struct RecordBatch {
    uint8_t batchStart;  // identifies the start of the batch
    uint64_t baseOffset; // base offset of the batch (this is not byte position in the file it a temp ID for the batch)
    uint32_t batchLen;   // length of the batch
    uint64_t timeStamp;  // timestamp of the batch
    uint32_t numRecords; // number of records in the batch
    std::vector<Record> records; // records in the batch
};

// This will be store in .index file (Each partition will have a separate .index
// file)
struct SparseIndex {
    uint64_t baseOffset;   // base offset of the record batch
    uint64_t bytePosition; // byte offset of the record batch in the .log file
};

std::vector<uint8_t> serializeRecordBatch(const RecordBatch &batch);
RecordBatch deserializeRecordBatch(const std::vector<uint8_t> &data);

} // namespace pubsub::storage