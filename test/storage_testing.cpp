#include "storage/partition.hpp"
#include "storage/record.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace pubsub::storage;
namespace fs = std::filesystem;

RecordBatch createTestBatch(uint32_t numRecords, const std::string &prefix) {
    RecordBatch batch;
    batch.batchStart = 0xAA;
    batch.baseOffset = 0;
    batch.batchLen = 0;
    batch.timeStamp = 1719324000;
    batch.numRecords = numRecords;

    for (uint32_t i = 0; i < numRecords; ++i) {
        Record rec;
        rec.recordOffsetDelta = i;
        rec.key = "key_" + prefix + "_" + std::to_string(i);
        rec.value = "value_" + prefix + "_" + std::to_string(i);
        batch.records.push_back(rec);
    }
    return batch;
}

int main() {
    std::string sandboxDir = "./data_storage";

    if (fs::exists(sandboxDir)) {
        fs::remove_all(sandboxDir);
    }

    std::cout << "====================================================\n";
    std::cout << "🚀 STARTING PHASE 1 TESTS WITH REAL RECORD.CPP 🚀\n";
    std::cout << "====================================================\n\n";

    {
        std::cout << "[STEP 1] Instantiating Partition Object...\n";
        Partition partition(sandboxDir);

        std::cout << "[STEP 2] Committing First Record Batch to Disk (Uses real serialization)...\n";
        RecordBatch batchA = createTestBatch(3, "A");
        partition.append(batchA);

        std::cout << "[STEP 3] Committing Second Record Batch...\n";
        RecordBatch batchB = createTestBatch(2, "B");
        partition.append(batchB);

        std::cout << "[STEP 4] Executing Direct Point-Lookups...\n";
        Record targetRecord;

        bool success1 = partition.read(1, targetRecord);
        assert(success1 == true);
        std::cout << "✔ Read Offset 1 - Key: " << targetRecord.key << ", Value: " << targetRecord.value << "\n";
        assert(targetRecord.value == "value_A_1");

        bool success2 = partition.read(4, targetRecord);
        assert(success2 == true);
        std::cout << "✔ Read Offset 4 - Key: " << targetRecord.key << ", Value: " << targetRecord.value << "\n";
        assert(targetRecord.value == "value_B_1");
    }

    std::cout << "----------------------------------------------------\n";
    std::cout << "⚡ RUNNING COLD RESTORE & RECOVERY CHECKS ⚡\n";
    std::cout << "----------------------------------------------------\n\n";

    {
        std::cout << "[STEP 5] Re-instantiating Partition (Uses real deserialization)...\n";
        Partition recoveredPartition(sandboxDir);

        std::cout << "[STEP 6] Verifying Historical Data Continuity post-boot...\n";
        Record recoveredRecord;

        bool successOld = recoveredPartition.read(3, recoveredRecord);
        assert(successOld == true);
        std::cout << "✔ Read Recovered Offset 3 - Key: " << recoveredRecord.key << ", Value: " << recoveredRecord.value
                  << "\n";
        assert(recoveredRecord.value == "value_B_0");
    }

    std::cout << "\n====================================================\n";
    std::cout << "🎉 ALL SANITY CHECKS PASSED USING YOUR REAL IMPLEMENTATION! 🎉\n";
    std::cout << "====================================================\n";

    return 0;
}