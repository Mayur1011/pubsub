#include <iostream>
#include <cassert>
#include <vector>
#include <string>

#include "storage/record.hpp"

using namespace pubsub::storage;

void testSerialization() {
    std::cout << "--- Starting Serialization Test ---\n";

    // 1. Create a dummy batch
    RecordBatch originalBatch;
    originalBatch.batchStart = 0xAB;
    originalBatch.baseOffset = 1000;
    originalBatch.timeStamp = 1686880000000; // Arbitrary epoch time
    originalBatch.numRecords = 2;

    // Add first record
    Record r1;
    r1.recordOffsetDelta = 0;
    r1.key = "user-123";
    r1.value = "{\"event\": \"login\"}";
    originalBatch.records.push_back(r1);

    // Add second record (test with empty key)
    Record r2;
    r2.recordOffsetDelta = 1;
    r2.key = "";
    r2.value = "{\"event\": \"logout\"}";
    originalBatch.records.push_back(r2);

    // 2. Serialize
    std::vector<uint8_t> buffer = serializeRecordBatch(originalBatch);
    std::cout << "Serialized buffer size: " << buffer.size() << " bytes\n";

    // 3. Deserialize
    RecordBatch deserializedBatch = deserializeRecordBatch(buffer);

    // 4. Assertions (If any of these fail, the program will crash here and point out the line)

    // Check Header
    assert(deserializedBatch.batchStart == originalBatch.batchStart);
    assert(deserializedBatch.baseOffset == originalBatch.baseOffset);
    assert(deserializedBatch.timeStamp == originalBatch.timeStamp);
    assert(deserializedBatch.numRecords == originalBatch.numRecords);

    // Check Batch Length Logic (Buffer size minus 13 bytes for magic + baseOffset + batchLen)
    uint32_t expectedBatchLen = buffer.size() - 13;
    assert(deserializedBatch.batchLen == expectedBatchLen);

    // Check Records
    assert(deserializedBatch.records.size() == 2);

    // Record 1
    assert(deserializedBatch.records[0].recordOffsetDelta == 0);
    assert(deserializedBatch.records[0].key == "user-123");
    assert(deserializedBatch.records[0].value == "{\"event\": \"login\"}");

    // Record 2
    assert(deserializedBatch.records[1].recordOffsetDelta == 1);
    assert(deserializedBatch.records[1].key == "");
    assert(deserializedBatch.records[1].value == "{\"event\": \"logout\"}");

    std::cout << "--- Test Passed: Serialization & Deserialization are 100% accurate! ---\n";
}

int main() {
    try {
        testSerialization();
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << '\n';
        return 1;
    }
    return 0;
}