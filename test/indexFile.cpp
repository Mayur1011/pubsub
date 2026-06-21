#include "storage/indexFile.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <random>
#include <vector>

using namespace pubsub::storage;
namespace fs = std::filesystem;

void test_index_file() {
    std::string test_file = "test_sparse.index";
    if (fs::exists(test_file))
        fs::remove(test_file);

    // 1. Write 10,000 entries
    std::vector<IndexEntry> truth_data;
    {
        IndexFile idx(test_file);

        uint64_t current_base_offset = 0;
        uint32_t current_file_pos = 0;

        for (int i = 0; i < 10000; ++i) {
            idx.append(current_base_offset, current_file_pos);
            truth_data.push_back({current_base_offset, current_file_pos});

            // Simulate random batches of 1-10 records
            uint64_t batch_records = (rand() % 10) + 1;
            uint64_t batch_bytes = 100 + (rand() % 500); // simulate 100-600 byte batches

            current_base_offset += batch_records;
            current_file_pos += batch_bytes;
        }

        std::cout << "Successfully appended 10,000 index entries.\n";
        assert(idx.getNumEntries() == 10000);
    } // File goes out of scope here, triggering destruction and truncation cleanup

    // 2. Re-open and Read 1,000 Random Samples
    {
        IndexFile idx(test_file);
        assert(idx.getNumEntries() == 10000);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<size_t> dist(0, truth_data.size() - 1);

        for (int i = 0; i < 1000; ++i) {
            size_t random_index = dist(gen);
            IndexEntry expected = truth_data[random_index];

            // Ask the index for an exact base_offset match
            int64_t result_pos = idx.lookup(expected.baseOffset);
            assert(result_pos == static_cast<int64_t>(expected.bytePosition));

            // Ask the index for a target offset that falls *inside* this batch
            // (e.g., base_offset + 1, assuming there was more than 1 record)

            // Ask the index for a target offset that falls *inside* this batch
            // Only valid if next batch starts after baseOffset + 1
            if (random_index + 1 < truth_data.size() &&
                truth_data[random_index + 1].baseOffset > expected.baseOffset + 1) {
                int64_t result_pos_inside = idx.lookup(expected.baseOffset + 1);
                assert(result_pos_inside == static_cast<int64_t>(expected.bytePosition));
            }
        }

        // Test boundary case: target offset before the very first entry
        assert(idx.lookup(0) == 0); // Assuming truth_data[0].base_offset == 0

        std::cout << "Successfully validated 1,000 random sparse index lookups.\n";
    }

    fs::remove(test_file);
}

int main() {
    try {
        test_index_file();
    } catch (const std::exception &e) {
        std::cerr << "Fatal Error: " << e.what() << '\n';
        return 1;
    }
    return 0;
}