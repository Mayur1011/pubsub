#pragma once
#include <cstddef>
#include <cstdint>
#include <string>

namespace pubsub::storage {
#pragma pack(push, 1)
struct IndexEntry {
    uint64_t baseOffset;
    uint32_t bytePosition; // position of byte in .log file
};
#pragma pack(pop)
class IndexFile {
    int FD;
    uint8_t *mmapBasePtr; // pointer to mmap for .index file
    size_t mmapSize;      // size of mmap for .index file
    size_t fileSize;      // size of .index file in bytes

  public:
    const size_t ALLOCATION_CHUNK = 1024 * 1024;
    IndexFile(const std::string &filePath);
    ~IndexFile();
    void append(uint64_t baseOffset, uint64_t bytePosition);
    int64_t lookup(uint64_t offset);
    size_t getNumEntries();
    bool getLastEntry(IndexEntry *indexEntry);
    size_t getIdxFileSize();
};
} // namespace pubsub::storage