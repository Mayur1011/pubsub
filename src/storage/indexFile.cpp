#include "storage/indexFile.hpp"
#include <algorithm>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace pubsub::storage {
IndexFile::IndexFile(const std::string &indexFilePath) {
    FD = -1;
    mmapBasePtr = nullptr;
    mmapSize = 0;
    fileSize = 0;

    FD = open(indexFilePath.c_str(), O_RDWR | O_CREAT, 0644);
    if (FD == -1) {
        throw std::runtime_error("[indexFile.cpp]: Failed to open index file");
    }
    struct stat fileInfo;
    if (fstat(FD, &fileInfo) == -1) {
        close(FD);
        throw std::runtime_error("[indexFile.cpp]: Failed to get file status");
    }
    fileSize = fileInfo.st_size;
    mmapSize = (fileSize > 0) ? fileSize : ALLOCATION_CHUNK;
    if (fileInfo.st_size == 0) {
        if (ftruncate(FD, mmapSize) < 0) {
            close(FD);
            throw std::runtime_error("[indexFile.cpp]: Failed to truncate index file");
        }
    }
    mmapBasePtr = (uint8_t *)mmap(nullptr, mmapSize, PROT_READ | PROT_WRITE, MAP_SHARED, FD, 0);
    if (mmapBasePtr == MAP_FAILED) {
        close(FD);
        throw std::runtime_error("[indexFile.cpp]: Failed to mmap index file");
    }
}
IndexFile::~IndexFile() {
    if (mmapBasePtr != MAP_FAILED and mmapBasePtr != nullptr) {
        msync(mmapBasePtr, fileSize, MS_SYNC);
        munmap(mmapBasePtr, mmapSize);
    }

    if (FD != -1) {
        ftruncate(FD, fileSize);
        close(FD);
    }
}
void IndexFile::append(uint64_t baseOffset, uint64_t bytePosition) {
    if (fileSize + sizeof(IndexEntry) > mmapSize) {
        size_t oldSize = mmapSize;
        mmapSize += ALLOCATION_CHUNK;
        if (::ftruncate(FD, mmapSize) < 0) {
            throw std::runtime_error("[indexFile.cpp]: Failed to extend index file capacity");
        }
        void *new_base = ::mremap(mmapBasePtr, oldSize, mmapSize, MREMAP_MAYMOVE);
        if (new_base == MAP_FAILED) {
            throw std::runtime_error("[indexFile.cpp]: Failed to mremap index file");
        }
        mmapBasePtr = static_cast<uint8_t *>(new_base);
    }
    IndexEntry *entries = reinterpret_cast<IndexEntry *>(mmapBasePtr + fileSize);
    entries->baseOffset = baseOffset;
    entries->bytePosition = bytePosition;
    fileSize += sizeof(IndexEntry);
}
int64_t IndexFile::lookup(uint64_t offset) {
    if (fileSize == 0)
        return -1;
    const IndexEntry *entries = reinterpret_cast<const IndexEntry *>(mmapBasePtr);
    size_t numEntries = fileSize / sizeof(IndexEntry);
    auto it = std::upper_bound(entries, entries + numEntries, offset,
                               [](uint64_t target, const IndexEntry &entry) { return target < entry.baseOffset; });
    if (it == entries) {
        return -1;
    }
    --it;
    return static_cast<int64_t>(it->bytePosition);
}
size_t IndexFile::getNumEntries() { return fileSize / sizeof(IndexEntry); }
} // namespace pubsub::storage