#pragma once
#include <cstdint>
#include <cstring>
#include <vector>

namespace pubsub::net {

class ConnectionBuffer {
    std::vector<uint8_t> buff;

  public:
    void append(const uint8_t *data, size_t len) { buff.insert(buff.end(), data, data + len); }
    bool read_frame(std::vector<uint8_t> &outFrame) {
        if (buff.size() < 4)
            return false;
        uint32_t frameSize = 0;
        std::memcpy(&frameSize, buff.data(), sizeof(frameSize));
        size_t totalFrameSize = 4 + frameSize;
        if (buff.size() < totalFrameSize) {
            return false; // full frame not yet received
        }
        outFrame.assign(buff.begin(), buff.begin() + totalFrameSize);
        consume(totalFrameSize);
        return true;
    }
    void consume(size_t bytes) { buff.erase(buff.begin(), buff.begin() + bytes); }
};

} // namespace pubsub::net