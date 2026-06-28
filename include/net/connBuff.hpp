#pragma once
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <vector>

namespace pubsub::net {

class ConnectionBuffer {
    std::vector<uint8_t> buf_;

  public:
    void append(const uint8_t *data, size_t len) { buf_.insert(buf_.end(), data, data + len); }

    std::optional<std::span<uint8_t>> try_read_frame() {
        if (buf_.size() < 4) {
            return std::nullopt; // Don't even have the FrameLength yet
        }

        uint32_t frameLen = 0;
        std::memcpy(&frameLen, buf_.data(), sizeof(uint32_t)); // here i am reading the frameLen, so whenever processing
                                                               // the frame, i need to skip the FrameLength field

        uint32_t totalFrameBytes = 4 + frameLen; // 4 bytes for length field itself

        if (buf_.size() < totalFrameBytes) {
            return std::nullopt; // Frame is still incomplete
        }

        return std::span<uint8_t>(buf_.data(), totalFrameBytes);
    }

    // Advance read cursor after processing by erasing the consumed bytes
    void consume(size_t bytes) { buf_.erase(buf_.begin(), buf_.begin() + bytes); }
};

} // namespace pubsub::net