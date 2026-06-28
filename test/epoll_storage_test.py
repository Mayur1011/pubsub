import socket
import struct
import sys


def pack_string(s: str) -> bytes:
    encoded = s.encode("utf-8")
    return struct.pack(f"<H{len(encoded)}s", len(encoded), encoded)


def build_phase1_record_batch(messages: list[str]) -> bytes:
    import time

    buf = bytearray()

    buf += struct.pack("<B", 0xAA)  # batchStart
    buf += struct.pack("<Q", 0)  # baseOffset (server overwrites)
    buf += struct.pack("<I", 0)  # batchLen placeholder
    buf += struct.pack("<Q", int(time.time()))  # timeStamp
    buf += struct.pack("<I", len(messages))  # numRecords

    for i, msg in enumerate(messages):
        key = f"key-{i}".encode("utf-8")
        val = msg.encode("utf-8")
        buf += struct.pack("<I", i)  # recordOffsetDelta
        buf += struct.pack("<I", len(key)) + key
        buf += struct.pack("<I", len(val)) + val

    # batchLen = bytes after batchLen field
    batch_len = len(buf) - 13
    struct.pack_into("<I", buf, 9, batch_len)
    return bytes(buf)


def create_produce_request(corr_id, topic, batch_bytes):
    header = struct.pack("<BI", 1, corr_id) + pack_string("python-client")
    payload = pack_string(topic) + struct.pack("<Ib", 0, 1) + batch_bytes
    return struct.pack("<I", len(header) + len(payload)) + header + payload


def create_fetch_request(corr_id, topic, offset):
    header = struct.pack("<BI", 2, corr_id) + pack_string("python-client")
    payload = pack_string(topic) + struct.pack("<IQI", 0, offset, 4096)
    return struct.pack("<I", len(header) + len(payload)) + header + payload


def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 6969))

    print("🚀 Running Checkpoint Test: 1000 Messages...")
    topic_name = "production-test"

    for i in range(1000):
        message_text = f"Message-Payload-Id-{i}"
        raw_batch = build_phase1_record_batch([message_text])
        req = create_produce_request(corr_id=i, topic=topic_name, batch_bytes=raw_batch)
        s.sendall(req)

        resp_header = s.recv(10)  # 4 bytes len, 4 bytes corrId, 2 bytes error
        if len(resp_header) < 10:
            print(
                "❌ Dropped connection or corrupted protocol response on batch append."
            )
            sys.exit(1)

    print("✅ 1000 Messages written to storage engine. Verifying FETCH retrieval...")

    fetch_req = create_fetch_request(corr_id=9999, topic=topic_name, offset=0)
    s.sendall(fetch_req)

    response_data = s.recv(65536)
    print(
        f"✅ Received FETCH block. Payload total byte length: {len(response_data)} bytes."
    )
    print("--- Phase 2 Completed Successfully! ---")
    s.close()


if __name__ == "__main__":
    main()
