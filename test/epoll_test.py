import socket
import struct


def pack_string(s: str) -> bytes:
    # 2-byte length prefix + string bytes
    encoded = s.encode("utf-8")
    return struct.pack(f"H{len(encoded)}s", len(encoded), encoded)


def create_produce_request(corr_id, client_id, topic, partition_id, batch_bytes):
    # Header: RequestType(1 byte), CorrelationId(4 bytes)
    header = struct.pack("<BI", 1, corr_id) + pack_string(client_id)
    # Payload: Topic, PartitionId(4 bytes), Acks(1 byte)
    payload = pack_string(topic) + struct.pack("<Ib", partition_id, 1) + batch_bytes

    # Wrap with FrameLength (total size of header + payload)
    frame_len = len(header) + len(payload)
    return struct.pack("<I", frame_len) + header + payload


def create_fetch_request(corr_id, client_id, topic, partition_id, offset, max_bytes):
    header = struct.pack("<BI", 2, corr_id) + pack_string(client_id)
    payload = pack_string(topic) + struct.pack("<IQI", partition_id, offset, max_bytes)

    frame_len = len(header) + len(payload)
    return struct.pack("<I", frame_len) + header + payload


def main():
    # Connect to blocking server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 6969))
    print("Connected to broker.")

    # 1. Send a PRODUCE request
    # Note: Using an empty byte array for the rawRecordBatch right now just to test networking.
    # We will inject the real Phase 1 binary layout here in Step 2.3.
    dummy_batch = b""
    req1 = create_produce_request(101, "python-producer", "test-topic", 0, dummy_batch)
    s.sendall(req1)

    # Read response
    resp1 = s.recv(1024)
    print(f"PRODUCE Response length: {len(resp1)} bytes")

    # 2. Send a FETCH request
    req2 = create_fetch_request(
        102, "python-consumer", "test-topic", 0, offset=0, max_bytes=4096
    )
    s.sendall(req2)

    # Read response
    resp2 = s.recv(1024)
    print(f"FETCH Response length: {len(resp2)} bytes")

    s.close()


if __name__ == "__main__":
    main()
