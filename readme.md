# ForgeMQ

A high-performance, Kafka-inspired message broker built in modern C++.

ForgeMQ is an event-driven broker that implements a custom binary TCP protocol, Linux `epoll`-based networking, durable append-only log storage, and consumer-group coordination semantics (join/leave/heartbeat/rebalance/generation fencing).

---

## Why this project

This project was built to deeply understand distributed systems internals by implementing broker fundamentals from scratch:

- low-level socket I/O and connection multiplexing
- binary protocol design and framing
- thread-safe request routing and worker execution
- persistent log/index storage layout and recovery
- consumer-group lifecycle and offset management

---

## Key capabilities

### 1) Event-driven networking (`epoll`)

- Non-blocking TCP server using Linux `epoll`
- Supports many concurrent producer/consumer connections
- Frame-oriented request parsing via per-connection buffering

### 2) Custom binary wire protocol

- Compact request/response schema over raw TCP (no HTTP/JSON overhead)
- Length-prefixed frames with correlation IDs for request tracking
- Typed operations for produce/fetch/offset/group-management/topic-creation

### 3) Concurrent worker architecture

- Deterministic routing of requests to worker threads by partition key
- Partition affinity minimizes cross-thread contention
- Asynchronous response callback path back to network layer

### 4) Durable partitioned log storage

- Append-only log segments per topic-partition
- Sparse index files for fast offset lookup
- Segment rotation and startup recovery paths

### 5) Consumer group coordination

- Join/leave/heartbeat flows for membership tracking
- Rebalance window + partition assignment to active members
- Generation-based validation to reject stale (“zombie”) consumers (`REJOIN`)
- Offset commit/fetch via internal offset state

---

## High-level architecture

```text
Client TCP Connections
        │
        ▼
 [epoll Network Loop]
  - read frames
  - deserialize protocol
  - enqueue disk/group tasks
        │
        ▼
 [Worker Pool]
  - partition-routed execution
  - produce/fetch/offset/group ops
        │
        ▼
 [Storage Engine]
  topics/<topic>/partition_<id>/
   - *.log (append-only)
   - *.index (sparse index)
```

---

## Repository layout

```text
app/                 # server entrypoint
include/             # public headers (net, storage, concurrency)
src/                 # implementation
  net/               # protocol + connection buffering
  storage/           # partition, segment, index, record logic
  concurrency/       # worker pool + group coordinator
test/                # e2e and stress tests
```

---

## Build and run

### Requirements

- Linux (uses `epoll`)
- `g++` with C++20 support
- `make`

### Build server

```bash
make
```

### Run server

```bash
make run-server
```

> Default broker listener is currently configured in `app/server.cpp` (port `6969`).

---

## Run tests

The project supports single-test execution through:

```bash
make run-test TEST=<test_name>
```

Examples:

```bash
make run-test TEST=consumer_groups
make run-test TEST=epollTest
make run-test TEST=stress_test
```

---

## Stress testing

A producer stress test can be used to benchmark throughput and latency under concurrency.

Typical outputs include:

- attempted/succeeded/failed request counts
- connection and I/O failure counts
- throughput (messages/sec)
- latency percentiles (p50/p95/p99)
- broker error-code histogram

For meaningful comparisons, run a matrix across:

- thread count (e.g., 10 / 20 / 50)
- total messages
- partition count (e.g., 2 / 8 / 16)