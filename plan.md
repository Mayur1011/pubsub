# Distributed Pub-Sub Broker — Detailed Incremental Build Plan

---

## Directory Layout (Final State)

```
broker/
├── src/
│   ├── storage/        # Phase 1: log segments, index, partition log
│   ├── net/            # Phase 2: TCP server, epoll, binary protocol
│   ├── concurrency/    # Phase 3: thread pool, request queue, jthread
│   ├── broker/         # Phase 4: topic manager, retention
│   ├── consumer/       # Phase 5: group coordinator, offset manager
│   ├── replication/    # Phase 6: ISR, follower-pull
│   └── raft/           # Phase 7: Raft state machine
├── tests/
├── bench/
└── CMakeLists.txt
```

---

# PHASE 1 — Local Durable Storage Engine

**Goal:** A library that can append RecordBatches to a binary log file and retrieve
any message by its logical offset in sub-millisecond time. No networking, no threading.
Just a rock-solid storage layer you can benchmark and unit-test in isolation.

---

### Step 1.1 — Define All On-Disk Binary Formats First

Do this before writing a single line of C++. Draw the byte layout on paper.
Every decision you lock in here ripples through the entire project.

**RecordBatch (the unit written to disk):**

```
[Magic          (1 byte )] = 0xAB, identifies a valid batch start
[BaseOffset     (8 bytes)] = logical offset of the first record in this batch
[BatchLength    (4 bytes)] = total byte size of everything after this field
[CRC32          (4 bytes)] = CRC over [Timestamp .. end of Records]
[Timestamp      (8 bytes)] = epoch ms of batch creation
[NumRecords     (4 bytes)] = number of records in this batch
[Records        (N bytes)] = variable length, see Record format below
```

**Record (inside a batch, delta-encoded):**

```
[OffsetDelta    (4 bytes)] = this record's offset = BaseOffset + OffsetDelta
[KeyLength      (4 bytes)] = 0xFFFFFFFF means null key
[Key            (N bytes)] = raw bytes
[ValueLength    (4 bytes)]
[Value          (N bytes)] = raw bytes
```

Why delta-encode? When a producer sends 500 records in one batch, storing full
8-byte offsets per record wastes space. The delta from BaseOffset fits in 4 bytes
for any reasonable batch size. The full logical offset of record i is
`BaseOffset + OffsetDelta[i]`.

**Why CRC covers everything after BatchLength but not BaseOffset/BatchLength:**
BaseOffset and BatchLength are written by the broker, not the producer. The CRC
covers the producer-generated content so you can detect corruption independently
of broker-assigned metadata.

**Sparse Index Entry (fixed-width, 16 bytes):**

```
[BaseOffset     (8 bytes)] = the BaseOffset of a RecordBatch
[FilePosition   (8 bytes)] = byte offset of that batch in the .log file
```

Entries are written in strictly ascending offset order, so the file is always
sorted — enabling `std::lower_bound` style binary search.

**Naming convention on disk:**

```
00000000000000000000.log    # segment whose first batch has BaseOffset=0
00000000000000000000.index  # sparse index for that segment
00000000000001048576.log    # next segment, first batch BaseOffset=1048576
00000000000001048576.index
```

The segment filename is the BaseOffset of its first batch, zero-padded to 20
digits. This lets you sort segment files lexicographically to reconstruct the
full log ordering.

---

### Step 1.2 — Implement `LogSegmImplement `LogSegmentent`

This is the class that manages one `.log` / `.index` file pair.

**Members:**

```cpp
class LogSegment {
    int log_fd_;           // open file descriptor for .log
    int index_fd_;         // open file descriptor for .index
    uint64_t base_offset_; // first batch offset in this segment
    uint64_t log_size_;    // current byte size of .log (for append position)
    uint64_t index_size_;  // current number of index entries * 16
    uint64_t bytes_since_last_index_entry_; // sparse index threshold tracker
};
```

**`append(RecordBatch& batch)` — the critical write path:**

1. Encode the batch into a `std::vector<uint8_t>` using your format from Step 1.1.
2. Compute CRC32 over bytes [Timestamp .. end of Records]. Write it into offset 13
   of the encoded buffer. Use `crc32` from `<zlib.h>` or implement the standard
   polynomial yourself (interview defensibility: know the algorithm).
3. `pwrite(log_fd_, buffer.data(), buffer.size(), log_size_)` — write at current
   end of file. Never use `lseek` + `write`; `pwrite` is atomic for the offset.
4. If `bytes_since_last_index_entry_ >= INDEX_INTERVAL` (e.g., 4096):
   - Build a 16-byte index entry: `[batch.base_offset][log_size_]`.
   - `pwrite(index_fd_, entry, 16, index_size_)`.
   - `index_size_ += 16`.
   - `bytes_since_last_index_entry_ = 0`.
5. `log_size_ += buffer.size()`.
6. `bytes_since_last_index_entry_ += buffer.size()`.
7. Do **not** `fsync` on every write by default. Add a configurable `flush_interval`
   — accumulate N batches, then `fdatasync(log_fd_)`. fdatasync skips updating
   the file metadata (atime, mtime) and is faster than fsync.

**`read(uint64_t target_offset)` — the lookup path:**

1. Binary search the index file to find the largest `BaseOffset <= target_offset`.
   Load the index file into memory as a `uint8_t*` pointer (via `mmap` — see
   Step 1.5). Cast to `IndexEntry*` array. Use `std::upper_bound` with a custom
   comparator on the offset field, then step back one entry.
2. Seek to the `FilePosition` from that index entry.
3. Scan the log file forward from that position, decoding RecordBatch headers.
   For each batch: if `BaseOffset + NumRecords > target_offset`, this batch
   contains the record. If `BaseOffset > target_offset`, something is wrong
   (corruption or a bug in your index). Return the error.
4. Within the matching batch, find the record with `OffsetDelta == target_offset
   - BaseOffset`. Verify CRC32 before returning. Return error on mismatch.

---

### Step 1.3 — Implement the Sparse Index Correctly

The sparse index is the core performance structure. Get it exactly right.

Write a standalone `IndexFile` class:

```cpp
class IndexFile {
    uint8_t* mmap_base_;  // mmap pointer to index file
    size_t   mmap_size_;  // current mmap size
    int      fd_;

public:
    // Append a new index entry (only called from LogSegment::append)
    void append(uint64_t base_offset, uint64_t file_position);

    // Returns the file_position of the largest entry whose base_offset <= target
    // Returns -1 if no such entry exists (target is before all entries)
    int64_t lookup(uint64_t target_offset) const;
};
```

`lookup` implementation detail: the mmap gives you a pointer to raw bytes. Cast
it to an array of 16-byte structs. The array is sorted by `base_offset` (you
maintain this invariant on write). Use `std::upper_bound` to find the first entry
with `base_offset > target_offset`, then step back one. Return its `file_position`.

For `append`: index files grow over time. Use `ftruncate` to pre-extend the file
by 1MB when you're about to run out of space. `mremap` on Linux can extend an
existing mmap without unmapping. This avoids the overhead of repeated `mmap` /
`munmap` cycles.

Write a unit test that: appends 10,000 batches (1-10 records each), then randomly
reads back 1,000 of them by offset and verifies the payload is correct and the CRC
passes. This test must pass before you touch Phase 2.

---

### Step 1.4 — Implement `PartitionLog`

This manages a list of LogSegments for a single partition. It's the interface the
rest of the broker talks to — `LogSegment` is an implementation detail.

```cpp
class PartitionLog {
    std::vector<std::unique_ptr<LogSegment>> segments_;
    LogSegment* active_segment_;  // the one being written to
    uint64_t next_offset_;        // monotonically increasing counter
    std::filesystem::path base_dir_;
};
```

**`append(RecordBatch& batch)`:**
1. Assign `batch.base_offset = next_offset_`.
2. `next_offset_ += batch.num_records`.
3. Delegate to `active_segment_->append(batch)`.
4. If `active_segment_->log_size() >= SEGMENT_SIZE_LIMIT` (100MB):
   - `fdatasync` and close the active segment.
   - Create a new `LogSegment` with filename `next_offset_` zero-padded to 20 digits.
   - `active_segment_ = new segment`.

**`read(uint64_t target_offset)`:**
1. Binary search `segments_` by `base_offset` to find which segment contains
   `target_offset`. This outer binary search is O(log S) where S is number of
   segments (typically small — a 1TB log at 100MB segments is only 10,240 segments).
2. Delegate to that segment's `read(target_offset)`.

**On startup / recovery:**
1. List all `.log` files in `base_dir_`, sort by filename (= base offset).
2. Re-open each as a `LogSegment`. For all but the last, open read-only. For the
   last (active), open read-write.
3. Scan the active segment from `index.last_entry.file_position` forward to the
   actual end of file, recovering any batches written after the last index entry.
   This is your crash recovery path — the index is sparse so you may need to
   replay up to `INDEX_INTERVAL` bytes of log to reconstruct `next_offset_`.
4. Verify the last batch's CRC. If it fails, truncate the log to the last valid
   batch boundary. This handles the case where a crash interrupted a write
   mid-batch.

---

### Step 1.5 — Add mmap for Reads

Open each **inactive** (sealed) segment's `.log` file with `mmap(MAP_SHARED |
MAP_POPULATE)`. `MAP_POPULATE` triggers readahead — the kernel pre-faults pages
into the page cache. For sealed segments, reads are now pure memory accesses with
zero syscall overhead.

Do not mmap the active segment's log file. It's being written to with `pwrite`,
and extending a file while it's mmap'd requires careful `ftruncate` + `mremap`
coordination. The complexity isn't worth it for the write path.

Add `madvise(MADV_SEQUENTIAL)` on the mmap'd region for index files during
startup scan. Add `madvise(MADV_RANDOM)` for random-read workloads.

---

### Step 1.6 — Benchmark Phase 1 in Isolation

Before moving to Phase 2, run these benchmarks and record the numbers. You will
cite these in interviews and on your resume.

- **Sequential write throughput:** Append 10 million single-record batches.
  Measure MB/s. Expected: limited by `fdatasync` frequency. With `fdatasync`
  every 1000 batches you should see 500+ MB/s on NVMe.
- **Random read latency:** After writing 10M records, read 100,000 random offsets.
  Measure p50/p99/p999 latency. Expected: p50 < 50µs (pure page cache hit after
  mmap warmup).
- **Index lookup time:** Time just the `IndexFile::lookup` call on a 1M-entry
  index. Expected: < 1µs (binary search on mmap'd memory).

Use `std::chrono::high_resolution_clock` for timing. Write a `bench/storage_bench.cpp`.

---

# PHASE 2 — Single-Process TCP Server

**Goal:** Wrap the Phase 1 storage library behind a TCP server. A producer can
connect and send a PRODUCE request; a consumer can send a FETCH request. One
client at a time for now — concurrency comes in Phase 3.

---

### Step 2.1 — Design the Binary Wire Protocol

Define the request and response frame formats precisely. Write them in a header
file (`net/protocol.h`) before implementing the server.

**Request Frame:**

```
[FrameLength    (4 bytes)] = total bytes in this frame, not including FrameLength itself
[ApiKey         (1 byte )] = 0x01 PRODUCE, 0x02 FETCH, 0x03 METADATA, 0x04 HEARTBEAT
[CorrelationId  (4 bytes)] = client-chosen ID echoed back in the response
[ClientId       (2 bytes)] = length-prefixed string follows (identifies the producer/consumer)
[ClientIdBytes  (N bytes)]
[Payload        (M bytes)] = API-specific, see below
```

**PRODUCE Payload:**

```
[TopicLength    (2 bytes)]
[Topic          (N bytes)]
[PartitionId    (4 bytes)]
[Acks           (1 byte )] = 0 (no ack), 1 (leader ack), -1 (ISR ack)
[BatchData      (M bytes)] = a RecordBatch in the Phase 1 binary format
```

**FETCH Payload:**

```
[TopicLength    (2 bytes)]
[Topic          (N bytes)]
[PartitionId    (4 bytes)]
[FetchOffset    (8 bytes)] = the logical offset to start reading from
[MaxBytes       (4 bytes)] = how many bytes of log data to return at most
```

**Response Frame:**

```
[FrameLength    (4 bytes)]
[CorrelationId  (4 bytes)] = echoed from request
[ErrorCode      (2 bytes)] = 0 = OK, non-zero = error (define an enum)
[Payload        (M bytes)] = API-specific
```

**FETCH Response Payload:**

```
[HighWatermark  (8 bytes)] = next_offset_ of this partition (consumer uses this
                              to know how far behind it is)
[RecordCount    (4 bytes)] = number of RecordBatches returned
[RecordData     (M bytes)] = raw bytes from the log file
```

Key insight: the RecordData in the FETCH response is the **raw bytes from the log
file**, not re-encoded. You'll serve this via `sendfile(2)` in Step 2.5, which
means the data goes from the page cache directly to the socket buffer — no copy
into your process's address space at all.

---

### Step 2.2 — Implement a Blocking Single-Client Server First

Before epoll, build a simple `accept()` → `recv()` → `process()` → `send()`
loop. This is throwaway code but it lets you validate the protocol framing and
the PRODUCE/FETCH handlers independently of epoll complexity.

Key detail in reading from a TCP socket: `recv()` does not guarantee you get a
full frame in one call. You must maintain a per-connection read buffer and
accumulate bytes until you have at least 4 bytes (the FrameLength), then
accumulate until you have `FrameLength` more bytes. Only then dispatch to a
handler. This partial-read handling is the most common bug in systems that
implement binary protocols.

```cpp
class ConnectionBuffer {
    std::vector<uint8_t> buf_;
    size_t write_pos_ = 0;

public:
    // Returns a complete frame if available, nullptr otherwise
    std::optional<std::span<uint8_t>> try_read_frame();
    void consume(size_t bytes);  // advance read cursor after processing
};
```

Write a simple test client in Python (`tests/test_client.py`) that produces 1000
messages and then fetches them back, verifying payload integrity. This validates
the full path: network → protocol parsing → storage write → storage read →
protocol encoding → network.

---

### Step 2.3 — Migrate to epoll Non-Blocking I/O

Replace the blocking server with an epoll event loop. This is the architectural
core of the networking layer.

**Socket setup:**

```cpp
// Create listening socket
int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);

// Set SO_REUSEPORT — allows multiple threads to bind to the same port,
// kernel distributes accepted connections between them (Phase 3 will use this)
int opt = 1;
setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

// Bind + listen
bind(server_fd, ...);
listen(server_fd, SOMAXCONN);  // SOMAXCONN = 4096+ on modern kernels
```

**epoll setup:**

```cpp
int epoll_fd = epoll_create1(EPOLL_CLOEXEC);

epoll_event ev;
ev.events = EPOLLIN;  // notify when data is ready to read
ev.data.fd = server_fd;
epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);
```

**Event loop:**

```cpp
while (running_) {
    epoll_event events[MAX_EVENTS];
    int n = epoll_wait(epoll_fd, events, MAX_EVENTS, timeout_ms);

    for (int i = 0; i < n; i++) {
        if (events[i].data.fd == server_fd) {
            // New connection
            int client_fd = accept4(server_fd, nullptr, nullptr, SOCK_NONBLOCK);
            // Set TCP_NODELAY on accepted socket
            int flag = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
            // Add client_fd to epoll
            epoll_event cev;
            cev.events = EPOLLIN | EPOLLET;  // edge-triggered
            cev.data.ptr = new ConnectionState(client_fd);  // heap-allocate state
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &cev);
        } else {
            auto* conn = static_cast<ConnectionState*>(events[i].data.ptr);
            handle_readable(conn);
        }
    }
}
```

**Why EPOLLET (edge-triggered)?** Level-triggered (default) fires every time the
fd is readable. Edge-triggered fires only when *new* data arrives. With ET, when
`handle_readable` is called, you must drain the socket completely until you get
`EAGAIN` — otherwise you'll miss data. But ET scales better with many connections
because epoll_wait doesn't return the same fd repeatedly when you're slow to drain.

**`handle_readable(ConnectionState* conn)`:**

```
loop:
  ret = recv(conn->fd, buf, chunk_size, 0)
  if ret > 0:
    conn->buffer.append(buf, ret)
    while conn->buffer.has_complete_frame():
      process_frame(conn, conn->buffer.next_frame())
      conn->buffer.consume_frame()
  elif ret == 0:
    close_connection(conn)
    return
  elif errno == EAGAIN or errno == EWOULDBLOCK:
    return  // no more data right now, wait for next epoll event
  else:
    close_connection(conn)
    return
```

---

### Step 2.4 — Set TCP_NODELAY and Understand Why

`TCP_NODELAY` disables Nagle's algorithm. Nagle buffers small outgoing packets
until either a full MSS (1460 bytes) is accumulated or a previous packet's ACK
is received. This is good for bulk throughput but terrible for latency-sensitive
protocols.

Without `TCP_NODELAY`: your PRODUCE response (a small ACK packet, maybe 20 bytes)
sits in the kernel's send buffer waiting for Nagle's algorithm to time out (up to
200ms). The producer is blocked waiting for the ACK. Your throughput collapses
to 5 messages/second at low concurrency.

With `TCP_NODELAY`: the ACK goes out immediately. Throughput at low concurrency
is bounded by round-trip time, not Nagle.

Set it on every accepted client socket. This is mandatory, not optional.

---

### Step 2.5 — Implement `sendfile(2)` for FETCH Responses

This is the zero-copy path. When a consumer sends a FETCH request, you need to
send bytes from a log segment file over the network. Without `sendfile`:

```
disk → kernel page cache → kernel→user copy → user buffer → user→kernel copy → socket buffer → NIC
```

With `sendfile(2)`:

```
disk → kernel page cache → socket buffer → NIC
```

Two kernel→user→kernel copies are eliminated. At high throughput this is the
difference between saturating a 10Gbps NIC and not.

Implementation:

```cpp
void send_fetch_response(int client_fd, int log_fd, off_t file_offset, size_t byte_count) {
    // First, send the response header (FrameLength, CorrelationId, ErrorCode,
    // HighWatermark, RecordCount) using a normal write() call
    write(client_fd, header_buf, header_size);

    // Then, zero-copy send the log data
    off_t offset = file_offset;
    ssize_t sent = sendfile(client_fd, log_fd, &offset, byte_count);
    // Handle partial sendfile (same EAGAIN logic as recv)
}
```

Caveat: `sendfile` only works for sealed (inactive) log segments, because the
file descriptor you pass must refer to a regular file. For the active segment
(still being written), fall back to `read()` + `write()`. Since most consumer
fetch requests are for older data that's in sealed segments, this caveat rarely
matters in practice.

---

### Step 2.6 — Implement the METADATA API

Producers and consumers need to discover which partitions exist for a topic and
what their current high-watermarks are. Without this, clients are hardcoded to
partition 0 — not a real broker.

METADATA Request: `[TopicLength][Topic]`

METADATA Response:

```
[NumPartitions  (4 bytes)]
per partition:
  [PartitionId  (4 bytes)]
  [HighWatermark(8 bytes)]
  [LeaderId     (4 bytes)] = always 0 in Phase 2, real in Phase 6
```

Clients call METADATA before any PRODUCE or FETCH to discover the cluster state.

---

# PHASE 3 — Concurrency and Thread Safety

**Goal:** Handle multiple concurrent clients. Multiple producers writing to
different partitions in parallel. The key design goal: **zero locking on the
hot write path** through partition-affinity routing.

---

### Step 3.1 — Thread-Safe Request Queue (MPSC)

Network threads receive requests and enqueue them. Worker threads dequeue and
process them. You need a multi-producer, single-consumer (MPSC) queue per worker.

Use `std::mutex` + `std::condition_variable` + `std::deque` first. This is
correct and straightforward. Don't jump to lock-free here — the queue is not
on the hot path (each request is enqueued once, dequeued once, and the actual
storage I/O dominates the time).

```cpp
template <typename T>
class BlockingQueue {
    std::deque<T> queue_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool closed_ = false;

public:
    void push(T item) {
        std::lock_guard lock(mu_);
        queue_.push_back(std::move(item));
        cv_.notify_one();
    }

    std::optional<T> pop() {  // blocks until item available or queue closed
        std::unique_lock lock(mu_);
        cv_.wait(lock, [&] { return !queue_.empty() || closed_; });
        if (queue_.empty()) return std::nullopt;
        T item = std::move(queue_.front());
        queue_.pop_front();
        return item;
    }

    void close() {
        std::lock_guard lock(mu_);
        closed_ = true;
        cv_.notify_all();
    }
};
```

---

### Step 3.2 — Partition-Affinity Worker Threads

This is the key design decision that eliminates locking on the write path.

**The idea:** Each partition is permanently assigned to exactly one worker thread.
All PRODUCE requests for partition N are routed to worker `N % num_workers`.
That worker is the *only* thread that ever calls `PartitionLog::append()` on
that partition. Since only one thread writes, you need no mutex protecting the
`PartitionLog`'s internal state.

```cpp
class WorkerThread {
    std::jthread thread_;                    // C++20: auto-joins on destruction
    BlockingQueue<Request> queue_;           // only this thread dequeues
    std::unordered_map<uint32_t, PartitionLog*> owned_partitions_;

public:
    void start() {
        thread_ = std::jthread([this](std::stop_token st) {
            while (!st.stop_requested()) {
                auto req = queue_.pop();
                if (!req) break;
                dispatch(*req);
            }
        });
    }

    void submit(Request req) { queue_.push(std::move(req)); }
};
```

**`std::jthread` and `std::stop_token` (C++20):** When the `WorkerThread` object
goes out of scope, `std::jthread`'s destructor calls `request_stop()` on the
internal stop source and then `join()`s the thread. The thread checks
`st.stop_requested()` in its loop and exits cleanly. This replaces the old pattern
of a `bool running_` + atomic flag + manual join.

**Routing in the network thread:**

```cpp
void route_produce_request(Request req) {
    uint32_t worker_id = req.partition_id % workers_.size();
    workers_[worker_id].submit(std::move(req));
}
```

---

### Step 3.3 — Multiple epoll Threads with SO_REUSEPORT

Create `N` network threads (where N = number of CPU cores or a configured value).
Each thread creates its own `epoll_fd` and calls `accept4()` on the same
`server_fd`. Because you set `SO_REUSEPORT` in Phase 2, the kernel load-balances
incoming connections across all threads' `accept4()` calls.

Each network thread independently reads requests and routes them to worker threads
via the affinity map. This means you have no shared state between network threads
— each connection is owned by exactly one network thread.

```
Core 0: net_thread_0 → epoll_fd_0, owns connections [C1, C5, C9 ...]
Core 1: net_thread_1 → epoll_fd_1, owns connections [C2, C6, C10 ...]
Core 2: worker_thread_0 → owns partitions [P0, P2, P4 ...]
Core 3: worker_thread_1 → owns partitions [P1, P3, P5 ...]
```

No connection state is shared. No partition state is shared. The only synchronization
is the `BlockingQueue` between net threads and worker threads.

---

### Step 3.4 — Response Path

Worker threads compute the response and need to send it back to the client. But
the client connection is owned by a net thread, not the worker thread. You can't
call `write()` from the worker thread on a socket owned by a net thread without
introducing synchronization.

Two clean approaches:

**Option A (simpler):** Give each `ConnectionState` a mutex-protected outbound
queue. Worker threads push responses into it. When the net thread's epoll loop
detects `EPOLLOUT` (socket writable), it drains the outbound queue and sends.
Add `EPOLLOUT` to the connection's epoll interest set when the outbound queue
becomes non-empty.

**Option B (more elegant):** Use a `pipe` or `eventfd` per connection. The worker
thread writes 8 bytes to the `eventfd` to signal "response ready." The net thread
has this `eventfd` registered in its epoll. When signaled, it reads the response
from a lock-free queue and sends it. This avoids `EPOLLOUT` polling.

Go with Option A first. Option B is a Phase 3+ optimization.

---

### Step 3.5 — Read Path Concurrency

`PartitionLog::read()` can be called from multiple threads simultaneously (multiple
consumers fetching from the same partition). The read path accesses:

- `segments_` vector: modified only during log rotation (new segment added). Protect
  with a `std::shared_mutex`. Readers take a shared lock; the worker thread that
  owns the partition takes an exclusive lock during log rotation only.
- `mmap` memory of sealed segments: safe to read concurrently — `mmap(MAP_SHARED)`
  with only reads is race-free.
- Active segment: reads of the active segment need a `std::shared_mutex` with the
  writer. Since only the owning worker thread writes, readers take shared and the
  worker takes exclusive during `append`.

In practice, 99% of consumer reads are from sealed segments (they're reading
historical data). The `shared_mutex` contention on the active segment is minimal.

---

### Step 3.6 — Stress Test Phase 3

Write a test that spawns 100 producer clients and 100 consumer clients. Producers
each send 10,000 messages as fast as possible. Consumers fetch from offset 0 and
validate every record. Run with thread sanitizer (`-fsanitize=thread`) to catch
any data races. Fix every race before Phase 4.

---

# PHASE 4 — Topics, Partitions, and Retention

**Goal:** A named topic with multiple partitions. Log rotation. Background retention
cleanup. This phase has no networking changes — it's pure broker infrastructure.

---

### Step 4.1 — Directory Structure

```
data/
└── topics/
    ├── orders/
    │   ├── partition_0/
    │   │   ├── 00000000000000000000.log
    │   │   ├── 00000000000000000000.index
    │   │   ├── 00000000000001048576.log
    │   │   └── 00000000000001048576.index
    │   └── partition_1/
    │       └── ...
    └── payments/
        └── partition_0/
            └── ...
```

Each partition is a `PartitionLog` (from Phase 1) in its own subdirectory. A
`TopicManager` class maps `(topic_name, partition_id)` to a `PartitionLog*`.

---

### Step 4.2 — TopicManager

```cpp
class TopicManager {
    std::filesystem::path base_dir_;
    // topic_name → (partition_id → PartitionLog)
    std::unordered_map<std::string,
        std::unordered_map<uint32_t, std::unique_ptr<PartitionLog>>> topics_;
    std::shared_mutex mu_;

public:
    // Creates the directory structure and a new PartitionLog
    void create_topic(const std::string& name, uint32_t num_partitions);

    // Returns nullptr if not found
    PartitionLog* get_partition(const std::string& topic, uint32_t partition_id);

    // Called at startup: scan base_dir_ and reconstruct all PartitionLogs
    void recover();
};
```

**`recover()`:** Iterate `base_dir_/topics/`, find all `topic/partition_N/`
directories, construct a `PartitionLog` for each. This is your broker's startup
procedure — zero hardcoded state, everything inferred from the filesystem.

Update PRODUCE and FETCH handlers to use `TopicManager` instead of a single
hardcoded `PartitionLog`.

---

### Step 4.3 — Log Rotation

Log rotation is already half-implemented in `PartitionLog::append()` from Phase 1
(checking `active_segment_->log_size() >= SEGMENT_SIZE_LIMIT`). What you need to
add here:

**Timestamped rotation:** A segment can be rotated either because it hit 100MB
*or* because it's older than a configured `segment.max.age` (e.g., 7 days). The
background retention thread (Step 4.4) handles the latter.

**Rotation atomicity:** When rotating, you need the segment rename to be atomic
so a concurrent reader doesn't see a half-formed segment. Since you're not
renaming (you're just creating a new segment file with a new name), there's no
atomicity issue — the old segment is closed but its file persists and is still
readable.

**Notifying the worker thread:** Log rotation must only happen on the worker thread
that owns the partition (partition-affinity from Phase 3). Add a `maybe_rotate()`
call at the start of `PartitionLog::append()`.

---

### Step 4.4 — Background Retention Thread

This thread wakes up every `retention.check.interval.ms` (e.g., 5 minutes) and
enforces the retention policy on all partitions.

```cpp
class RetentionManager {
    std::jthread thread_;
    TopicManager& topic_mgr_;
    RetentionConfig cfg_;  // max.bytes per partition, max.age.ms

public:
    void start() {
        thread_ = std::jthread([this](std::stop_token st) {
            while (!st.stop_requested()) {
                // Sleep, interruptible by stop
                std::this_thread::sleep_for(cfg_.check_interval);
                if (st.stop_requested()) break;
                enforce_retention();
            }
        });
    }

    void enforce_retention() {
        // For each (topic, partition):
        //   get list of sealed segments sorted by base_offset (ascending)
        //   accumulate total bytes from newest to oldest
        //   any segment that causes total_bytes > max_bytes → delete
        //   any segment whose last_write_time < now - max_age → delete
        //   never delete the active segment
    }
};
```

Deletion: `std::filesystem::remove(segment.log_path)` and
`std::filesystem::remove(segment.index_path)`. Before deleting, take the
`PartitionLog`'s `shared_mutex` exclusively so no reader has a reference to the
segment. After deletion, remove the segment from `PartitionLog::segments_`.

**An important invariant to maintain:** The retention thread must coordinate with
the `PartitionLog` so it doesn't delete a segment that a consumer is actively
reading. The simplest approach: `PartitionLog` exposes a `get_sealed_segments()`
that returns a list under the shared_mutex. The retention thread calls this,
computes which to delete, then for each deletion, re-acquires the exclusive lock
and removes it. If a consumer holds a reference to that segment (via shared_ptr),
deletion is deferred until the last reader drops its reference.

Change segment storage to `std::shared_ptr<LogSegment>`. Readers get a
`shared_ptr` copy. Retention can erase from the vector (dropping the vector's
reference) and the file will be deleted when the last reader's `shared_ptr`
goes out of scope via a custom deleter that calls `unlink`.

---

### Step 4.5 — Log Compaction (Optional but Elite)

Compaction is for topics where only the latest value per key matters. Instead of
deleting old segments by time, you merge old segments: scan them all, keep only
the last value for each key, write a new compacted segment. This is a background
operation on sealed segments.

This is genuinely hard to implement correctly (handling deletes via tombstone
records, ensuring the active segment is never compacted, coordinating with
concurrent readers). Mark this as optional for the core 7-phase plan — add it
after Phase 7 as a polish pass if you have time.

---

# PHASE 5 — Consumer Offsets and Consumer Groups

**Goal:** Consumers can commit their progress. Multiple consumers in a group share
partitions. The broker tracks which consumer owns which partition and detects failures.

---

### Step 5.1 — FETCH with Explicit Offset

Update the FETCH API from Phase 2 to require a `FetchOffset` parameter. The
consumer must track its own current offset and pass it on each request. The
broker returns up to `MaxBytes` of records starting at `FetchOffset`.

The consumer's offset management loop:

```
fetch_offset = 0
loop:
  response = broker.fetch(topic, partition, fetch_offset, max_bytes=1MB)
  for each record in response:
    process(record)
  fetch_offset = response.high_watermark  // or last processed offset + 1
  commit_offset(fetch_offset)             // Step 5.2
```

---

### Step 5.2 — `__consumer_offsets` Internal Topic

Create a special topic `__consumer_offsets` on broker startup (if it doesn't
exist). This topic has a fixed number of partitions (e.g., 50). The partition for
a given `(group_id, topic, partition)` is:

```cpp
uint32_t offset_partition = fnv1a_hash(group_id + ":" + topic + ":" 
                             + std::to_string(partition)) % 50;
```

**Committing an offset:** The OFFSET_COMMIT API handler takes
`(group_id, topic, partition, committed_offset)` and produces a record to
`__consumer_offsets` with:

```
key   = group_id + ":" + topic + ":" + partition  (string)
value = committed_offset (8 bytes, big-endian)
```

**Reading committed offsets at startup:** Since `__consumer_offsets` is a
compacted topic (only latest value per key matters), on startup you replay it
to reconstruct the committed offset for each `(group, topic, partition)` tuple.
You keep this in-memory as `std::unordered_map<OffsetKey, uint64_t>` for fast
OFFSET_FETCH responses.

Implement `__consumer_offsets` using the exact same `PartitionLog` code as any
other topic. There is no special case — that's the entire point of dogfooding.
The only difference: `__consumer_offsets` has compaction enabled (Phase 4.5 if
you implement it) and infinite retention.

---

### Step 5.3 — Heartbeat Protocol and Session Timeout

A `GROUP_COORDINATOR` is the broker node responsible for managing a consumer
group. In Phase 5 (single node), this is always the local broker.

**New API: HEARTBEAT**

Request: `[GroupId][MemberId][GenerationId]`
Response: `[ErrorCode]`

The consumer sends a HEARTBEAT every `heartbeat.interval.ms` (e.g., 3000ms).
The broker's group coordinator tracks the last heartbeat time per `(group_id,
member_id)`.

**Session timeout thread** (runs in the group coordinator):

```cpp
// Runs every heartbeat.interval.ms
void check_sessions() {
    auto now = std::chrono::steady_clock::now();
    for each (group, member) in active_members:
        if now - last_heartbeat[group][member] > session_timeout:
            remove_from_group(group, member)
            trigger_rebalance(group)
}
```

When a consumer dies without sending a LEAVE_GROUP request, the session timeout
(default 30s, configurable) is the only mechanism that removes it. Set
`session.timeout.ms` much larger than `heartbeat.interval.ms` (3× is standard)
to tolerate GC pauses and network jitter.

---

### Step 5.4 — JOIN_GROUP and Partition Assignment

When a consumer wants to join a group, it sends JOIN_GROUP. The group coordinator:

1. Adds the new member to the group's pending-join list.
2. Starts (or restarts) a `rebalance_timeout` timer. During this window,
   other members may also re-join (they detect rebalancing via error codes).
3. After the timer fires, assigns partitions to members using round-robin:

```cpp
std::vector<PartitionAssignment> assign(
        const std::string& topic, 
        const std::vector<std::string>& member_ids,
        uint32_t num_partitions) {
    std::vector<PartitionAssignment> assignments(member_ids.size());
    for (uint32_t p = 0; p < num_partitions; p++) {
        assignments[p % member_ids.size()].partitions.push_back(p);
    }
    return assignments;
}
```

4. Increments `generation_id` (a monotonic counter per group). This is critical:
   any HEARTBEAT or OFFSET_COMMIT from a member with an old `generation_id` is
   rejected. This prevents a slow/partitioned consumer from committing offsets
   after it's been evicted from the group.

5. Responds to each member's JOIN_GROUP with their assigned partitions and the
   new `generation_id`.

Consumers receive their partition assignments in the JOIN_GROUP response. They
then begin fetching from those partitions using the last committed offsets from
`__consumer_offsets`.

---

### Step 5.5 — LEAVE_GROUP

When a consumer shuts down cleanly, it sends LEAVE_GROUP. The coordinator removes
it immediately and triggers a rebalance without waiting for session timeout. This
is the difference between a 30-second consumer failover (session timeout) and a
sub-second one (clean shutdown).

---

# PHASE 6 — Replication (ISR Protocol)

**Goal:** Run 3 broker nodes. Every partition has one leader and two followers.
Writes go to the leader; the leader returns success only after the ISR has
confirmed the data. Follower-pull model.

---

### Step 6.1 — Cluster Configuration

Add a `cluster.properties` file:

```
broker.id=0
listeners=localhost:9092
cluster.nodes=localhost:9092,localhost:9093,localhost:9094
```

At startup, the broker reads its ID and the full node list. It opens outbound TCP
connections to the other brokers. This inter-broker connection pool is separate
from the client-facing server.

**Partition leadership:** For now, use a static assignment:
`leader = partition_id % num_brokers`. In Phase 7, Raft replaces this.

---

### Step 6.2 — Follower-Pull Replication

Followers pull from the leader, not the other way. Each follower runs a
`ReplicationFetcher` thread per partition it follows.

```cpp
class ReplicationFetcher {
    BrokerClient leader_client_;     // TCP connection to the leader
    PartitionLog& local_log_;
    uint64_t fetch_offset_;          // our current replication position

    void run(std::stop_token st) {
        while (!st.stop_requested()) {
            auto response = leader_client_.fetch(topic_, partition_, fetch_offset_, 4_MB);
            if (response.error == NO_NEW_DATA) {
                std::this_thread::sleep_for(5ms);
                continue;
            }
            local_log_.append_replicated(response.record_data);
            fetch_offset_ = response.high_watermark;
            // Send acknowledgment to leader
            leader_client_.ack(topic_, partition_, fetch_offset_);
        }
    }
};
```

Why follower-pull? The leader doesn't need to know follower speeds or manage
per-follower queues. Followers pull as fast as they can. If a follower is slow,
it just stays behind — it doesn't create backpressure on the leader or force the
leader to buffer data for it.

---

### Step 6.3 — ISR Tracking on the Leader

The leader maintains an In-Sync Replica set per partition:

```cpp
struct ReplicaState {
    uint64_t fetch_offset;       // last confirmed offset from this follower
    std::chrono::steady_clock::time_point last_ack_time;
    bool in_isr;
};

class LeaderState {
    std::unordered_map<uint32_t, ReplicaState> replicas_;  // broker_id → state
    std::vector<uint32_t> isr_;   // broker IDs currently in the ISR
    std::mutex isr_mu_;           // ISR changes are infrequent, mutex is fine

    void on_follower_ack(uint32_t broker_id, uint64_t confirmed_offset) {
        std::lock_guard lock(isr_mu_);
        replicas_[broker_id].fetch_offset = confirmed_offset;
        replicas_[broker_id].last_ack_time = now();
        // Check if this follower should re-join the ISR
        if (!replicas_[broker_id].in_isr && 
            (next_offset_ - confirmed_offset) < replica_lag_threshold_) {
            isr_.push_back(broker_id);
            replicas_[broker_id].in_isr = true;
        }
    }

    void shrink_isr_check() {
        // Called periodically; remove lagging followers from ISR
        std::lock_guard lock(isr_mu_);
        isr_.erase(std::remove_if(isr_.begin(), isr_.end(), [&](uint32_t id) {
            return (next_offset_ - replicas_[id].fetch_offset) > replica_lag_threshold_
                || (now() - replicas_[id].last_ack_time) > replica_lag_time_ms_;
        }), isr_.end());
    }
};
```

---

### Step 6.4 — Write Quorum Using ISR

When a producer sends `Acks = -1` (wait for all ISR), the leader:

1. Appends the batch locally.
2. Creates a `PendingWrite` future/promise keyed on the batch's last offset.
3. Returns the socket response only when the `PendingWrite` is resolved.
4. `PendingWrite` is resolved in `on_follower_ack()` when enough ISR members have
   confirmed `confirmed_offset >= batch_last_offset`.

"Enough" = all members currently in the ISR (not a majority — this is stricter
than quorum but necessary for durability in Kafka's model). `min.insync.replicas`
is the floor: if ISR size drops below this threshold, the leader rejects writes
with `NOT_ENOUGH_REPLICAS` instead of risking data loss by writing to too few
replicas.

```cpp
void on_follower_ack(uint32_t broker_id, uint64_t confirmed_offset) {
    // ... update replica state ...
    // Check if any pending writes are now fully replicated
    auto it = pending_writes_.lower_bound(confirmed_offset);
    // ... resolve futures for all entries <= confirmed_offset in ISR ...
}
```

---

### Step 6.5 — Leader Failover (Manual for Now)

When the leader crashes, one of the ISR members must become the new leader. In
Phase 6, implement this manually: send a LEADER_CHANGE admin API call to one of
the ISR brokers, passing the partition ID. It verifies it's in the ISR, sets
itself as leader, and starts accepting writes.

The `min.insync.replicas` check ensures that the new leader has all data that
was durably committed by the old leader. This is the key ISR safety guarantee:
if data was acknowledged to a producer, it was in the ISR, and the new leader
came from the ISR, therefore the new leader has that data.

---

# PHASE 7 — Raft Consensus

**Goal:** Replace manual leader election and static partition assignment with a
Raft state machine. Leaders are elected automatically on failure. Membership can
change safely.

---

### Step 7.1 — Raft State Machine Structure

Each Raft node has one of three roles: Follower, Candidate, Leader. State
transitions:

```
Follower  → Candidate: election timeout fires (no heartbeat from leader)
Candidate → Leader:    wins election (majority votes)
Candidate → Follower:  sees higher term, or another candidate wins
Leader    → Follower:  sees higher term in any RPC
```

**Persistent state (must survive crashes — fsync before every RPC response):**

```cpp
struct RaftPersistentState {
    uint64_t current_term;   // latest term seen
    int32_t  voted_for;      // candidate voted for in current term (-1 = none)
    // The Raft log entries (stored as binary file, not in this struct)
};
```

Write persistent state to `raft_state.bin` using `pwrite` + `fdatasync`. Do not
return from a RequestVote or AppendEntries RPC handler until this file is fsynced.
Violating this is not a performance optimization — it's a correctness violation
that can cause split-brain.

---

### Step 7.2 — Raft Log Entry Format

The Raft log is separate from the partition data log. It stores commands:

```
[Term       (8 bytes)] = the term in which this entry was appended
[EntryType  (1 byte )] = 0x01 PARTITION_WRITE, 0x02 CONFIG_CHANGE, 0x03 NOOP
[DataLength (4 bytes)]
[Data       (N bytes)] = for PARTITION_WRITE: (topic, partition, record_batch)
```

The Raft log is an append-only binary file managed similarly to Phase 1 segments,
but simpler: no sparse index needed (you access by log index, not by record offset,
and Raft logs are much smaller than data logs). Use a single in-memory array of
log entry positions for O(1) index lookup.

---

### Step 7.3 — Leader Election

**Election timer:** Each Follower maintains an election timeout, randomized between
150ms and 300ms (Raft's recommended range). Reset it whenever you receive a valid
heartbeat AppendEntries from the current leader.

If the timer fires, transition to Candidate:

```cpp
void start_election() {
    current_term_++;
    role_ = CANDIDATE;
    voted_for_ = my_id_;
    persist_state();  // fsync before sending RPCs

    uint64_t votes = 1;  // vote for self
    for (uint32_t peer : peers_) {
        send_request_vote(peer, current_term_, my_id_,
                          last_log_index_, last_log_term_);
    }
    // Reset election timer in case this election times out
    reset_election_timer();
}
```

**RequestVote RPC handler (on receiver):**

```cpp
RequestVoteResponse handle_request_vote(RequestVoteArgs args) {
    // Rule 1: Reject if term < current_term
    if (args.term < current_term_) return {current_term_, false};

    // Rule 2: Update term if args.term > current_term_ (revert to Follower)
    if (args.term > current_term_) {
        current_term_ = args.term;
        voted_for_ = -1;
        role_ = FOLLOWER;
        persist_state();
    }

    // Rule 3: Grant vote only if we haven't voted for someone else
    // AND the candidate's log is at least as up-to-date as ours
    bool log_ok = (args.last_log_term > last_log_term_)
               || (args.last_log_term == last_log_term_ 
                   && args.last_log_index >= last_log_index_);

    if ((voted_for_ == -1 || voted_for_ == args.candidate_id) && log_ok) {
        voted_for_ = args.candidate_id;
        persist_state();  // fsync before responding
        reset_election_timer();
        return {current_term_, true};
    }
    return {current_term_, false};
}
```

When a Candidate receives votes from a majority of nodes (including itself), it
transitions to Leader and immediately sends a no-op AppendEntries to all peers
(the NOOP entry). This commits any uncommitted entries from the previous term
(Raft's log matching property).

---

### Step 7.4 — Log Replication (AppendEntries)

The Leader sends AppendEntries RPCs to all followers. In steady state (no new
entries), this doubles as a heartbeat. The Leader sends AppendEntries at
`heartbeat.interval.ms` (e.g., every 50ms) to prevent election timeouts.

**AppendEntries RPC:**

```
[Term           (8 bytes)] = leader's current term
[LeaderId       (4 bytes)]
[PrevLogIndex   (8 bytes)] = index of log entry immediately before new ones
[PrevLogTerm    (8 bytes)] = term of prevLogIndex entry
[NumEntries     (4 bytes)]
[Entries        (N bytes)] = the new entries to append (0 for heartbeat)
[LeaderCommit   (8 bytes)] = leader's commit index
```

**AppendEntries handler (on follower):**

```cpp
AppendEntriesResponse handle_append_entries(AppendEntriesArgs args) {
    if (args.term < current_term_) return {current_term_, false};
    reset_election_timer();   // valid leader heartbeat
    
    if (args.term > current_term_) {
        current_term_ = args.term;
        role_ = FOLLOWER;
        persist_state();
    }

    // Log consistency check: our log must contain the entry at prevLogIndex
    // with prevLogTerm. If not, reject (leader will retry with earlier entries)
    if (args.prev_log_index > 0) {
        if (log_.size() <= args.prev_log_index ||
            log_[args.prev_log_index].term != args.prev_log_term) {
            return {current_term_, false};
        }
    }

    // Append new entries (truncate any conflicting entries first)
    for (auto& entry : args.entries) {
        uint64_t idx = args.prev_log_index + 1;
        if (idx < log_.size() && log_[idx].term != entry.term) {
            log_.truncate(idx);  // delete conflicting suffix
        }
        if (idx >= log_.size()) {
            log_.append(entry);
            persist_log_entry(entry);  // fsync
        }
    }

    // Advance commit index
    if (args.leader_commit > commit_index_) {
        commit_index_ = std::min(args.leader_commit, last_log_index_);
        apply_committed_entries();  // apply to the partition data log
    }

    return {current_term_, true};
}
```

**Commit rule:** The leader advances `commit_index` when an entry is stored on a
majority of servers. Crucially, the leader can only commit entries from the
current term this way (not previous-term entries directly). The no-op entry sent
on election handles the previous-term entries implicitly.

---

### Step 7.5 — State Machine Application

`apply_committed_entries()` takes each committed Raft log entry and applies it to
the actual partition data log (Phase 1's `PartitionLog::append()`). The partition
data log is the Raft state machine. The Raft log is the WAL; the partition log is
the state.

The commit index and last applied index must be tracked separately. Committed
entries are durable (majority has them); applied entries have been written to the
partition log. On crash recovery:

1. Load `raft_state.bin` → `current_term`, `voted_for`.
2. Replay Raft log entries up to `commit_index` into the partition log.
3. The partition log may already have these entries (from a previous successful
   application before the crash). Skip already-applied entries based on their
   index vs `last_applied_index`.

---

### Step 7.6 — Raft Log Snapshots

Without snapshots, the Raft log grows forever. A new node joining or a crashed
node recovering must replay the entire log from entry 0 — which could be months
of data.

**Snapshot trigger:** When `last_applied_index - snapshot_index > SNAPSHOT_THRESHOLD`
(e.g., every 10,000 entries), take a snapshot.

**Snapshot content:** Serialize the current state of all partition logs:
`(topic, partition, next_offset, committed_offsets)`. You don't need to snapshot
the actual record data (it's on disk in the partition log). You only snapshot the
*metadata* that the Raft state machine tracks.

**Snapshot file format:**

```
[SnapshotIndex (8 bytes)] = last_applied_index when snapshot was taken
[SnapshotTerm  (8 bytes)] = log term at SnapshotIndex
[NumPartitions (4 bytes)]
per partition:
  [TopicLength (2 bytes)]
  [Topic       (N bytes)]
  [PartitionId (4 bytes)]
  [NextOffset  (8 bytes)]
```

After a snapshot is successfully written to disk, truncate the Raft log to remove
all entries before `SnapshotIndex`.

**InstallSnapshot RPC:** When a follower is so far behind that the leader has
already discarded the log entries it needs (they were before the snapshot), the
leader sends the snapshot directly via `InstallSnapshot`. The follower discards
its current log and state, installs the snapshot, and resumes replication from
`SnapshotIndex + 1`.

---

### Step 7.7 — Cluster Membership Changes

Adding or removing a node from the cluster is dangerous if done naively. If you
go from 3 nodes to 5 nodes by simply telling each node "there are now 5 nodes,"
there's a brief window where two majorities can exist (one of size 2 from the
old config, one of size 3 from the new config), enabling split-brain.

**Implement single-server membership changes (the simplified Raft approach):**

You can safely add or remove one node at a time. To add node D to a cluster of
{A, B, C}:

1. Add a CONFIG_CHANGE log entry to the Raft log: `{ADD_NODE, D, D's address}`.
2. This entry is replicated and committed like any other entry.
3. When applied, each node updates its peer list to include D.
4. D receives an InstallSnapshot from the leader to catch up.

The safety guarantee: adding one node at a time means the old majority (2 of 3)
and the new majority (3 of 4) always overlap by at least one node. A leader in
the old config cannot co-exist with a leader in the new config.

For removal: similar process with REMOVE_NODE. The removed node must not be the
current leader (step down first if it is).

---

## Final Benchmark Targets (Cite These on Your Resume)

| Metric                                | Target         | How to Measure                          |
|---------------------------------------|----------------|-----------------------------------------|
| Producer throughput (single partition)| 500 MB/s       | bench/produce_bench.cpp, NVMe disk      |
| Producer throughput (10 partitions)   | 1+ GB/s        | 10 concurrent producer threads          |
| Consumer fetch throughput             | 1+ GB/s        | sendfile from page cache                |
| End-to-end latency (Acks=1)          | < 500µs p99    | producer → leader ack roundtrip         |
| End-to-end latency (Acks=-1, ISR=3)  | < 5ms p99      | producer → ISR quorum → ack roundtrip   |
| Raft leader election time             | < 300ms        | kill leader, measure time to new leader |
| Recovery time after crash             | < 10s          | restart broker, measure until serving   |
| Random read latency (warm cache)      | < 50µs p50     | Phase 1 benchmark                       |

---

## Things to Know Cold for Interviews

These will be asked. Have specific answers.

- Why sparse index and not one entry per record?
- Why CRC over payload but not over offset/length fields?
- Why follower-pull replication and not leader-push?
- What happens if ISR shrinks to 1 during a produce with `min.insync.replicas=2`?
- How does the `generation_id` prevent a slow consumer from corrupting offsets?
- Why must `votedFor` be fsynced before responding to RequestVote?
- What is the "leader completeness" property in Raft and how does `log_ok` check enforce it?
- What is the difference between `commit_index` and `last_applied_index`?
- Why can't the leader commit entries from previous terms directly?
- How does `sendfile` achieve zero-copy and what is its Linux kernel path?
- Why does `EPOLLET` require you to drain the socket until `EAGAIN`?
- What is `SO_REUSEPORT` and how does the kernel distribute connections?
