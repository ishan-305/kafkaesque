# Kafkaesque — Architecture & System Design

**Version:** v1 (Phase 1 complete)
**Scope:** single-broker, durable, restartable event streaming platform in C++20.
No replication, no consensus, no consumer-group rebalancing (see [§9](#9-known-limitations--future-work)).

This document has two halves:

- **[Part A — High-Level Design](#part-a--high-level-design-hld):** what the components are, how data and requests flow, the threading and durability model.
- **[Part B — Low-Level Design](#part-b--low-level-design-lld):** exact on-disk formats, wire protocol byte layouts, per-module class specs, recovery algorithms, locking, and complexity.

---

# Part A — High-Level Design (HLD)

## 1. System Overview

Kafkaesque is a message log, not a queue. Producers append records to named
**topics**; each topic is split into **partitions**; each partition is an
ordered, append-only, durable log on disk. Consumers read by **offset** and
own their read position — the broker never deletes a message because it was
read, and many consumers can read the same data independently.

**The v1 durability contract:** once the broker ACKs a produce, the record
survives `kill -9`. On restart, every acked record is readable from offset 0,
in order. (Validated: produce 5000 → SIGKILL → restart → consume 5000 in order.)

```
                        ┌──────────────────────── Broker process ────────────────────────┐
                        │                                                                 │
 ┌──────────┐  TCP      │  ┌───────────┐    ┌────────────┐    ┌──────────────────────┐   │
 │ Producer │ ────────► │  │ TcpServer │───►│   Broker   │───►│     TopicManager     │   │
 └──────────┘  frames   │  │ (accept + │    │ (dispatch) │    │ (owns PartitionLogs) │   │
                        │  │  threads) │    │            │    └──────────┬───────────┘   │
 ┌──────────┐           │  └───────────┘    │            │               │ owns N        │
 │ Consumer │ ────────► │                   │            │    ┌──────────▼───────────┐   │
 └──────────┘           │                   │            │───►│     PartitionLog     │   │
                        │                   │            │    │  (one per partition) │   │
                        │                   │            │    └──────────┬───────────┘   │
                        │                   │            │               │ dir of        │
                        │                   │            │    ┌──────────▼───────────┐   │
                        │                   └───────────►│    │ Segment (.log+.index)│   │
                        │                    OffsetStore │    │      (rolling)       │   │
                        │                   (__offsets)  │    └──────────────────────┘   │
                        └─────────────────────────────────────────────────────────────────┘
                                                     │
                                                 ────▼────
                                                  disk (fsync)
```

## 2. Layered Architecture

Strict one-way dependency order. Each module depends only on modules above it;
nothing below the line knows sockets, nothing above the line knows files.

```
Layer 0  common          crc32, ByteBuffer, Status        (pure bytes, no I/O)
Layer 1  record          ser/de of one record             (bytes ↔ struct)
Layer 2  index           sparse offset → file-pos map     (one small file)
Layer 3  segment         one .log + .index file pair      (file I/O starts here)
Layer 4  partition_log   directory of segments  ⭐ CORE   (offset authority)
──────────────────────── storage/network boundary ────────────────────────────
Layer 5  protocol        wire message ser/de              (pure bytes, no sockets)
Layer 6  tcp_server      accept loop, Conn                (sockets, no protocol)
Layer 7  broker          dispatch + TopicManager          (orchestration only)
Layer 8  offset_store    durable consumer offsets         (one append-log file)
Layer 9  client          Producer, Consumer               (sync request/response)
```

Key boundary rules, enforced by what each class is *not allowed* to know:

| Module | Owns | Explicitly does NOT do |
|---|---|---|
| `ByteBuffer` | growable write buf + read cursor | file or socket I/O |
| `Record` ser/de | one record's bytes + CRC | offsets, scanning, files |
| `OffsetIndex` | sparse floor lookup | reading the log file |
| `Segment` | one file pair, append/read at pos | roll decisions, fsync policy |
| `PartitionLog` | offset counter, roll, locate segment | partition routing, topics |
| `protocol` | frame + payload byte layouts | sockets, partial reads |
| `TcpServer`/`Conn` | bytes in/out, framing reads | message semantics |
| `Broker` | request dispatch, ack policy | storage internals, socket internals |
| `TopicManager` | PartitionLog lifetime, key routing | request handling |
| `OffsetStore` | durable (group,topic,partition)→offset | anything else |

## 3. Data Model

```
Topic "orders" (num_partitions = 4, chosen at create time)
 ├── partition 0  →  directory  data/orders-0/
 ├── partition 1  →  directory  data/orders-1/
 ├── partition 2  →  directory  data/orders-2/
 └── partition 3  →  directory  data/orders-3/

data/orders-0/                            one PartitionLog
 ├── 00000000000000000000.log             segment: offsets [0, 3061)
 ├── 00000000000000000000.index
 ├── 00000000000000003061.log             segment: offsets [3061, 6132)
 ├── 00000000000000003061.index
 ├── 00000000000000006132.log             ACTIVE segment (tail, appends go here)
 └── 00000000000000006132.index

data/__offsets                            OffsetStore append-log (all groups)
```

- **Offset** = position of a record within one partition. Starts at 0,
  increments by 1, never reused. Offsets are per-partition — there is no
  global order across partitions of a topic.
- **Offset is implicit on disk.** A record's offset is
  `segment.base_offset + its position in the segment`. Not storing it per
  record saves 8 bytes/record and makes corruption of the ordering impossible.
- **Segment** = one bounded chunk of a partition (rolls at
  `max_segment_bytes`, default 64 MB). Bounding segments is what makes
  future retention (delete oldest file) and fast recovery (scan only the
  tail segment... future work: v1 scans all) possible.
- **High watermark** = next offset to be assigned = end of log. Consumers
  stop there.
- **Consumer position is consumer-owned.** The broker stores it only when the
  consumer explicitly commits, keyed by `(group, topic, partition)`.

## 4. Request Flows

### 4.1 Produce (the write path)

```
Producer                    Broker thread (per conn)              Disk
   │                             │                                  │
   │  PRODUCE{topic,part=-1,     │                                  │
   │          key,value}         │                                  │
   ├────────────────────────────►│                                  │
   │                             │ 1. decode ProduceReq             │
   │                             │ 2. partition = hash(key) % N     │
   │                             │    (or explicit if part >= 0)    │
   │                             │ 3. TopicManager.get_or_create    │
   │                             │ 4. PartitionLog.append:          │
   │                             │      lock partition mutex        │
   │                             │      roll segment if too big     │
   │                             │      offset = next_offset++      │
   │                             │      serialize + pwrite ────────►│ .log
   │                             │      index entry every 8 recs    │
   │                             │ 5. PartitionLog.flush ──────────►│ fsync .log + .index
   │  ACK{offset}                │ 6. encode ACK                    │
   │◄────────────────────────────┤                                  │
```

Step 5 is the durability contract: **fsync happens before the ACK leaves the
broker**. An acked message can never be lost to a crash. This is also the v1
throughput ceiling (one fsync per message) — see [§8](#8-design-decisions--trade-offs).

### 4.2 Fetch (the read path)

```
Consumer                    Broker thread                         Disk
   │  FETCH{topic,part,          │                                  │
   │        offset=42,           │                                  │
   │        max_bytes}           │                                  │
   ├────────────────────────────►│                                  │
   │                             │ 1. decode FetchReq               │
   │                             │ 2. log = TopicManager.get        │
   │                             │ 3. loop while under max_bytes    │
   │                             │    and below high watermark:     │
   │                             │      PartitionLog.read(cur):     │
   │                             │        binary-search segment     │
   │                             │        index floor lookup        │
   │                             │        scan ≤ 7 records          │
   │                             │        pread record ◄────────────│ .log
   │                             │        verify CRC                │
   │  FETCH{records[],           │ 4. encode response               │
   │        next_offset,         │                                  │
   │        high_watermark}      │                                  │
   │◄────────────────────────────┤                                  │
```

Reads are batched: the broker packs records until `max_bytes` is exceeded
(always at least one). The response carries `next_offset` so the consumer
never computes offsets itself, and `high_watermark` so it knows when it is
caught up. Every read re-verifies the CRC — disk corruption is detected at
read time, not silently returned.

### 4.3 Subscribe + commit (consumer progress)

```
Consumer                          Broker
   │  METADATA{topic,group,part}    │
   ├───────────────────────────────►│  OffsetStore.fetch(group,topic,part)
   │  METADATA{num_partitions,      │  (0 if never committed)
   │           committed_offset,    │
   │           high_watermark}      │
   │◄───────────────────────────────┤
   │        ... poll loop (FETCH) advances local position ...
   │  COMMIT_OFFSET{group,topic,    │
   │                part,offset}    │
   ├───────────────────────────────►│  OffsetStore.commit:
   │                                │    append entry to __offsets, fsync
   │  ACK{offset}                   │
   │◄───────────────────────────────┤
```

Commit semantics are **at-most-once per record for the group** in the happy
path: the consumer commits `position` (everything already polled). If the
consumer crashes after processing but before committing, the group re-reads
those records on resume — so processing should be idempotent. (True
at-least-once with retries is Phase 3.)

### 4.4 Broker startup (crash recovery)

```
Broker.start(port)
 ├─ TopicManager.load()
 │   ├─ scan data_dir for "<topic>-<partition>" directories
 │   └─ for each: PartitionLog.open(dir)
 │       ├─ list *.log files, sort by base_offset
 │       ├─ for each segment: Segment.open
 │       │   ├─ scan log from byte 0, record by record
 │       │   ├─ on torn/corrupt tail record → ftruncate log there
 │       │   └─ rebuild .index in memory + rewrite file  (log = source of truth)
 │       ├─ if no segments → create segment with base_offset 0
 │       └─ next_offset = last_segment.next_offset()      (offset counter recovered)
 ├─ OffsetStore.open("__offsets")
 │   ├─ replay file into map, last entry per key wins
 │   └─ truncate torn tail entry if crash happened mid-commit
 ├─ TcpServer.listen(port)
 └─ spawn serve thread → accept loop
```

Recovery never trusts derived state: the `.index` file is thrown away and
rebuilt from the `.log` scan, and both the log and the offsets file tolerate a
torn final entry (the one write that may have been in flight at crash time) by
truncating it. A torn log record was by definition never acked (the fsync
before ACK hadn't completed), so truncating it does not violate the contract.

## 5. Threading & Concurrency Model

**Thread-per-connection (v1).**

```
main thread            serve thread              conn threads (one per client)
    │  Broker.start()      │                          │
    ├──────────spawn──────►│  accept() loop           │
    │                      ├──────────spawn──────────►│  loop: read frame →
    │                      │                          │        dispatch →
    │  (blocks on signal)  │                          │        write response
```

- Each client connection gets a dedicated thread running a synchronous
  read → dispatch → respond loop. Requests **on one connection** are strictly
  ordered; requests on different connections interleave.
- Shared state and its guard:

| Shared state | Guard | Granularity |
|---|---|---|
| one partition's segments + offset counter | `PartitionLog::mu_` | per partition — two partitions never contend |
| topics map | `TopicManager::mu_` | global, but held only for map lookup/create |
| committed offsets map + file | `OffsetStore::mu_` | global (commits are rare vs produces) |
| server thread/fd bookkeeping | `TcpServer::mu_` | accept/stop only |

- **The partition is the unit of parallelism** — same as real Kafka. Writes to
  the same partition serialize on its mutex (correct: they must agree on the
  offset counter); writes to different partitions run truly parallel.
- Known bottleneck, accepted for v1 correctness: thousands of connections
  would mean thousands of threads. The fix (epoll/kqueue event loop) is a
  contained change — only `tcp_server.cpp` knows about threads.

## 6. Durability Model

| Data | When durable | Mechanism |
|---|---|---|
| produced record | before PRODUCE is acked | `pwrite` + `fsync` of .log (+ .index) |
| index entries | with the segment flush | rebuilt from log anyway if lost |
| committed consumer offset | before COMMIT_OFFSET is acked | append to `__offsets` + `fsync` |
| topic/partition existence | on first append | directories on disk |

Failure matrix:

| Crash moment | Outcome |
|---|---|
| after pwrite, before fsync, before ACK | record may vanish — fine, never acked; producer sees dead conn |
| after fsync, before ACK | record survives; producer sees dead conn and may retry → duplicate (at-least-once retry is Phase 3) |
| mid-write (torn record at tail) | truncated at recovery; was never acked |
| mid-commit of consumer offset | torn entry truncated; group resumes from previous commit → re-reads some records |
| index file lost/corrupt | irrelevant — rebuilt from log on open |

---

# Part B — Low-Level Design (LLD)

## 7. Byte-Level Formats

All integers little-endian, fixed width. No varints (educational clarity over
space).

### 7.1 Record (in `.log` files) — `record.h/.cpp`

```
offset 0        4        8               16          16+K            20+K
        ┌────────┬────────┬───────────────┬───────────┬────┬──────────┬───────┐
        │ length │  crc   │   timestamp   │  key_len  │key │ value_len│ value │
        │  u32   │  u32   │      u64      │    u32    │ K B│   u32    │  V B  │
        └────────┴────────┴───────────────┴───────────┴────┴──────────┴───────┘
                 ◄────────────── crc covers all of this ──────────────────────►
        ◄─────────────────── length counts all of this ───────────────────────►
```

- `length` = bytes after the length field = `4 + 8 + 4 + K + 4 + V`.
  Minimum legal value 20 (empty key, empty value); smaller ⇒ `CORRUPT`.
- `crc` = CRC-32 (polynomial `0xEDB88320`, table-driven, init `0xFFFFFFFF`,
  final XOR) over every byte after the crc field.
- `encoded_size(r)` = `8 + 4 + K + 4 + V + 8` = full on-disk footprint; used
  by segment-roll decisions and fetch `max_bytes` accounting.
- Deserialize error ladder: incomplete frame ⇒ `SHORT_READ`; crc mismatch ⇒
  `CRC`; internal lengths inconsistent with `length` ⇒ `CORRUPT`.

### 7.2 Sparse index (in `.index` files) — `index.h/.cpp`

```
        ┌────────────┬──────────┐┌────────────┬──────────┐
        │ rel_offset │ file_pos ││ rel_offset │ file_pos │ ...
        │    u32     │   u32    ││    u32     │   u32    │
        └────────────┴──────────┘└────────────┴──────────┘
```

- `rel_offset = offset - segment.base_offset` (u32 is enough: a segment never
  holds 4B records).
- One entry per `Segment::kIndexInterval = 8` records, written for records
  0, 8, 16, … — so entry `(0, 0)` always exists in a non-empty segment.
- Entries are sorted by construction (append-only, monotonic); out-of-order
  append is rejected with `INVALID_ARGUMENT`.

### 7.3 Segment file naming

```
<base_offset padded to 20 digits>.log / .index      e.g. 00000000000000003061.log
```

20 digits = max u64, so lexicographic order == numeric order (`ls` shows
segments in offset order; recovery sorts numerically anyway).

### 7.4 Consumer offsets file (`data/__offsets`) — `offset_store.h/.cpp`

```
        ┌───────────┬───────┬───────────┬───────┬───────────┬────────┐
        │ group_len │ group │ topic_len │ topic │ partition │ offset │  (repeated)
        │    u32    │  ...  │    u32    │  ...  │    u32    │  u64   │
        └───────────┴───────┴───────────┴───────┴───────────┴────────┘
```

Pure append-log; the in-memory `map<(group,topic,partition), offset>` is the
read view, **last entry wins** on replay. No compaction in v1 (file grows by
~30 bytes per commit; fine at v1 scale).

### 7.5 Wire frame (TCP) — `protocol.h/.cpp`

```
        ┌────────┬──────┬───────────────────┐
        │  len   │ type │      payload      │
        │  u32   │  u8  │   len - 1 bytes   │
        └────────┴──────┴───────────────────┘
```

- `len` counts everything after itself (type byte + payload), so `len >= 1`.
- `Conn::read_frame_bytes` rejects `len == 0` or `len > 64 MB` (`CORRUPT`)
  before allocating — a garbage length prefix can't OOM the broker.
- Strings inside payloads: `[len:u32][bytes]`, same helper everywhere.

Message types and payload layouts (`MsgType : u8`):

| type | value | payload layout |
|---|---|---|
| `PRODUCE` | 1 | `topic:str, partition:i32 (-1 ⇒ key-routed), key:str, value:str` |
| `FETCH` | 2 | req: `topic:str, partition:u32, offset:u64, max_bytes:u32` — resp: `count:u32, count × serialized Record, next_offset:u64, high_watermark:u64` |
| `ACK` | 3 | `offset:u64` |
| `ERROR` | 4 | `code:u32 (a Status), message:str` |
| `COMMIT_OFFSET` | 5 | `group:str, topic:str, partition:u32, offset:u64` |
| `METADATA` | 6 | req: `topic:str, group:str, partition:u32` — resp: `num_partitions:u32, committed_offset:u64, high_watermark:u64` |

FETCH responses reuse the **exact on-disk record encoding** (§7.1) inside the
payload — one serializer, one deserializer, one CRC check for both disk and
wire.

Every request gets exactly one response frame on the same connection: the
matching response type, or `ERROR{code, message}`. Request/response, no
pipelining (v1 clients are synchronous).

## 8. Module Internals

### 8.1 `common` — `Status`, `ByteBuffer`, `crc32`

- `enum class Status { OK, CRC, SHORT_READ, CORRUPT, NOT_FOUND, INVALID_ARGUMENT, IO_ERROR }`
  — return codes everywhere; **no exceptions on the hot path**. Out-params
  carry results; `Status` carries success/failure.
- `ByteBuffer` = `vector<uint8_t>` (write side, append-only) + `read_ptr_`
  cursor (read side). `get_*` bounds-check via `remaining()` and return
  `SHORT_READ` instead of reading garbage. Used as: serialization target,
  deserialization source, and frame assembly buffer.
- `crc32`: 256-entry table built once (thread-safe via static-local init),
  returned by const reference.

### 8.2 `Segment` — one file pair

```cpp
class Segment {
  static constexpr uint64_t kIndexInterval = 8;
  static Status create(dir, base_offset, unique_ptr<Segment>&); // O_TRUNC both files
  static Status open  (dir, base_offset, unique_ptr<Segment>&); // recovery scan
  Status append(const Record&, uint64_t offset);   // offset MUST == next_offset()
  Status read_at(uint64_t byte_pos, Record&, uint64_t& next_pos) const;  // pread
  Status read(uint64_t abs_offset, Record&) const; // index floor + forward scan
  Status flush();                                  // fsync log, flush+fsync index
  uint64_t size_bytes() / base_offset() / next_offset();  // next = base + count
  // state: log_fd_, log_size_, record_count_, OffsetIndex index_
};
```

Invariants:
- `append` asserts the caller-passed offset equals `next_offset()` — offsets
  are assigned by `PartitionLog`, and the segment refuses to let them skew
  (`INVALID_ARGUMENT`). The offset is then *discarded*: position encodes it.
- Writes use `pwrite` at `log_size_` (no seek state to corrupt), loop-until-
  written, index entry added **before** advancing counters so entry
  `(count, pos)` always points at a record start.
- `read_at` reads the 4-byte length, bounds-checks `pos + 4 + length` against
  `log_size_`, then preads the rest and runs the full `deserialize` (CRC
  re-verified on every read).
- **Recovery scan** (`open`): walk `read_at` from byte 0. Every clean record:
  count it, add an index entry every 8. First `SHORT_READ`/`CRC`/`CORRUPT`:
  stop, `ftruncate` the file there. The old `.index` file is ignored and
  recreated — the log is the single source of truth.

### 8.3 `OffsetIndex` — floor lookup

- In-memory `vector<pair<u32 rel, u32 pos>>` + fd for persistence.
- `lookup(target)`: `std::upper_bound` on `rel`, step back one ⇒ **floor**
  entry (largest ≤ target); empty index ⇒ `(0, 0)`, which is always correct
  because a segment's first record sits at byte 0. O(log E).
- `flush()`: writes only entries appended since last flush (`flushed_count_`
  high-water mark) via `pwrite` at the exact file position, then fsync —
  flushing is O(new entries), not O(all entries).
- `load()` tolerates a trailing partial entry (8-byte granularity) by
  truncating it.

### 8.4 `PartitionLog` — the core

```cpp
struct PartitionLogOptions {
  uint64_t max_segment_bytes = 64 MB;   // roll threshold
  bool fsync_each_append   = false;     // broker flushes per-produce instead
};
class PartitionLog {
  static Status open(dir, unique_ptr<PartitionLog>&, opts);
  Status append(const Record&, uint64_t& assigned_offset);
  Status read(uint64_t offset, Record&);
  Status read_from(uint64_t offset, size_t max, vector<Record>&);
  uint64_t high_watermark();
  Status flush();                        // active segment only
  // state: mutex mu_, vector<unique_ptr<Segment>> segments_ (sorted), next_offset_
};
```

- **Sole authority for the offset counter.** `append`: lock → roll if
  `active.size_bytes() >= max_segment_bytes` (flush old, create new segment
  with `base = next_offset_`) → `offset = next_offset_` → segment append →
  `next_offset_ = offset + 1`. Counter increments only after a successful
  write, so a failed append doesn't burn an offset.
- `open`: `create_directories`; parse every `*.log` filename to a base offset
  (`std::from_chars`, non-numeric names skipped); sort; `Segment::open` each;
  empty dir ⇒ create segment 0; `next_offset_ = back().next_offset()`.
- Segment location: `upper_bound` over `base_offset` then step back — the last
  segment with `base ≤ offset`. O(log S).
- Older segments were flushed when rolled, so `flush()` only needs the active
  one.
- Read cost: O(log S) segment search + O(log E) index lookup + ≤ 7 sequential
  record preads. Write cost: O(1) amortized + the fsync.

### 8.5 `TcpServer` / `Conn`

- `Conn` owns one fd (closes in destructor). `read_n`/`write_all` loop until
  complete; peer close during a read surfaces as `SHORT_READ`, which every
  caller treats as "connection over".
- `read_frame_bytes`: read 4-byte len → validate (`0 < len ≤ 64 MB`) → read
  exactly len bytes → hand back the *whole* frame so `protocol::read_frame`
  can parse it. Framing lives here; meaning lives in `protocol`.
- `TcpServer::listen(0)` supports ephemeral ports (reads the real port back
  with `getsockname`) — tests never collide.
- `serve`: accept loop; per connection set `TCP_NODELAY` (request/response
  workload — Nagle would add latency), spawn a thread, track its fd.
  Threads deregister their fd on exit *before* the fd closes, so `stop()`
  can never `shutdown()` an fd number the OS already reused.
- `stop()`: flag → shutdown+close the listen fd (unblocks `accept`) →
  shutdown all live conn fds (unblocks their reads) → join all threads.
  Idempotent; destructor calls it.

### 8.6 `Broker` + `TopicManager` + `OffsetStore`

`Broker::handle_conn` per-connection loop:

```
read frame → parse (bad frame ⇒ ERROR resp) → switch(type) → handler → write response
```

Handlers are thin orchestration, ~30 lines each:

- `handle_produce`: decode → pick partition (explicit `partition >= 0`, else
  `TopicManager::route = hash(key) % N`) → `get_or_create` → build `Record`
  with broker-assigned `timestamp = now_millis()` → `append` → **`flush`
  before ACK** (unless `fsync_each_append` already did) → `ACK{offset}`.
- `handle_fetch`: decode → `get` (no auto-create on read; unknown ⇒
  `ERROR{NOT_FOUND}`) → accumulate records while
  `bytes_used + encoded_size(next) ≤ max_bytes` (always at least one, so one
  giant record can't wedge a small-buffer consumer) → respond with records +
  `next_offset` + `high_watermark`.
- `handle_commit`: decode (empty group ⇒ `CORRUPT`) → `OffsetStore::commit`
  (append + fsync) → `ACK`.
- `handle_metadata`: `num_partitions`, `committed_offset` for the group
  (0 if none), partition `high_watermark`. Doubles as the consumer's
  "where do I resume" call.

`TopicManager`:

- `topics_: map<string, vector<unique_ptr<PartitionLog>>>`, one global mutex
  held only for map access — record I/O happens on the `PartitionLog` outside
  this lock.
- Directory convention `<topic>-<partition>`; `load()` rediscovers topics by
  parsing on the **last** `-` (topic names may contain dashes).
- `get_or_create`: existing topic + out-of-range partition ⇒ `NOT_FOUND`
  (partition count is fixed at creation); unknown topic ⇒ auto-create all
  `default_partitions` partitions at once, so key routing is stable from the
  first message.
- `route`: `std::hash<string>(key) % N`. Same key ⇒ same partition ⇒
  per-key ordering holds. (Caveat: `std::hash` is implementation-defined —
  fine single-broker, would need a fixed hash like murmur/fnv the moment two
  processes must agree. Noted for Phase 3 replication.)

### 8.7 Clients (`Producer`, `Consumer`)

Both are thin sync wrappers over one `Conn` + a shared
`request(conn, type, payload) → (resp_type, resp_payload)` helper.
`ERROR` responses map back to the embedded `Status` code, so
`producer.send_to("metrics", 99, ...) == Status::NOT_FOUND` — server errors
and local errors flow through the same enum.

- `Producer::send` (key-routed, `partition = -1`) / `send_to` (explicit);
  blocks until ACK; returns the assigned offset.
- `Consumer` state machine: `connect` → `subscribe(topic, partition, group)`
  (METADATA sets `position_ = committed_offset`, i.e. resume where the group
  left off; never committed ⇒ 0) → `poll` loop (FETCH from `position_`,
  advance to `next_offset` from the response) → `commit` (persist
  `position_`) → optionally `seek(0)` for a full replay.
- Caught-up check: `position() < high_watermark()`.

## 9. Known Limitations & Future Work

Deliberate v1 boundaries — each is a contained change because of the layer
boundaries in §2:

| Limitation | Where the fix goes | Phase |
|---|---|---|
| fsync per produce (throughput ceiling) | group commit in `Broker`/`PartitionLog` (batch acks behind one fsync); benchmark both | 2 |
| `read_from` re-runs index lookup per record | sequential `read_at` chaining inside `Segment` | 2 |
| thread per connection | epoll/kqueue in `tcp_server.cpp` only | 2+ |
| recovery scans every segment (O(log size)) | trust flushed index for sealed segments, scan only active | 2 |
| producer retry on lost ACK ⇒ duplicates possible | idempotent producer / at-least-once protocol | 3 |
| no retention — segments accumulate forever | delete sealed segments by age/size in `PartitionLog` | 3 |
| single broker, no replication | leader/follower log shipping | 3 |
| one consumer per partition, manual assignment | consumer groups: coordinator, heartbeats, rebalance | 3 |
| `std::hash` not stable across builds | fixed hash (fnv/murmur) in `TopicManager::route` | with replication |
| clients accept numeric IPs only (`inet_pton`) | `getaddrinfo` in `client_util.h` | whenever |
| `__offsets` file grows unboundedly | compaction (rewrite last-entry-per-key) | 3 |

## 10. Test Map (what proves what)

| Test target | Proves |
|---|---|
| `common_test` | ByteBuffer round-trips, SHORT_READ, CRC test vector `0xCBF43926` |
| `record_test` | ser/de round-trip, 1-bit corruption ⇒ `CRC`, truncation ⇒ `SHORT_READ`, back-to-back records |
| `index_test` | floor semantics (exact/between/past-end/before-first), persistence, ordering guard |
| `segment_test` | 100-record reopen, non-zero base offset, offset-skew rejection, **torn-tail truncation + append-after-recovery** |
| `partition_log_test` | **THE GATE**: 10k append → reopen → read 0..9999 in order across rolled segments; counter recovery; batch reads |
| `protocol_test` | every message frame round-trips; partial frame ⇒ `SHORT_READ` |
| `tcp_server_test` | echo round-trip; 4 concurrent clients × 50 frames |
| `integration_test` | end-to-end produce/fetch over real TCP; **broker restart, fetch from 0** (the Phase 1 milestone); committed offset survives restart; multi-partition routing stability |
| `smoke_client` + broker binary | out-of-process: produce 5000 → `kill -9` → restart → consume 5000 in order |
