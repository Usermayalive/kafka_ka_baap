# Durability: what each guarantee costs

Every throughput number in this project maps to exactly one durability mode below. If a
number doesn't say which mode it was measured in, don't trust it.

Measurements: Apple M1 (8 cores, 8 GB), macOS 26.6, internal SSD (APFS), JDK 25. 1 KB
records, all connections producing to a single partition, measured end-to-end through the
broker by `BrokerBenchmark` (Netty → per-connection executor → quotas → idempotence →
`Partition`). Each configuration runs 1 s warmup + 3 s measurement.

## The three tiers

### 1. Page cache — `OS_PAGE_CACHE` (default)

`append()` returns once the record and its index entry are written to the kernel page cache.
No `fsync`.

| Survives | Loses |
|---|---|
| Broker process crash (`kill -9`, OOM, JVM crash) | OS crash, kernel panic, power loss: the unflushed tail |

This is Apache Kafka's default (`log.flush.interval.messages` is effectively infinite). Kafka
gets durability from replication across machines rather than from fsync; StormLog does not
replicate (see README, non-goals), so in this mode an OS crash on the single broker that owns
a partition can lose recently acknowledged writes.

**Recovery:** on startup, `Partition` drops index entries whose record is not fully in the
log and truncates log bytes that have no index entry (`truncateTornTail`). Readers never see
a torn record; the lost tail's offsets are reassigned to new writes.

### 2. Group-commit fsync — `PHYSICAL_FSYNC` (opt-in)

`append()` returns only after the record's log and index bytes are forced to stable storage.
Concurrent appenders share one flush: the first waiter becomes the flush leader, snapshots
the log end offset, forces log + index *outside* the partition lock, and releases everyone
whose record is covered. Records appended during a flush join the next one.

| Survives | Loses |
|---|---|
| Process crash, OS crash, power loss (for acknowledged writes) | Nothing acknowledged |

Enable per broker: `SimpleKafkaBroker.setDurabilityMode(PHYSICAL_FSYNC)` or the 5th CLI
argument.

**The single-connection ceiling.** A connection's requests are processed strictly in order
(that's what keeps an idempotent producer's sequences valid), so one connection can never
have two records in the same flush. Its ceiling is one flush per request:
`1 / (2 × force latency)` ≈ 1 / 7.3 ms ≈ **~130 op/s** here, no matter how many requests it
pipelines (pipelining only adds queueing latency). Throughput scales with the number of
*connections* sharing the partition, because their records batch into the same flush.

### 3. Per-message force (removed)

The original `Partition.append` forced the log and then the index on every message, under the
partition write lock: ~7.8 ms per message, ~128 op/s **total**, with zero benefit from
concurrency because the lock serialized the flushes. It was the broker's only mode until the
group-commit rewrite. Tier 2 gives the same guarantee and scales with connections.

## Measured (end-to-end through the broker)

| Durability | Idempotent | Connections | In flight | Payload | Throughput | Bandwidth | p50 | p99 |
|---|---|---|---|---|---|---|---|---|
| OS_PAGE_CACHE | no | 1 | 1 | 1 KB | 18,087 op/s | 17.66 MB/s | 0.05 ms | 0.09 ms |
| OS_PAGE_CACHE | no | 1 | 16 | 1 KB | 77,951 op/s | 76.12 MB/s | 0.13 ms | 0.70 ms |
| OS_PAGE_CACHE | no | 4 | 1 | 1 KB | 40,946 op/s | 39.99 MB/s | 0.08 ms | 0.20 ms |
| OS_PAGE_CACHE | no | 4 | 16 | 1 KB | 85,747 op/s | 83.74 MB/s | 0.63 ms | 2.16 ms |
| OS_PAGE_CACHE | no | 16 | 1 | 1 KB | 41,230 op/s | 40.26 MB/s | 0.44 ms | 1.04 ms |
| OS_PAGE_CACHE | no | 16 | 16 | 1 KB | 88,715 op/s | 86.64 MB/s | 2.47 ms | 8.59 ms |
| OS_PAGE_CACHE | yes | 1 | 1 | 1 KB | 22,396 op/s | 21.87 MB/s | 0.04 ms | 0.22 ms |
| OS_PAGE_CACHE | yes | 1 | 16 | 1 KB | 93,968 op/s | 91.77 MB/s | 0.12 ms | 0.56 ms |
| OS_PAGE_CACHE | yes | 4 | 1 | 1 KB | 40,548 op/s | 39.60 MB/s | 0.07 ms | 0.36 ms |
| OS_PAGE_CACHE | yes | 4 | 16 | 1 KB | 71,934 op/s | 70.25 MB/s | 0.79 ms | 2.46 ms |
| OS_PAGE_CACHE | yes | 16 | 1 | 1 KB | 41,978 op/s | 40.99 MB/s | 0.41 ms | 0.93 ms |
| OS_PAGE_CACHE | yes | 16 | 16 | 1 KB | 65,439 op/s | 63.91 MB/s | 3.40 ms | 12.06 ms |
| PHYSICAL_FSYNC | no | 1 | 1 | 1 KB | 135 op/s | 0.13 MB/s | 7.06 ms | 10.56 ms |
| PHYSICAL_FSYNC | no | 1 | 16 | 1 KB | 131 op/s | 0.13 MB/s | 124.79 ms | 138.09 ms |
| PHYSICAL_FSYNC | no | 4 | 1 | 1 KB | 273 op/s | 0.27 MB/s | 15.08 ms | 22.98 ms |
| PHYSICAL_FSYNC | no | 4 | 16 | 1 KB | 470 op/s | 0.46 MB/s | 133.87 ms | 169.41 ms |
| PHYSICAL_FSYNC | no | 16 | 1 | 1 KB | 997 op/s | 0.97 MB/s | 15.90 ms | 30.05 ms |
| PHYSICAL_FSYNC | no | 16 | 16 | 1 KB | 1,361 op/s | 1.33 MB/s | 188.21 ms | 235.48 ms |
| PHYSICAL_FSYNC | yes | 1 | 1 | 1 KB | 102 op/s | 0.10 MB/s | 8.76 ms | 15.57 ms |
| PHYSICAL_FSYNC | yes | 1 | 16 | 1 KB | 123 op/s | 0.12 MB/s | 127.81 ms | 164.87 ms |
| PHYSICAL_FSYNC | yes | 4 | 1 | 1 KB | 250 op/s | 0.24 MB/s | 14.12 ms | 33.34 ms |
| PHYSICAL_FSYNC | yes | 4 | 16 | 1 KB | 326 op/s | 0.32 MB/s | 197.05 ms | 224.98 ms |
| PHYSICAL_FSYNC | yes | 16 | 1 | 1 KB | 816 op/s | 0.80 MB/s | 18.03 ms | 34.76 ms |
| PHYSICAL_FSYNC | yes | 16 | 16 | 1 KB | 813 op/s | 0.79 MB/s | 308.00 ms | 385.23 ms |
| OS_PAGE_CACHE | yes | 16 | 16 | 64 B | 76,198 op/s | 4.65 MB/s | 2.72 ms | 9.72 ms |
| OS_PAGE_CACHE | yes | 16 | 16 | 1 KB | 44,429 op/s | 43.39 MB/s | 5.00 ms | 17.11 ms |
| OS_PAGE_CACHE | yes | 16 | 16 | 16 KB | 30,337 op/s | 474.01 MB/s | 8.27 ms | 16.27 ms |

Single run; page-cache rows vary roughly ±30% run to run on this laptop (e.g. 1 connection × 16 in flight measured 78K–121K op/s across runs). Fsync rows are stable within ~10%. Reproduce with `BrokerBenchmark.main` (CSV in `target/bench/`).

How to read it:

- **Page cache, 1 connection × 16 in flight** is the best single-producer case: pipelining
  hides the network round trip and the per-request work is microseconds.
- **More connections lower page-cache throughput** on one partition: every append takes the
  partition write lock, and 16 connections × 16 in flight means 256 queued requests
  contending for 16 worker threads and one lock. Latency (p99) grows accordingly. Spreading
  load over more partitions is the real-world answer.
- **Fsync scales with connections, not with in-flight depth.** 16 connections ≈ 7–8× one
  connection; 16 in flight per connection only adds latency.
- **Idempotence costs little in page-cache mode** (bookkeeping under a per-partition lock).
  In fsync mode it initially capped every partition at ~125 op/s because the idempotence lock
  was held across the flush; fixed by appending under the lock and waiting for durability
  after releasing it (`IdempotentGroupCommitTest`).

### Not comparable: `WriteAheadLog` numbers

`BenchmarkHarness` Scenarios 2–3 (~200K op/s page cache, ~100K op/s "fsync") measure
`storage.WriteAheadLog` in isolation, **which the broker does not use**. Its fsync number is
high because the benchmark submits 10,000 asynchronous appends at once, so each flush covers
thousands of records. Do not quote those as broker numbers.

## Platform note: what `force()` means on macOS vs Linux

On macOS, `fsync(2)` only pushes data to the drive; the drive may still hold it in a volatile
write cache. Only `fcntl(F_FULLFSYNC)` forces the drive to flush. Measured on this machine:

| Call | Latency |
|---|---|
| `fsync(2)` | 0.026 ms |
| `fcntl(F_FULLFSYNC)` | 3.60 ms |
| Java `FileChannel.force(true)` | 3.66 ms |

So the JDK 25 `force()` used here performs a full flush, and the fsync numbers above are
genuinely durable to power loss. On Linux, `fsync`/`fdatasync` issue a cache flush to the
device; latency depends heavily on the drive: datacenter NVMe with power-loss protection
often completes in tens of microseconds, consumer drives in milliseconds. Expect the fsync
tier to scale very differently on Linux server hardware; re-measure before quoting it.

## Other durability-relevant state

### Producer state snapshots (`.producer_state`)

Idempotence state (last 5 sequences + offsets per producer) lives in memory and is
snapshotted every 100 accepted sequences per partition (`IdempotenceManager`), via
`.tmp` + atomic rename (not fsynced).

**Loss window:** up to 99 sequences per partition. After a crash the broker restores the last
snapshot, so a producer that retries a request it had already sent (in that window) can have
it accepted again instead of deduplicated: a **duplicate is possible, loss is not**. A
producer whose next sequence is ahead of the restored state gets `OUT_OF_ORDER_SEQUENCE` and
must bump its epoch (re-init), which resets its sequence to 0. This matches Kafka's
documented behavior; Kafka narrows the window by rebuilding state from the log tail, which
StormLog can't do because its log format doesn't store producer IDs.

### `nextOffset` and compaction (`.partition.meta`)

Offsets are never reissued. Compaction can drop the highest offsets (e.g. a tombstoned
key), so before swapping files `compact()` durably writes the current next offset to
`.partition.meta`; startup uses `max(meta, last index offset + 1)`.

### Compaction crash safety

Compacted files are built and fsynced in `.compact_staging`, which is then atomically renamed
to `.compact_ready`: the commit point. A crash before it rolls back (staging is deleted, the
original files are untouched); after it rolls forward (installation is idempotent and is
re-run on startup). The individual file moves are not atomic as a group, which is exactly why
recovery must never discard `.compact_ready`.

## Summary table

| Mode | Acked write survives process crash | …OS crash / power loss | Throughput shape |
|---|---|---|---|
| Page cache (default) | ✅ | ❌ tail lost, truncated cleanly | ~20K–120K op/s/partition |
| Group-commit fsync | ✅ | ✅ | ~130 op/s per connection, scales with connections |
| Per-message force (removed) | ✅ | ✅ | ~128 op/s total, does not scale |
