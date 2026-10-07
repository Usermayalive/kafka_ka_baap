# StormLog: a Kafka-style log broker, built from scratch in Java

StormLog is a partitioned, append-only log broker in Java 25: segmented storage with group
commit and crash recovery, a pipelined binary protocol on Netty, idempotent producers, consumer
groups with cooperative rebalancing, tiered storage to S3, and an embedded Raft metadata quorum
(Apache Ratis) instead of ZooKeeper. It also has two things Kafka doesn't ship: broker-side
Avro schema enforcement and GDPR crypto-shredding of tagged fields. Every performance number
below is measured through the broker's real request path and states its durability mode, and
each design decision and failure mode is written down in [`docs/`](docs).

## Architecture

```mermaid
flowchart LR
    P[Producers] -->|KafkaFrame / Netty| N
    C[Consumers / groups] -->|KafkaFrame / Netty| N
    subgraph Broker
        N[Netty event loops] --> E[Per-connection<br/>serializing executor<br/>+ backpressure]
        E --> Q[Quotas] --> S[Schema validation<br/>+ PII encryption] --> I[Idempotence]
        I --> PT[Partition<br/>segments + dense index<br/>group-commit fsync]
        PT -->|sealed segments, Zstd| T[(S3 / SeaweedFS)]
        E --> G[Group coordinator<br/>+ __consumer_offsets]
        E --> M[Raft metadata member<br/>Apache Ratis]
        K[(DEK store<br/>RocksDB)] --- S
        O[/metrics, /admin/lag<br/>__audit/] --- E
    end
    M <-->|Raft| M2[Other brokers' quorum members]
```

Each broker embeds a member of the metadata quorum, and the Raft leader acts as the controller.
Metadata (brokers, topics, partition leadership, schemas, erasures) is a replicated state
machine; the data path never touches consensus.

## Quick start

Requires JDK 25. Maven comes with the wrapper.

```bash
# Build and run the tests (119). -Dbuild.dir keeps CLI builds apart from an IDE compiling into target/
./mvnw test -Dbuild.dir=target-cli

# A real 3-broker cluster on one machine: produce, consume, a poison pill to the DLQ,
# kill -9 the controller, failover, restart, metrics. No Docker needed.
./mvnw package -Dbuild.dir=target-cli -DskipTests
./scripts/demo-cluster.sh
```

A recorded run of the demo is in [`docs/demo-transcript.md`](docs/demo-transcript.md).

S3 tiering is tested against [SeaweedFS](https://github.com/seaweedfs/seaweedfs)'s S3 API
(MinIO's community binaries are no longer distributed); any S3-compatible endpoint works.

CLI: `java -cp target-cli/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.cli.StormLogCli --bootstrap HOST:PORT <command>`
with `describe`, `create-topic`, `produce`, `produce-batch`, `consume`, `consume-group`,
`consume-with-dlq`, `commit`, `forget-entity`.

## Benchmarks

### Through the broker (in-process clients, 1 KB records, one partition)

From [`BrokerBenchmark`](src/test/java/com/simplekafka/storage/BrokerBenchmark.java): real Netty
connections into the broker's full produce path. Full matrix and caveats are in
[`docs/durability.md`](docs/durability.md).

| Durability | Connections × in flight | Throughput | p99 |
|---|---|---|---|
| Page cache (default) | 1 × 16 | ~78K–121K op/s | < 1 ms |
| Page cache, idempotent | 16 × 16 | ~57K–70K op/s | ~10–15 ms |
| fsync per acknowledged write | 1 × 1 | ~130 op/s | ~11 ms |
| fsync, idempotent, group commit | 16 × 1 | ~820–920 op/s | ~35 ms |

The fsync numbers are true-to-disk: on macOS, Java's `force()` takes as long as
`F_FULLFSYNC` (3.6 ms on this machine), not plain `fsync` (0.03 ms).

### Against Apache Kafka 4.3.1

![StormLog vs Apache Kafka 4.3.1](docs/img/stormlog-vs-kafka.png)

Same Apple M1 laptop, one broker process each (1 GB heap), one partition, `acks=1`, page-cache
durability (both systems' default), 5 requests in flight, loopback networking. Produced by
[`scripts/bench-vs-kafka.sh`](scripts/bench-vs-kafka.sh); raw data in
[`docs/bench/vs-kafka.csv`](docs/bench/vs-kafka.csv).

Three scenarios, each with the configuration that defines it:

**1. Large records (≥ 4 KB): StormLog matches or beats Kafka on bandwidth.**
Kafka with default producer batching (`linger.ms=5`, `batch.size=16384`) against StormLog (one
record per request). At this size a batch holds one or two records, so batching stops helping and
per-request storage overhead decides.

| Record size | StormLog | Kafka (batched) |
|---|---|---|
| 4 KB | 173 MB/s | 149 MB/s |
| 16 KB | 439 MB/s | 200 MB/s |

**2. Small records with batching: Kafka is about 9× faster, because StormLog's client doesn't batch.**
Same configurations as scenario 1. Kafka's producer packs hundreds of small records into one
request; StormLog's sends one record per request. This is the client's biggest gap.

| Record size | StormLog | Kafka (batched) |
|---|---|---|
| 64 B | 54K rec/s | 479K rec/s |
| 256 B | 53K rec/s | 370K rec/s |
| 1 KB | 48K rec/s | 132K rec/s |

**3. Minimum-latency configuration (no batching on either side): StormLog has sub-millisecond p99.**
Kafka with `batch.size=0`, `linger.ms=0`; both at a fixed 5,000 records/s for 60 s per size, so
neither is saturated and producer startup is under 1% of samples. Latency is measured by each
client from send to acknowledgment; Kafka's tool reports whole milliseconds. No production
Kafka deployment runs unbatched, so this shows per-request path cost, not typical Kafka latency.

| Record size | StormLog p50 / p99 | Kafka, batching off, p50 / p99 |
|---|---|---|
| 64 B | 0.07 / 0.18 ms | 1 / 8 ms |
| 256 B | 0.07 / 0.30 ms | 1 / 18 ms |
| 1 KB | 0.07 / 0.62 ms | 1 / 11 ms |
| 4 KB | 0.08 / 0.59 ms | 1 / 21 ms |
| 16 KB | 0.06 / 0.83 ms | 0 / 16 ms |

At maximum rate with batching off on both sides, StormLog also does about
2.2× Kafka's record rate (it does less per request: no record-batch format, CRC or batch
headers).

Disk writes per payload byte are the same for both (about 1.0 at ≥ 1 KB). Caveats: one
machine, loopback, macOS; Kafka is tuned for Linux and real networks. Both run without
replication in this comparison.

## What's different from Kafka

- **Schema enforcement in the broker.** Avro schemas live in the Raft metadata;
  `BACKWARD`/`FORWARD`/`FULL` checks run atomically inside the state machine, and every record
  on a schema'd topic is validated on the produce path. Kafka delegates this to an external
  registry and trusts clients.
- **GDPR crypto-shredding.** Fields tagged `"pii": true` are AES-256-GCM encrypted under a
  per-entity key before they're written; `FORGET_ENTITY` destroys the key on every broker, so
  the data is unreadable everywhere it was copied (local segments, S3 tiers, backups) without
  rewriting the log. Only SHA-256 hashes of entity IDs are ever persisted. See
  [`docs/gdpr-threat-model.md`](docs/gdpr-threat-model.md).
- **Native dead-letter queues.** The consumer routes poison pills to `<topic>.dlq` with an
  envelope (origin, exception, retry count). If the DLQ write fails, the partition pauses
  instead of skipping data.
- **Atomic producer state.** Idempotence snapshots use temp-file-plus-atomic-rename with a
  versioned format; the documented loss window is up to 99 sequences, so a duplicate is
  possible and data loss is not.
- **Broker-side cooperative group assignment** (in the spirit of KIP-848) with static
  membership on by default.
- **Bounded metrics.** Per-topic and per-(group, topic) series are capped and overflow into
  `__other__`: 100 groups × 100 topics produces 356 series. Per-partition lag is served on
  demand at `/admin/lag`.

## Explicit non-goals

| Non-goal | Why | Consequence |
|---|---|---|
| Replication | Doing it correctly (ISR, high watermark, leader epochs, truncation) is its own project; the original fire-and-forget version silently diverged and was removed | A dead broker's partitions are **offline** until it returns; the controller never elects a replica without the data |
| Distributed rate limiting | Quotas are per broker and per client ID; a cluster-wide budget needs coordination on the hot path | A client spread over N brokers gets up to N × its quota |
| Tiered storage for compacted topics | Compaction rewrites history that's already in S3 | Compacted topics stay local (Kafka has the same limit) |
| TLS, authentication, ACLs | Out of scope for a storage/coordination project | Run only on a trusted network |
| Transactions / exactly-once across partitions | Needs a transaction coordinator and markers in the log | Idempotence is per producer session and partition |
| Producer-side batching | The client sends one record per request | Kafka is much faster for small records (see benchmarks) |

## Known limitations

- **Group lag is partial.** A group's coordinator reports lag only for partitions it hosts.
- **No coordinator failover.** If a group's coordinator broker is down, that group can't commit or rebalance until it returns.
- **Combined-mode controller.** Every broker runs a quorum member, so losing the controller also loses the partitions it leads (see [failure modes](docs/failure-modes.md#correlated-failure-the-controller-also-leads-partitions)); production would use dedicated controller nodes.

## Bugs found and fixed along the way

Each was found by measuring or by a test written to break the feature, and each has a regression test.

| Bug | Effect |
|---|---|
| The broker never started its Netty server; the advertised throughput measured an engine the broker didn't use | Real broker throughput was ~120 op/s, not 200K+ |
| Two `fsync`s per message under the write lock | 7.8 ms per message regardless of concurrency |
| Idempotence lock held across the fsync wait; flush leader held the partition lock during `force()` | Idempotent fsync throughput stuck at ~125 op/s at any concurrency |
| Producer snapshot reloaded from disk on every request | Every idempotent producer rejected after its 101st message per partition |
| Compaction crash window deleted the only copy of the data | Total partition data loss on a crash mid-swap |
| Compaction wrote survivors in key order | Index binary search returned wrong records |
| Tombstone compaction rewound the next offset | Offsets reissued after restart |
| Backpressure watched the shared pool, not the connection | One pipelining client could fill broker memory |
| A pool rejection still ran the task later | Two responses for one request |
| Partitions looked up by list index; recreated on every metadata reload | Wrong partition served on brokers hosting a subset |
| Legacy fetch numbered records sequentially | Consumers mis-positioned after compaction |
| Shutdown of the last quorum member retried forever | Broker shutdown hung for minutes |
| Erasure missed by a broker catching up from a Raft snapshot; raw entity IDs in the Raft log | A "forgotten" person's data still readable; their identifier persisted forever |
| Restart races: heartbeats started after the registration had been applied, registration gave up after one timeout, watch callbacks blocked metadata notifications | A restarted broker could be fenced again, or stay unregistered or with a stale view of the cluster forever |
| An IDE compiling into `target/` emitted class files for code that didn't compile | Maven reported success on broken code (hence `-Dbuild.dir`) |

## Documentation

| Document | Contents |
|---|---|
| [`docs/durability.md`](docs/durability.md) | Durability tiers, what each number means, macOS vs Linux `fsync` |
| [`docs/failure-modes.md`](docs/failure-modes.md) | Failure matrix for every subsystem, including correlated controller failure |
| [`docs/decisions.md`](docs/decisions.md) | 22 architecture decisions with alternatives and trade-offs |
| [`docs/gdpr-threat-model.md`](docs/gdpr-threat-model.md) | Crypto-shredding design, what it protects against and what it doesn't |
| [`docs/demo-transcript.md`](docs/demo-transcript.md) | Recorded run of the cluster demo |

## Layout

```
src/main/java/com/simplekafka/
  broker/        SimpleKafkaBroker, Partition (segments, index, group commit, compaction, tiering hooks)
  network/       Netty server, frame codec, handler, idempotence, quotas, backpressure
  metadata/      Raft state machine, metadata image, quorum, ZooKeeper-compatible façade
  coordinator/   __consumer_offsets store, group coordinator
  tiered/        RemoteStorageManager SPI, S3 and local backends, Zstd codec
  schema/        wire format, Avro validation, PII tagging rules
  gdpr/          CryptoShredder, DEK store (RocksDB), env and AWS KMS master key providers
  observability/ metrics with cardinality limits, audit log, admin HTTP
  client/        client, consumers, group consumer, DLQ router, idempotent producer
  cli/           StormLogCli, StormLogPerf
scripts/         demo-cluster.sh, bench-vs-kafka.sh, plot_vs_kafka.py
```
