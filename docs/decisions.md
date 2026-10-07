# Architecture decision log

One entry per significant decision: what was chosen, what else was considered, and why. Each
entry includes the trade-off it accepted.

### 1. Dense per-record index instead of reusing the sparse-index `WriteAheadLog`
The broker's `Partition` keeps a dense `(offset, position)` index per segment. The
`storage.WriteAheadLog` engine (sparse index, contiguous offsets) was faster in isolation, but
it computes offsets by counting records, so it can't represent the gaps compaction leaves.
Swapping it in would have renumbered offsets on the first compaction and invalidated every
consumer's committed position. Making the WAL offset-aware is a storage-format migration, so the
broker path stayed on `Partition` and got group commit instead (decision 2). Cost: a 16-byte
index entry per record (68× larger than the sparse index for 100K records).

### 2. Group commit instead of a per-message fsync
The original `Partition.append` forced the log and then the index on every message while holding
the write lock: 7.8 ms per message, ~128 op/s regardless of concurrency. The alternatives were
dropping fsync entirely (fast, no durability option) or delegating to the WAL (blocked by
decision 1). The chosen design has the first waiter force once on behalf of everyone appended
since, *outside* the partition lock. Throughput then scales with concurrent connections (16
connections: ~1,350 op/s) while each acknowledged write is still on stable storage.

### 3. Page cache as the default durability
Kafka's default is no fsync, relying on replication for durability, and StormLog follows it
(decision 11): with RF ≥ 2 and `acks=all` an acknowledged write survives one machine's OS crash.
With RF = 1 the default survives process crashes and loses the unflushed tail on OS crash or
power loss; fsync mode is available per broker. Torn tails are
truncated to a clean record boundary on startup, so a crash never yields a corrupt read.

### 4. A compaction commit point (atomic directory rename)
Compaction swaps a log and an index; two file moves can't be atomic together. Rather than a
journal, compacted files are built and fsynced in `.compact_staging`, which is atomically renamed
to `.compact_ready`. Recovery rolls back before the rename and rolls forward after it (the
install is idempotent). This fixed a real data-loss bug: recovery used to delete the staging
directory even after the original files were gone.

### 5. Netty with length-prefixed frames as the only client protocol
The project originally had two stacks: a blocking NIO protocol with unframed 8 KB reads (used by
the actual broker) and a Netty stack (used only in tests). Keeping both would have meant
implementing every Phase 4–6 API twice. The legacy stack was deleted. Correlation IDs allow
pipelining; each connection's requests execute in order (`SerializingExecutor`), which is what
keeps an idempotent producer's sequence numbers valid.

### 6. Per-connection backpressure via `autoRead`
The first attempt watched the shared worker pool's queue, but that queue holds at most one task
per connection, so one client pipelining thousands of requests never triggered it, and its
per-connection queue grew without bound. Backpressure now watches each connection's own backlog
(pause at 256, resume at 64), and lets TCP flow control push back on the client.

### 7. Idempotence bookkeeping outside the fsync wait
Idempotent producers serialize on a per-partition lock. Holding it across the fsync wait capped
every partition at one flush per request (~125 op/s at any concurrency). The append to the page
cache happens under the lock; the durability wait happens after it is released. Duplicates also
wait for durability, because their original may still be unflushed.

### 8. Tiered storage: whole-segment upload with an LRU read cache
Ranged reads from S3 would avoid downloading whole segments, but segments are 1 MB and a
consumer catching up reads them sequentially anyway. Whole-segment Zstd uploads keep the remote
format trivial (two objects per segment) and let cold reads reuse the local read path. Uploads
are asynchronous on roll; local copies are deleted only after upload; failed uploads retry on
startup. Compacted topics are excluded, because compaction rewrites history (Kafka has the same
restriction).

### 9. Consumer offsets in a compacted internal topic
Offsets could have gone into the Raft metadata, but commit rates (every batch, every consumer)
would flood the consensus log. `__consumer_offsets` is an ordinary compacted topic, partitioned
by group, so commit load spreads across brokers and the group's coordinator is simply that
partition's leader. The in-memory view is rebuilt by replaying the partition.

### 10. Broker-side cooperative assignment (KIP-848 style)
The classic Kafka protocol computes assignments on a client leader with stop-the-world
rebalances; the eager variant revokes everything. The broker here computes a sticky, balanced
target and grants a partition only after its previous owner reports releasing it. Members keep
untouched partitions through a rebalance, and the assignment logic lives in one place. Static
membership is the client default, so rolling restarts don't trigger rebalances.

### 11. Pull replication with ISR and leader epochs (not chain replication)
The original fire-and-forget replication appended to followers without offsets and silently
diverged, so it was removed and later rebuilt properly. Followers pull from the leader (Kafka's
model); the leader's high watermark is the minimum log end across the in-sync replicas, and only
in-sync replicas are eligible for election. Chain replication (leader → F1 → F2) was considered:
it halves leader egress at RF=3, but every write pays every hop's latency and a mid-chain failure
needs chain repair. Divergence after failover is resolved with leader-epoch history (KIP-101/279);
truncating to the high watermark, Kafka's original design, can lose committed data.

### 12. Embedded Raft (Apache Ratis) instead of ZooKeeper or a hand-written Raft
ZooKeeper is an external dependency with its own operational surface, and Kafka removed it for
the same reason (KRaft). Writing Raft from scratch is a project of its own and a classic source
of subtle bugs. Ratis provides a tested log, elections, snapshots and membership changes, while
the state machine, the commands and the broker integration are StormLog's own code.

### 13. Typed, deterministic metadata commands
Commands (`REGISTER_BROKER`, `CREATE_TOPIC`, `ELECT_LEADER`, …) are applied identically on
every node, so anything nondeterministic (partition placement, timestamps) is decided by the
proposer and carried in the command. Validation that must be atomic with the write (schema
compatibility, duplicate topics) runs inside apply, so concurrent proposals can't race past it.

### 14. A ZooKeeper-compatible façade over Raft
`MetadataLog` exposes `ZookeeperClient`'s exact methods, so the broker's call sites didn't
change when ZooKeeper was removed. Path writes map to typed commands; the multi-step topic
creation is buffered into one atomic `CREATE_TOPIC`. The façade is transitional by design: new
code (leader election, schemas) uses typed calls directly.

### 15. Broker liveness as leader-local soft state
ZooKeeper's ephemeral nodes were replaced by heartbeats served by the Raft leader and kept in
memory. Replicating each heartbeat would add a log entry per broker per 500 ms. A new leader
grants every registered broker a full session timeout before fencing it, which avoids
mass-fencing on failover. This matches KRaft.

### 16. Read-your-writes on followers
`submit` returns when the leader commits, but a follower that proposed the change may not have
applied it yet. A schema registered through a follower was "not visible" in the response. Every
submit now waits until the local state machine has applied the reply's log index.

### 17. Schemas in Raft, compatibility checked in apply
A separate schema registry service would add another moving part and another consistency
boundary. Storing schemas in the metadata image gives every broker the same view, invalidates the
local parsed-schema cache through state-machine notifications, and lets compatibility checks run
atomically with registration.

### 18. Avro, with an in-band schema ID header
Avro has a mature compatibility checker (`SchemaCompatibility`) with precise error reporting,
which produces the "rejected with a reason" behavior. The `[magic][schema_id][len]` header lets
the broker validate without a registry round trip and lets consumers decode any record with one
cached lookup per schema ID.

### 19. Broker-side field encryption for crypto-shredding
Client-side encryption would keep the broker out of the trust boundary but requires distributing
keys to every producer and consumer. Encrypting on the leader keeps clients unchanged and
protects data at rest everywhere it goes: local segments, S3 tiers and backups. The threat model
states plainly that a compromised broker sees plaintext.

### 20. One KMS key per entity, hashed identifiers, key generations
With a shared master key, deleting a wrapped DEK doesn't revoke stale copies (backups,
un-compacted RocksDB files). Per-entity KMS keys make erasure irrevocable, at about $1 per key per
month. Entities are referenced only by SHA-256 so erasure doesn't persist the identifier in the
Raft log, KMS, or the audit trail. Keys are versioned by erasure count, so data produced after an
erasure is readable while older data stays erased.

### 21a. Replicated data keys
Per-broker data keys broke under replication: a follower holds the leader's ciphertext, so after
failover it couldn't decrypt. Wrapped DEKs moved into the Raft metadata (first proposal wins), so
every replica uses the same key and erasure removes it cluster-wide. Cost: wrapped keys (about
60 bytes per entity and generation) live in every node's metadata snapshot, and stale Raft log
segments hold them until purged; per-entity KMS keys keep erasure irrevocable regardless.

### 21. Bounded metric cardinality
Topic and group names are chosen by clients, so per-label series are unbounded input to the
monitoring system. Each labeled family is capped and overflows into `__other__`; per-partition
detail is served on demand from `/admin/lag` instead of being scraped continuously.

### 22. Benchmarks only through the broker's real path
Early numbers (~220K op/s) measured `WriteAheadLog`, which the broker never ran; the broker was
doing ~120 op/s. Published numbers now come from `BrokerBenchmark` (real Netty connections
into the broker path) and from `bench-vs-kafka.sh` (separate broker processes, the same
workload for Kafka), and each states its durability mode.
