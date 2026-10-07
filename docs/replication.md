# Replication

Each partition has `replicationFactor` replicas on different brokers. One is the leader; the
others are followers that pull from it. The design follows Kafka's (ISR, high watermark,
leader epochs) because its failure semantics are well understood and testable. Code:
[`ReplicaManager`](../src/main/java/com/simplekafka/broker/ReplicaManager.java),
[`LeaderEpochCache`](../src/main/java/com/simplekafka/broker/LeaderEpochCache.java), the
replication methods on [`Partition`](../src/main/java/com/simplekafka/broker/Partition.java);
tests in [`ReplicationTest`](../src/test/java/com/simplekafka/broker/ReplicationTest.java).

## The write path

1. A producer sends to the leader. The leader appends locally (page cache or fsync, per the
   partition's durability mode).
2. Each follower runs a fetcher that repeatedly sends `REPLICA_FETCH(fetchOffset = its log end,
   lastFetchedEpoch)`. The fetch offset acknowledges everything before it. The leader long-polls
   up to 500 ms when there's nothing new, so replication latency is one round trip, not a polling
   interval.
3. The follower appends the records **at the leader's offsets, byte for byte**. PII stays
   encrypted in transit and at rest on every replica.
4. The leader's **high watermark (HW)** is the minimum log end across the in-sync replicas.
   Records below it are *committed*: present on every in-sync replica.
5. `acks=all` (the client default) completes once the HW passes the record's offset;
   `acks=1` completes after the leader's own append.

Consumers fetch from the leader and **never read past the HW**, so a consumer can never see a
record that a failover later removes.

## In-sync replicas (ISR)

The leader tracks when each follower last caught up to its log end. A follower that hasn't
caught up for `stormlog.replica.lag.max.ms` (10 s) leaves the ISR; a follower that reaches the
HW rejoins. The leader proposes these changes as `UPDATE_ISR` through the Raft metadata quorum,
and the state machine rejects a change from anyone but the partition's current leader, so a
deposed leader can't rewrite the ISR.

`min.insync.replicas` (broker setting, default 1): an `acks=all` produce to a partition whose
ISR is smaller is rejected with `NOT_ENOUGH_REPLICAS` rather than silently being less durable.
With RF=3 and `min.insync.replicas=2`, an acknowledged write survives the loss of any one broker
even while another is lagging.

## Failover

When a broker's heartbeats stop, the controller elects, for each partition it led, the first
**live member of the ISR**, and drops dead brokers from the ISR in the same Raft command. The
leader epoch increments. Out-of-sync replicas are never elected ("unclean" election would
silently lose committed records); if no in-sync replica is alive, the partition goes offline
until one returns.

Measured in `ReplicationTest`: a follower is leading within the broker session timeout plus
an election (~3–4 s), with every committed record intact.

A follower learns the HW one fetch after the leader computes it, so a new leader can start with
a HW slightly behind records committed just before the crash. Those records are on its disk and
become visible as soon as the remaining followers fetch from it (milliseconds); a polling
consumer just sees them on its next poll. Kafka behaves the same way.

## Divergence and truncation (leader epochs)

A crashed leader may have records nobody else received (written with `acks=1`, or committed
locally before followers fetched). The new leader writes *different* records at those offsets.
When the old leader returns as a follower, those tails must be cut before it fetches.

Every replica keeps a leader-epoch history: `(epoch, first offset written in that epoch)`.
Leaders add an entry on taking over; followers copy the leader's entries as they replicate. A
follower's fetch carries its latest epoch; if that's older than the leader's, the leader answers
with where that epoch ended *in the leader's log* (the start of the next epoch it knows). If the
follower's log extends past that point, the response tells it to truncate there first.

This is Kafka's KIP-101/279 design. Truncating to the follower's high watermark instead (Kafka's
original approach) can lose committed records or leave two replicas with different records at
the same offset. `ReplicationTest.testRestartedLeaderTruncatesDivergentTailAndRejoins` creates
exactly that situation and checks the replicas end byte-identical.

## Cost

On three broker JVMs on one laptop (1 KB records, one connection, 5 in flight): RF=1 40K
records/s; RF=3 with `acks=1` 16.6K/s; RF=3 with `acks=all` 5.7K/s, bounded by one replication
round trip per request on a connection. Below saturation, `acks=all` p99 is about 2.4 ms. Details
in the README.

## What else replicates

| State | How | After failover |
|---|---|---|
| Partition data | Follower fetch | New leader serves everything committed |
| Consumer offsets | `__consumer_offsets` is replicated (RF = min(3, brokers)); commits wait for the HW | New coordinator rebuilds from the replicated log |
| PII data keys | Wrapped DEKs live in the Raft metadata | New leader decrypts; erasure removes keys cluster-wide |
| Group membership | Not replicated (soft state) | Members get `UNKNOWN_MEMBER` and rejoin |
| Idempotent producer state | Carried in replica fetches, captured together with the leader's log end; followers adopt an entry only once their log covers it and drop entries for truncated records | Retries are deduplicated by the new leader: exactly once across failover (`testIdempotentBatchingProducerSurvivesLeaderCrashWithoutDuplicates`: 3,000 sends, 0 failures, no duplicates) |
| Tiered segments | Only the leader uploads, and only segments fully below the HW | A new leader uploads from its own copy (same object keys) |

## Not built (yet)

- **Preferred-leader rebalancing.** After a failover the old leader returns as a follower, so
  leadership can concentrate on fewer brokers. Kafka periodically moves leadership back to each
  partition's first replica.
- **Batched replica fetch.** One fetch request per partition, not one per broker pair, so a
  broker with many follower partitions sends many concurrent long-polls.
- **Reassignment.** The replica set is fixed at topic creation.
- **Chain replication.** Considered and rejected: it halves the leader's egress at RF=3 but adds
  every hop's latency to every write and needs chain repair on a mid-chain failure.
