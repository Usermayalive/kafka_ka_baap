# Failure-mode analysis

For each subsystem: what can fail, how it's detected, what users see, how it recovers, and
which test exercises it. "Untested" means the behavior follows from the code but no automated
test forces that failure.

Durability semantics are in [durability.md](durability.md); erasure semantics are in
[gdpr-threat-model.md](gdpr-threat-model.md).

## Phase 1: Partition storage (`Partition`)

| Failure | Detection | Impact | Recovery | Test |
|---|---|---|---|---|
| Broker process crash (page-cache mode) | — | None for acknowledged writes: they're in the kernel page cache | Restart reads them back | `ClusterTest` (crash + restart serves all pre-crash records) |
| OS crash / power loss (page-cache mode) | Startup scan | Unflushed tail lost; log and index tails may diverge | `truncateTornTail` drops index entries past the log end and log bytes without an index entry; the lost offsets are reassigned | `PartitionDurabilityTest.testRecoveryTruncates*`, `testRecoveryDropsIndexEntries*` |
| OS crash / power loss (fsync mode) | — | None for acknowledged writes | — | Group commit: `PartitionDurabilityTest.testGroupCommit*` |
| `force()` fails (I/O error) | Exception in flush leader | Waiters get the error; the records are in the page cache but unacknowledged | Producer retries (idempotent producers dedupe) | Untested |
| Segment roll during an in-flight group flush | `ClosedChannelException` in the flush leader | None: rolls force before closing in fsync mode | — | `testGroupCommitSurvivesSegmentRoll` |
| Disk full | `IOException` on append | Produce fails; reads unaffected | Free space | Untested |
| Crash mid-compaction, before the commit point | `.compact_staging` present at startup | None: original files untouched | Staging deleted (roll back) | `testCompactionCrashBeforeCommitPointRollsBack` |
| Crash mid-compaction, after the commit point | `.compact_ready` present at startup | None | Installation re-run (roll forward, idempotent) | `testCompactionCrashAfterCommitPointRollsForward` |
| Compaction removes the highest offsets (tombstones) | — | Would rewind `nextOffset` and reissue offsets | `.partition.meta` persisted before the swap; startup uses `max(meta, last index + 1)` | `testTombstoneCompactionNeverReissuesOffsets*` |

## Phase 2: Client-side error handling (DLQ)

| Failure | Detection | Impact | Recovery | Test |
|---|---|---|---|---|
| Poison-pill record (handler always throws) | Retry policy exhausts | Record routed to `<topic>.dlq` with an envelope; consumption continues | Inspect/replay from the DLQ | `DeadLetterQueueTest` |
| DLQ produce fails | Router result `isFatalDlqProduceError` | Consumer **pauses**, cursor rewound to the poison pill, committed offset does not advance | Resume after the DLQ recovers | `DeadLetterQueueTest` (offset safety) |
| Transient handler error | Exception classification | Retried with backoff, not dead-lettered | — | `RetryPolicyTest` |

## Phase 3: Networking and idempotence

| Failure | Detection | Impact | Recovery | Test |
|---|---|---|---|---|
| Producer retry of an acknowledged request | Sequence within the 5-entry window | Original offset returned, no duplicate append | — | `Phase3…Test` (duplicate cases) |
| Sequence gap (lost request) | `seq > last + 1` | `OUT_OF_ORDER_SEQUENCE` | Producer bumps its epoch (resets to 0) | `Phase3…Test` |
| Zombie producer (old epoch) | `epoch < current` | `PRODUCER_FENCED` | — | `Phase3…Test` |
| Broker crash: producer state loss window | Restored from `.producer_state` (every 100 sequences) | Up to 99 recent sequences forgotten: a retry in that window can be **duplicated**, never lost | Producer epoch bump on `OUT_OF_ORDER` | Snapshot restore tested; window documented |
| One client pipelines faster than the broker processes | Per-connection queue ≥ 256 | Broker stops reading the socket; TCP backpressure blocks the client | Resumes at ≤ 64 queued | `ConnectionBackpressureTest` |
| Shared worker pool saturated | `RejectedExecutionException` | `BROKER_OVERLOADED`, exactly one response per request | Client retry | `ConnectionBackpressureTest.testRejectedTaskNeverRuns` |
| Client exceeds quota | Token bucket | `QUOTA_EXCEEDED` with a throttle hint | Client backs off | `Phase3…Test` |
| Oversized frame (> 16 MB) | Frame decoder | Connection closed | — | `testOversizedFrameIsClosed` |

## Phase 4: Tiered storage, offsets, consumer groups, observability

| Failure | Detection | Impact | Recovery | Test |
|---|---|---|---|---|
| S3 unavailable during upload | Upload future fails | Segment stays local (nothing is deleted before upload) | Retried on next broker start | `TieredStorageTest.testSealedSegmentNotUploadedBeforeCrash…` |
| S3 unavailable during a cold read | Download fails | That fetch fails; hot (local) data unaffected | Client retries | Untested |
| Crash between upload and manifest write | Segment uploaded but not in `.remote_segments` | Re-uploaded on restart (idempotent overwrite) | — | Follows from startup re-upload |
| Coordinator broker down | `FIND_COORDINATOR` → new leader of the offsets partition | Groups mapped to it rejoin (membership is soft state); committed offsets survive | New coordinator rebuilds offsets from the replicated `__consumer_offsets` | `ReplicationTest.testCommittedOffsetsSurviveCoordinatorCrash` |
| Consumer crashes (no leave) | Session timeout (heartbeats stop) | Its partitions are unassigned until the timeout, then reassigned | Automatic | `GroupConsumerTest.testCrashedMember…` |
| Consumer restarts quickly (static member) | Same instance ID within the timeout | No rebalance; same assignment | — | `testStaticMemberRestart…` |
| Rebalance | Generation bump | Only moved partitions pause; the old owner commits before releasing | Cooperative handoff | `testThirdMemberTriggersStickyCooperativeRebalance` |
| Metrics label explosion (clients create many topics/groups) | `CardinalityLimiter` | Excess label values aggregate into `__other__` | — | `ObservabilityTest` (356 series for 100 × 100) |
| Lag for partitions on other brokers | — | **Limitation:** a coordinator reports lag only for partitions it hosts | — | Documented |

## Phase 5: Raft metadata quorum

| Failure | Detection | Impact | Recovery | Test |
|---|---|---|---|---|
| Metadata leader (controller) dies | Raft election timeout (400–800 ms) | Metadata changes pause; existing partitions keep serving produce and fetch | New leader in ~1.1 s (3 nodes, measured) | `MetadataQuorumTest.testThreeNodesElect…` |
| Broker dies silently | Missed heartbeats > 3 s at the leader | Unregistered; its partitions marked offline (leader −1) | Re-registers on restart and is re-elected leader of its partitions | `ClusterTest.testClusterSurvivesBrokerCrash…` |
| Restarted ex-leader with a divergent log | Leader reports `INCONSISTENCY` and re-sends from the divergence point | Catch-up can take 10+ s (the leader's retry backoff grew while the node was down); the broker doesn't serve until its registration applies locally | Registration retries with backoff; heartbeats start *before* registering, so the node isn't fenced while catching up; clients retry leadership errors for 30 s | `demo-cluster.sh` (repeated runs); `ClusterTest` |
| Broker fenced while still alive (GC pause, network blip) | It sees itself missing from the broker set | Its partitions go offline until it returns | Re-registers itself automatically (as ZooKeeper clients re-create ephemeral nodes) | `ClusterTest.testBrokerFencedWhileAliveReRegistersItself` |
| Slow controller reaction blocks metadata notifications | — | Previously, a broker could keep a stale broker list forever | Watch callbacks run on a dedicated thread and re-read the image; broker lists come from the image | Demo runs |
| Majority of the quorum down | Proposals time out | No metadata changes (no topic creation, no failover); data path for live leaders continues | Restore a majority | Untested (follows from Raft) |
| Network partition: minority side | Can't reach a majority | The minority can't elect a leader or commit; no split brain | Heal | Untested (Raft guarantee) |
| Every node restarts | — | — | Snapshot restored, then log tail replayed | `testStateSurvivesRestartOfEveryNode…` |
| New node joins | Starts as a non-voting learner | None until promoted | Promotion once caught up | `testLearnerCatchesUp…` |
| Promotion to an even voter count | Guard in `promote` | Refused (`409` via admin) | Promote two at once | `testLearnerCatchesUpAndEvenQuorumPromotionIsRefused` |
| Last quorum member shuts down | Unregister can't commit | — | Bounded 2 s wait (it used to hang for minutes) | `ClusterTest` teardown |
| Partition leader dies, RF ≥ 2 | Controller after session timeout | Brief unavailability; then a live **in-sync** follower leads with every committed record | Automatic failover (leader epoch +1, dead broker leaves the ISR) | `ReplicationTest.testReplicasConverge…` |
| Partition leader dies, RF = 1 (or no live ISR member) | Controller after session timeout | **Partition offline** until an in-sync replica returns; no unclean election | The broker returns with its data | `ClusterTest` |
| Ex-leader returns with unreplicated records | Leader-epoch comparison on its first fetch | Its divergent tail would conflict with the new leader's records | Truncates to where its epoch ended on the new leader, then catches up and rejoins the ISR | `testRestartedLeaderTruncatesDivergentTailAndRejoins` |
| Follower falls behind (slow disk, GC, partition) | Not caught up for 10 s | Leaves the ISR; `acks=all` stops waiting for it | Rejoins on reaching the high watermark | `testMinInsyncReplicas…` |
| ISR smaller than `min.insync.replicas` | Produce path | `acks=all` rejected with `NOT_ENOUGH_REPLICAS` (`acks=1` still accepted) | Followers rejoin | `testMinInsyncReplicas…` |
| Records written but not yet on every in-sync replica | High watermark | Invisible to consumers until committed; never served if later truncated | — | `testConsumersOnlySeeCommittedRecords` |

### Correlated failure: the controller also leads partitions

Every broker embeds a quorum member (KRaft "combined mode"), so the Raft leader (the
controller) is also an ordinary broker leading partitions. When that machine dies, two things
fail at once:

1. **Metadata leadership.** For the ~1 s election, nothing can change cluster metadata. The
   failure-handling path itself (unregistering the dead broker, marking its partitions offline)
   can't start until the new controller is elected, so detection takes the election plus the 3 s
   session timeout: **~2.8 s measured** in `ClusterTest` before the dead broker's partitions are
   marked offline.
2. **Its partitions.** With RF ≥ 2 they fail over to in-sync followers once the new controller
   acts; with RF = 1 they stay offline until the broker returns.

**Mitigation (production):** run dedicated controller nodes, i.e. quorum members that host no
partitions (KRaft's `process.roles=controller`). A broker failure is then never a controller
failure, and a controller failure never takes data offline. StormLog's `MetadataQuorum` already
runs independently of the data path, so the change is to start it without a Netty server or
partitions. It isn't packaged as a mode.

## Phase 6: Schemas and GDPR

| Failure | Detection | Impact | Recovery | Test |
|---|---|---|---|---|
| Producer sends unframed/garbage/unregistered-schema data | Validation on the leader | `SCHEMA_VIOLATION` with the reason; nothing reaches the log | Fix the producer | `SchemaAndGdprTest.testInvalidRecords…` |
| Incompatible schema registration | Avro compatibility check inside Raft apply | `INCOMPATIBLE_SCHEMA` with details; concurrent registrations can't race past it | — | `testBackwardCompatibilityRejects…` |
| Registration via a follower | — | Was "not yet visible" | `submit` waits until applied locally (read-your-writes) | `ClusterTest.testSchemaRegistered…` |
| No master key configured, PII topic | Produce path | Produce rejected with a clear error | Configure a provider | `testPiiSchemaRulesAndMissingKeyProvider` |
| Broker down during an erasure | Erasure counts in the replicated image | Would have kept the key and decrypted | Reconcile on startup and on snapshot load; decrypt checks the replicated generation; no serving until caught up | `testErasureLearnedFromSnapshotOnly…` |
| Crash between key deletion and the KMS call | Completion marker missing | KMS key not yet scheduled for deletion | Reconciliation repeats the erasure | Follows from idempotent `forget` |
| KMS unavailable | API error | Encrypt/decrypt of uncached keys fails; cached DEKs keep working | KMS recovers | Untested |
| DEK store lost or corrupted | Unwrap fails | All PII on that broker reads `[REDACTED]`: **the same effect as erasure** | Back up the DEK store separately from segments | Untested |
