# Demo: a 3-broker cluster surviving a controller crash

Recorded output of [`scripts/demo-cluster.sh`](../scripts/demo-cluster.sh), unedited apart from
removing terminal colors and the shell's "Killed: 9" notice. Three broker JVMs on one machine
form an embedded Raft metadata quorum; the script then runs, in order:

1. **Topic creation:** `orders` gets 3 partitions, one led by each broker (plus the
   auto-provisioned `orders.dlq`).
2. **Produce and consume:** 30 messages; a consumer group reads all of them and commits.
3. **Poison pill:** an unparseable record is retried, then dead-lettered with its origin and
   exception, and consumption continues past it.
4. **`kill -9` of the controller:** the Raft leader, which also leads a partition, is killed.
   A new controller is elected; after the 3 s session timeout the dead broker's partition is
   marked `OFFLINE`. Producing to the other partitions keeps working; the 3 sends routed to the
   offline partition fail, as they must without replication.
5. **Restart:** the broker catches up on the metadata log, re-registers, is re-elected
   leader of its partition, and serves all of its pre-crash data (`orders-0: 12 messages`).
6. **Observability:** committed-offset lag from the group's coordinator, and Prometheus metrics.

Across 8 consecutive runs (killing each broker at least twice), every run recovered every
partition with all its data.

```text

==> Starting 3 brokers (Raft quorum 1@127.0.0.1:19191,2@127.0.0.1:19192,3@127.0.0.1:19193)
{"self":1,"leader":1,"voters":[1,2,3],"learners":[]}

==> Creating topic 'orders' with 3 partitions (one per broker)
Created orders with 3 partitions
Brokers: [BrokerInfo{id=1, host='127.0.0.1', port=19092}, BrokerInfo{id=2, host='127.0.0.1', port=19093}, BrokerInfo{id=3, host='127.0.0.1', port=19094}]
  orders: p0->broker1 p1->broker2 p2->broker3
  orders.dlq: p0->broker1

==> Producing 30 messages
Produced 30/30 to orders
orders-0@0  message-0
orders-0@1  message-3
orders-0@2  message-6
  ...

==> Consumer group 'billing' reads everything and commits
Group billing member owned [TopicPartition[topic=orders, partition=0], TopicPartition[topic=orders, partition=1], TopicPartition[topic=orders, partition=2]], received 30

==> Poison pill: one unparseable record is dead-lettered; consumption continues past it
Processed 11 records; consumer position 12 (not stuck behind the poison pill)
orders.dlq@0  from orders-0@10  java.lang.IllegalArgumentException: Unparseable order at offset 10: POISON {not json  payload="POISON {not json"

==> kill -9 broker 1 (the controller AND a partition leader)
{"self":2,"leader":3,"voters":[1,2,3],"learners":[]}
Brokers: [BrokerInfo{id=2, host='127.0.0.1', port=19093}, BrokerInfo{id=3, host='127.0.0.1', port=19094}]
  orders: p0->OFFLINE p1->broker2 p2->broker3
  orders.dlq: p0->OFFLINE

==> Producing 9 more messages: the offline partition fails, the others keep working
Produced 6/9 to orders (3 failed: partition offline)

==> Restarting broker 1: it re-registers and is re-elected leader of its partition
Brokers: [BrokerInfo{id=1, host='127.0.0.1', port=19092}, BrokerInfo{id=2, host='127.0.0.1', port=19093}, BrokerInfo{id=3, host='127.0.0.1', port=19094}]
  orders: p0->broker1 p1->broker2 p2->broker3
  orders.dlq: p0->broker1
  orders-0: 12 messages
  orders-1: 13 messages
  orders-2: 13 messages

==> Observability: committed-offset lag (reported by the group's coordinator, for partitions it hosts)
broker@19092: [{"group":"billing","topic":"orders","partition":0,"committedOffset":10,"logEndOffset":12,"lag":2}]

==> Metrics on broker at port 19093
stormlog_request_latency_seconds_count{api="produce",broker="2",outcome="ok"} 13
stormlog_topic_records_in_total{broker="2",topic="orders"} 13.0

==> Done. Broker logs: demo-data/broker-*.log
```

## Recording a video walkthrough

The plan calls for a ~5-minute screen recording. This repository can't produce one by itself;
this is the shot list the script was built around:

| Time | Shot | Command |
|---|---|---|
| 0:00 | Architecture diagram (README) and what's being shown | — |
| 0:30 | Start the cluster; show the quorum | `./scripts/demo-cluster.sh` |
| 1:00 | Topic layout, produce, group consume | (script) |
| 1:45 | Poison pill routed to the DLQ, consumer keeps going | (script) |
| 2:15 | `kill -9` the controller: new leader, partition offline, others still accepting writes | (script) |
| 3:00 | Restart: re-registration, partition back online with its data | (script) |
| 3:30 | Metrics: `stormlog_topic_records_in_total`, `stormlog_group_lag_records` | `curl localhost:20092/metrics` (point any Prometheus at it) |
| 4:00 | GDPR: register a PII schema, produce, `forget-entity`, fetch shows `[REDACTED]` | `StormLogCli ... forget-entity u-123` |
| 4:30 | Benchmark chart vs Kafka and how to read it | README |
