# Demo: a replicated 3-broker cluster surviving a controller crash

Recorded output of [`scripts/demo-cluster.sh`](../scripts/demo-cluster.sh), unedited apart from
removing terminal colors and the shell's "Killed: 9" notice. Three broker JVMs on one machine
form an embedded Raft metadata quorum; the script then:

1. **Creates `orders`**: 3 partitions with replication factor 3, each partition led by a
   different broker and replicated to the other two (plus the auto-provisioned `orders.dlq`).
2. **Produces and consumes** 30 messages; a consumer group reads all of them and commits.
3. **Dead-letters a poison pill**: an unparseable record is retried, then routed to
   `orders.dlq` with its origin and exception, and consumption continues past it.
4. **`kill -9`s the controller**, the Raft leader that also leads a partition. After the broker
   session timeout, leadership of its partitions moves to in-sync followers: no partition goes
   offline, every committed record is still readable, and all new writes succeed.
5. **Restarts the broker.** It re-registers, truncates anything uncommitted (leader-epoch
   comparison), catches up as a follower and rejoins the ISR.
6. **Observability**: committed-offset lag from the group's coordinator, and Prometheus metrics.

```text

==> Starting 3 brokers (Raft quorum 1@127.0.0.1:19191,2@127.0.0.1:19192,3@127.0.0.1:19193)
{"self":1,"leader":2,"voters":[1,2,3],"learners":[]}

==> Creating topic 'orders': 3 partitions, replication factor 3 (each led by a different broker)
Created orders with 3 partitions, replication factor 3
Brokers: [BrokerInfo{id=1, host='127.0.0.1', port=19092}, BrokerInfo{id=2, host='127.0.0.1', port=19093}, BrokerInfo{id=3, host='127.0.0.1', port=19094}]
  orders: p0->broker1 (replicas [1, 2, 3]) p1->broker2 (replicas [2, 3, 1]) p2->broker3 (replicas [3, 1, 2])
  orders.dlq: p0->broker1 (replicas [1, 2, 3])

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

==> kill -9 broker 2 (the controller AND a partition leader)
{"self":1,"leader":1,"voters":[1,2,3],"learners":[]}
Brokers: [BrokerInfo{id=1, host='127.0.0.1', port=19092}, BrokerInfo{id=3, host='127.0.0.1', port=19094}]
  orders: p0->broker1 (replicas [1, 2, 3]) p1->broker3 (replicas [2, 3, 1]) p2->broker3 (replicas [3, 1, 2])
  orders.dlq: p0->broker1 (replicas [1, 2, 3])
  committed records before the crash: 32, readable after failover: 32

==> Producing 9 more messages: every partition has a live leader, so all succeed
Produced 9/9 to orders

==> Restarting broker 2: it re-registers, truncates anything uncommitted, and catches up as a follower
Brokers: [BrokerInfo{id=1, host='127.0.0.1', port=19092}, BrokerInfo{id=2, host='127.0.0.1', port=19093}, BrokerInfo{id=3, host='127.0.0.1', port=19094}]
  orders: p0->broker1 (replicas [1, 2, 3]) p1->broker3 (replicas [2, 3, 1]) p2->broker3 (replicas [3, 1, 2])
  orders.dlq: p0->broker1 (replicas [1, 2, 3])
  orders-0: 15 messages
  orders-1: 13 messages
  orders-2: 13 messages

==> Observability: committed-offset lag (reported by the group's coordinator, for partitions it hosts)
broker@19092: [{"group":"billing","topic":"orders","partition":0,"committedOffset":10,"logEndOffset":15,"lag":5},{"group":"billing","topic":"orders","partition":1,"committedOffset":10,"logEndOffset":13,"lag":3},{"group":"billing","topic":"orders","partition":2,"committedOffset":10,"logEndOffset":13,"lag":3}]

==> Metrics on broker at port 19092
stormlog_request_latency_seconds_count{api="produce",broker="1",outcome="ok"} 16
stormlog_topic_records_in_total{broker="1",topic="orders"} 15.0
stormlog_topic_records_in_total{broker="1",topic="orders.dlq"} 1.0

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
| 2:15 | `kill -9` the controller: leadership fails over to in-sync followers, nothing lost, writes continue | (script) |
| 3:00 | Restart: truncation of anything uncommitted, catch-up, ISR rejoin | (script) |
| 3:30 | Metrics: `stormlog_topic_records_in_total`, `stormlog_group_lag_records` | `curl localhost:20092/metrics` (point any Prometheus at it) |
| 4:00 | GDPR: register a PII schema, produce, `forget-entity`, fetch shows `[REDACTED]` | `StormLogCli ... forget-entity u-123` |
| 4:30 | Benchmark chart vs Kafka and how to read it | README |
