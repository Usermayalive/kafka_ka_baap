# my_own_simple_kafka

A distributed messaging system built from scratch in Java, designed to replicate the core architecture of Apache Kafka. 

## Features
- **Custom Binary Protocol**: Uses Java NIO ByteBuffers for efficient zero-copy network communication.
- **Zookeeper Coordination**: Handles broker registration, controller election, and topic metadata syncing across the cluster.
- **Segmented Storage**: Persists data to disk using append-only `.log` segments and sparse `.index` files for O(1) reads.
- **Leader-Follower Replication**: Automatically replicates partition data across multiple brokers for fault tolerance.
- **Producer & Consumer API**: Provides easy-to-use client libraries for routing messages to partitions and polling for new data.

## Getting Started
Ensure Zookeeper is running locally on port 2181, then run:
```bash
mvn clean package
```
Start your brokers, create your topics, and produce/consume away!
