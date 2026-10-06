package com.simplekafka.metadata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.*;

/**
 * The cluster metadata as of some Raft log index: registered brokers, topics with their
 * partition leadership, and (Phase 6) topic schemas. Mutated only by {@link #apply}, from the
 * Raft apply thread; readers get copies.
 */
public class MetadataImage {
    private static final Gson GSON = new GsonBuilder().create();

    public record BrokerRegistration(int id, String host, int port, long incarnation) {}

    /** leader == -1: offline. leaderEpoch increments on every election. */
    public record PartitionState(int leader, List<Integer> replicas, List<Integer> isr, int leaderEpoch) {}

    public record TopicImage(String name, Map<String, String> config, List<PartitionState> partitions) {}

    public record SchemaVersion(int id, int version, String schema) {}

    /** What an applied command changed, for watchers. */
    public enum Change { BROKERS, TOPICS, PARTITION_STATE, SCHEMAS, NONE }

    private final TreeMap<Integer, BrokerRegistration> brokers = new TreeMap<>();
    private final TreeMap<String, TopicImage> topics = new TreeMap<>();
    private final TreeMap<String, List<SchemaVersion>> schemas = new TreeMap<>();
    private final TreeMap<String, String> schemaCompatibility = new TreeMap<>();
    private int nextSchemaId = 1;

    /** Applies a command; returns what changed, or throws IllegalStateException if rejected. */
    public synchronized Change apply(MetadataCommand c) {
        switch (c.type) {
            case REGISTER_BROKER -> {
                brokers.put(c.brokerId, new BrokerRegistration(c.brokerId, c.host, c.port, c.incarnation));
                return Change.BROKERS;
            }
            case UNREGISTER_BROKER -> {
                BrokerRegistration current = brokers.get(c.brokerId);
                // Stale unregister (the broker already re-registered with a newer incarnation) is a no-op
                if (current == null || (c.incarnation != null && current.incarnation() != c.incarnation)) {
                    return Change.NONE;
                }
                brokers.remove(c.brokerId);
                return Change.BROKERS;
            }
            case CREATE_TOPIC -> {
                if (topics.containsKey(c.topic)) {
                    throw new IllegalStateException("Topic already exists: " + c.topic);
                }
                topics.put(c.topic, new TopicImage(c.topic, new TreeMap<>(c.config), List.copyOf(c.partitions)));
                return Change.TOPICS;
            }
            case UPDATE_ISR -> {
                TopicImage t = topic(c.topic);
                PartitionState p = t.partitions().get(c.partition);
                replacePartition(t, c.partition, new PartitionState(p.leader(), p.replicas(), List.copyOf(c.isr), p.leaderEpoch()));
                return Change.PARTITION_STATE;
            }
            case ELECT_LEADER -> {
                TopicImage t = topic(c.topic);
                PartitionState p = t.partitions().get(c.partition);
                if (p.leader() == c.leader) {
                    return Change.NONE;
                }
                replacePartition(t, c.partition, new PartitionState(c.leader, p.replicas(), p.isr(), p.leaderEpoch() + 1));
                return Change.PARTITION_STATE;
            }
            case REGISTER_SCHEMA -> {
                List<SchemaVersion> versions = schemas.computeIfAbsent(c.topic, k -> new ArrayList<>());
                for (SchemaVersion v : versions) {
                    if (v.schema().equals(c.schema)) {
                        return Change.NONE; // idempotent re-registration
                    }
                }
                versions.add(new SchemaVersion(nextSchemaId++, versions.size() + 1, c.schema));
                return Change.SCHEMAS;
            }
            case SET_SCHEMA_COMPATIBILITY -> {
                schemaCompatibility.put(c.topic, c.compatibility);
                return Change.SCHEMAS;
            }
            default -> throw new IllegalStateException("Unknown command " + c.type);
        }
    }

    private TopicImage topic(String name) {
        TopicImage t = topics.get(name);
        if (t == null) {
            throw new IllegalStateException("Unknown topic: " + name);
        }
        return t;
    }

    private void replacePartition(TopicImage t, int partition, PartitionState state) {
        List<PartitionState> partitions = new ArrayList<>(t.partitions());
        partitions.set(partition, state);
        topics.put(t.name(), new TopicImage(t.name(), t.config(), List.copyOf(partitions)));
    }

    public synchronized Map<Integer, BrokerRegistration> brokers() {
        return new TreeMap<>(brokers);
    }

    public synchronized Map<String, TopicImage> topics() {
        return new TreeMap<>(topics);
    }

    public synchronized TopicImage topicImage(String name) {
        return topics.get(name);
    }

    public synchronized List<SchemaVersion> schemas(String topic) {
        return List.copyOf(schemas.getOrDefault(topic, List.of()));
    }

    public synchronized SchemaVersion schemaById(int id) {
        for (List<SchemaVersion> versions : schemas.values()) {
            for (SchemaVersion v : versions) {
                if (v.id() == id) {
                    return v;
                }
            }
        }
        return null;
    }

    public synchronized String schemaCompatibility(String topic) {
        return schemaCompatibility.get(topic);
    }

    // ---- snapshots ----

    private record Snapshot(List<BrokerRegistration> brokers, List<TopicImage> topics,
                            Map<String, List<SchemaVersion>> schemas, Map<String, String> schemaCompatibility,
                            int nextSchemaId) {}

    public synchronized String toJson() {
        return GSON.toJson(new Snapshot(new ArrayList<>(brokers.values()), new ArrayList<>(topics.values()),
                schemas, schemaCompatibility, nextSchemaId));
    }

    public synchronized void loadJson(String json) {
        Snapshot s = GSON.fromJson(json, Snapshot.class);
        brokers.clear();
        topics.clear();
        schemas.clear();
        schemaCompatibility.clear();
        s.brokers().forEach(b -> brokers.put(b.id(), b));
        s.topics().forEach(t -> topics.put(t.name(), t));
        if (s.schemas() != null) {
            s.schemas().forEach((k, v) -> schemas.put(k, new ArrayList<>(v)));
        }
        if (s.schemaCompatibility() != null) {
            schemaCompatibility.putAll(s.schemaCompatibility());
        }
        nextSchemaId = s.nextSchemaId() == 0 ? 1 : s.nextSchemaId();
    }
}
