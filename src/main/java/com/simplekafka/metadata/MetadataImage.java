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
    public enum Change { BROKERS, TOPICS, PARTITION_STATE, SCHEMAS, ENTITY_FORGOTTEN, NONE }

    /** BACKWARD: new schema reads data written with the latest. FORWARD: the latest reads new data. */
    public enum Compatibility { NONE, BACKWARD, FORWARD, FULL }

    public static final Compatibility DEFAULT_COMPATIBILITY = Compatibility.BACKWARD;

    private final TreeMap<Integer, BrokerRegistration> brokers = new TreeMap<>();
    private final TreeMap<String, TopicImage> topics = new TreeMap<>();
    private final TreeMap<String, List<SchemaVersion>> schemas = new TreeMap<>();
    private final TreeMap<String, String> schemaCompatibility = new TreeMap<>();
    private int nextSchemaId = 1;
    /**
     * Erasure count per hashed entity key (its current key generation). Part of snapshots, so a
     * broker catching up from one still learns of erasures it never saw as commands.
     */
    private final TreeMap<String, Integer> erasures = new TreeMap<>();

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
                org.apache.avro.Schema candidate;
                try {
                    candidate = new org.apache.avro.Schema.Parser().parse(c.schema);
                } catch (RuntimeException e) {
                    throw new IllegalStateException("Invalid Avro schema: " + e.getMessage());
                }
                String piiViolation = com.simplekafka.schema.PiiSchemaRules.violation(candidate);
                if (piiViolation != null) {
                    throw new IllegalStateException(piiViolation);
                }
                List<SchemaVersion> versions = schemas.computeIfAbsent(c.topic, k -> new ArrayList<>());
                for (SchemaVersion v : versions) {
                    if (new org.apache.avro.Schema.Parser().parse(v.schema()).equals(candidate)) {
                        return Change.NONE; // idempotent re-registration
                    }
                }
                if (!versions.isEmpty()) {
                    // Deterministic, so checking here (not in the proposer) makes check-and-register atomic
                    org.apache.avro.Schema latest = new org.apache.avro.Schema.Parser().parse(versions.get(versions.size() - 1).schema());
                    String incompatibility = incompatibility(compatibilityOf(c.topic), latest, candidate);
                    if (incompatibility != null) {
                        throw new IllegalStateException(incompatibility);
                    }
                }
                versions.add(new SchemaVersion(nextSchemaId++, versions.size() + 1, candidate.toString()));
                return Change.SCHEMAS;
            }
            case FORGET_ENTITY -> {
                erasures.merge(c.entityKey, 1, Integer::sum);
                return Change.ENTITY_FORGOTTEN;
            }
            case SET_SCHEMA_COMPATIBILITY -> {
                try {
                    Compatibility.valueOf(c.compatibility);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Unknown compatibility mode: " + c.compatibility);
                }
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

    /** How many times the entity has been erased: data encrypted under an older generation is gone. */
    public synchronized int erasureGeneration(String entityKey) {
        return erasures.getOrDefault(entityKey, 0);
    }

    public synchronized Map<String, Integer> erasures() {
        return new TreeMap<>(erasures);
    }

    public synchronized String schemaCompatibility(String topic) {
        return schemaCompatibility.get(topic);
    }

    public synchronized Compatibility compatibilityOf(String topic) {
        String mode = schemaCompatibility.get(topic);
        return mode == null ? DEFAULT_COMPATIBILITY : Compatibility.valueOf(mode);
    }

    /** @return a human-readable reason the candidate breaks the mode against latest, or null if compatible */
    static String incompatibility(Compatibility mode, org.apache.avro.Schema latest, org.apache.avro.Schema candidate) {
        StringBuilder reasons = new StringBuilder();
        if (mode == Compatibility.BACKWARD || mode == Compatibility.FULL) {
            describe(reasons, "BACKWARD (new schema cannot read data written with the latest)",
                    org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility(candidate, latest));
        }
        if (mode == Compatibility.FORWARD || mode == Compatibility.FULL) {
            describe(reasons, "FORWARD (latest schema cannot read data written with the new one)",
                    org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility(latest, candidate));
        }
        return reasons.length() == 0 ? null : "Schema is incompatible under " + mode + ": " + reasons;
    }

    private static void describe(StringBuilder out, String direction,
                                 org.apache.avro.SchemaCompatibility.SchemaPairCompatibility result) {
        if (result.getType() == org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
            return;
        }
        out.append(direction).append(':');
        for (var problem : result.getResult().getIncompatibilities()) {
            out.append(" [").append(problem.getType()).append(" at ").append(problem.getLocation())
                    .append(": ").append(problem.getMessage()).append(']');
        }
    }

    // ---- snapshots ----

    private record Snapshot(List<BrokerRegistration> brokers, List<TopicImage> topics,
                            Map<String, List<SchemaVersion>> schemas, Map<String, String> schemaCompatibility,
                            int nextSchemaId, Map<String, Integer> erasures) {}

    public synchronized String toJson() {
        return GSON.toJson(new Snapshot(new ArrayList<>(brokers.values()), new ArrayList<>(topics.values()),
                schemas, schemaCompatibility, nextSchemaId, erasures));
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
        erasures.clear();
        if (s.erasures() != null) {
            erasures.putAll(s.erasures());
        }
    }
}
