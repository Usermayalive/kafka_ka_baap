package com.simplekafka.metadata;

import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * A replicated metadata mutation. Commands are JSON in the Raft log and are applied in log
 * order by every node's {@link MetadataStateMachine}, so applying them must be deterministic:
 * no clocks, randomness, or local state. Anything nondeterministic (e.g. which brokers get a
 * new topic's partitions) is decided by the proposer and carried in the command.
 */
public final class MetadataCommand {
    public enum Type {
        REGISTER_BROKER,
        UNREGISTER_BROKER,
        CREATE_TOPIC,
        UPDATE_ISR,
        ELECT_LEADER,
        REGISTER_SCHEMA,
        SET_SCHEMA_COMPATIBILITY
    }

    private static final Gson GSON = new Gson();

    public Type type;
    public Integer brokerId;
    public String host;
    public Integer port;
    public Long incarnation;
    public String topic;
    public Map<String, String> config;
    public List<MetadataImage.PartitionState> partitions;
    public Integer partition;
    public Integer leader;
    public List<Integer> isr;
    public String schema;
    public String compatibility;

    private MetadataCommand(Type type) {
        this.type = type;
    }

    public static MetadataCommand registerBroker(int brokerId, String host, int port, long incarnation) {
        MetadataCommand c = new MetadataCommand(Type.REGISTER_BROKER);
        c.brokerId = brokerId;
        c.host = host;
        c.port = port;
        c.incarnation = incarnation;
        return c;
    }

    public static MetadataCommand unregisterBroker(int brokerId, long incarnation) {
        MetadataCommand c = new MetadataCommand(Type.UNREGISTER_BROKER);
        c.brokerId = brokerId;
        c.incarnation = incarnation;
        return c;
    }

    public static MetadataCommand createTopic(String topic, Map<String, String> config, List<MetadataImage.PartitionState> partitions) {
        MetadataCommand c = new MetadataCommand(Type.CREATE_TOPIC);
        c.topic = topic;
        c.config = config;
        c.partitions = partitions;
        return c;
    }

    public static MetadataCommand updateIsr(String topic, int partition, List<Integer> isr) {
        MetadataCommand c = new MetadataCommand(Type.UPDATE_ISR);
        c.topic = topic;
        c.partition = partition;
        c.isr = isr;
        return c;
    }

    /** {@code leader} -1 marks the partition offline (no live in-sync replica). */
    public static MetadataCommand electLeader(String topic, int partition, int leader) {
        MetadataCommand c = new MetadataCommand(Type.ELECT_LEADER);
        c.topic = topic;
        c.partition = partition;
        c.leader = leader;
        return c;
    }

    public static MetadataCommand registerSchema(String topic, String schema) {
        MetadataCommand c = new MetadataCommand(Type.REGISTER_SCHEMA);
        c.topic = topic;
        c.schema = schema;
        return c;
    }

    public static MetadataCommand setSchemaCompatibility(String topic, String compatibility) {
        MetadataCommand c = new MetadataCommand(Type.SET_SCHEMA_COMPATIBILITY);
        c.topic = topic;
        c.compatibility = compatibility;
        return c;
    }

    public byte[] toBytes() {
        return GSON.toJson(this).getBytes(StandardCharsets.UTF_8);
    }

    public static MetadataCommand fromBytes(byte[] bytes) {
        return GSON.fromJson(new String(bytes, StandardCharsets.UTF_8), MetadataCommand.class);
    }
}
