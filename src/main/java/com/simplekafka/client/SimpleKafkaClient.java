package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.network.KafkaFrame;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metadata-aware client: discovers brokers and partition leaders from a bootstrap broker and
 * routes produce/fetch requests to the leader over pooled {@link BrokerConnection}s.
 */
public class SimpleKafkaClient implements AutoCloseable {
    private final String clientId;
    private final String bootstrapBroker;
    private final int bootstrapPort;
    private final Map<String, List<PartitionLayout>> topicLayouts = new ConcurrentHashMap<>();
    private final Map<Integer, BrokerInfo> clusterBrokers = new ConcurrentHashMap<>();
    private final Map<String, BrokerConnection> connections = new ConcurrentHashMap<>();
    private final Map<String, BrokerInfo> coordinators = new ConcurrentHashMap<>();

    public record PartitionLayout(int partition, int leader, List<Integer> replicas) {}

    /** A fetched record with its broker-assigned offset (not contiguous after compaction). */
    public record FetchedRecord(long offset, byte[] payload) {}

    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this(bootstrapBroker, bootstrapPort, "simple-client");
    }

    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort, String clientId) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.clientId = clientId;
    }

    public void initialize() throws IOException {
        refreshMetadata();
    }

    public void refreshMetadata() throws IOException {
        DataInputStream in = connection(bootstrapBroker, bootstrapPort)
                .request(KafkaFrame.API_KEY_METADATA, payload(o -> o.writeUTF(clientId)));

        Map<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();
        int brokerCount = in.readInt();
        for (int i = 0; i < brokerCount; i++) {
            int id = in.readInt();
            String host = in.readUTF();
            int port = in.readInt();
            brokers.put(id, new BrokerInfo(id, host, port));
        }
        Map<String, List<PartitionLayout>> layouts = new ConcurrentHashMap<>();
        int topicCount = in.readInt();
        for (int t = 0; t < topicCount; t++) {
            String name = in.readUTF();
            int partitionCount = in.readInt();
            List<PartitionLayout> partitions = new ArrayList<>(partitionCount);
            for (int p = 0; p < partitionCount; p++) {
                int partition = in.readInt();
                int leader = in.readInt();
                int replicaCount = in.readInt();
                List<Integer> replicas = new ArrayList<>(replicaCount);
                for (int r = 0; r < replicaCount; r++) {
                    replicas.add(in.readInt());
                }
                partitions.add(new PartitionLayout(partition, leader, replicas));
            }
            layouts.put(name, partitions);
        }
        clusterBrokers.clear();
        clusterBrokers.putAll(brokers);
        topicLayouts.clear();
        topicLayouts.putAll(layouts);
    }

    public int getNumPartitions(String topic) throws IOException {
        return layout(topic).size();
    }

    public Map<Integer, BrokerInfo> getBrokers() {
        return clusterBrokers;
    }

    /** Tries each known broker until the controller accepts the request. */
    public void createTopic(String topic, int numPartitions, short replicationFactor) throws IOException {
        if (clusterBrokers.isEmpty()) {
            refreshMetadata();
        }
        byte[] request = payload(o -> {
            o.writeUTF(clientId);
            o.writeUTF(topic);
            o.writeInt(numPartitions);
            o.writeShort(replicationFactor);
        });
        IOException last = null;
        for (BrokerInfo broker : clusterBrokers.values()) {
            try {
                connection(broker.getHost(), broker.getPort()).request(KafkaFrame.API_KEY_CREATE_TOPIC, request);
                refreshMetadata();
                return;
            } catch (IOException e) {
                last = e; // not the controller (or unreachable): try the next broker
            }
        }
        throw new IOException("Failed to create topic " + topic + ": no broker accepted the request", last);
    }

    /** Non-idempotent produce; see NettyIdempotentProducer for exactly-once-per-session semantics. */
    public long send(String topic, int partition, byte[] message) throws IOException {
        byte[] request = payload(o -> {
            o.writeUTF(clientId);
            o.writeUTF(topic);
            o.writeInt(partition);
            o.writeLong(KafkaFrame.NO_PRODUCER_ID);
            o.writeShort(0);
            o.writeInt(0);
            o.writeInt(message.length);
            o.write(message);
        });
        DataInputStream in = leaderRequest(topic, partition, KafkaFrame.API_KEY_PRODUCE, request);
        in.readUTF();
        in.readInt();
        return in.readLong();
    }

    public List<FetchedRecord> fetchRecords(String topic, int partition, long offset, int maxBytes) throws IOException {
        DataInputStream in = leaderRequest(topic, partition, KafkaFrame.API_KEY_FETCH, payload(o -> {
            o.writeUTF(clientId);
            o.writeUTF(topic);
            o.writeInt(partition);
            o.writeLong(offset);
            o.writeInt(maxBytes);
        }));
        int count = in.readInt();
        List<FetchedRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long recordOffset = in.readLong();
            byte[] data = new byte[in.readInt()];
            in.readFully(data);
            records.add(new FetchedRecord(recordOffset, data));
        }
        return records;
    }

    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        List<byte[]> payloads = new ArrayList<>();
        for (FetchedRecord r : fetchRecords(topic, partition, offset, maxBytes)) {
            payloads.add(r.payload());
        }
        return payloads;
    }

    /** Locates the broker coordinating {@code group} (cached until it stops answering). */
    public BrokerInfo findCoordinator(String group) throws IOException {
        BrokerInfo cached = coordinators.get(group);
        if (cached != null) {
            return cached;
        }
        DataInputStream in = connection(bootstrapBroker, bootstrapPort).request(KafkaFrame.API_KEY_FIND_COORDINATOR,
                payload(o -> {
                    o.writeUTF(clientId);
                    o.writeUTF(group);
                }));
        BrokerInfo coordinator = new BrokerInfo(in.readInt(), in.readUTF(), in.readInt());
        coordinators.put(group, coordinator);
        return coordinator;
    }

    public void commitOffset(String group, String topic, int partition, long offset, String metadata) throws IOException {
        coordinatorRequest(group, KafkaFrame.API_KEY_OFFSET_COMMIT, payload(o -> {
            o.writeUTF(clientId);
            o.writeUTF(group);
            o.writeUTF(topic);
            o.writeInt(partition);
            o.writeLong(offset);
            o.writeUTF(metadata == null ? "" : metadata);
        }));
    }

    /** @return the group's committed offset for the partition, or -1 if it never committed */
    public long fetchCommittedOffset(String group, String topic, int partition) throws IOException {
        return coordinatorRequest(group, KafkaFrame.API_KEY_OFFSET_FETCH, payload(o -> {
            o.writeUTF(clientId);
            o.writeUTF(group);
            o.writeUTF(topic);
            o.writeInt(partition);
        })).readLong();
    }

    /** Sends to the group's coordinator, re-discovering it once if it moved or is unreachable. */
    DataInputStream coordinatorRequest(String group, short apiKey, byte[] request) throws IOException {
        for (int attempt = 0; ; attempt++) {
            BrokerInfo coordinator = findCoordinator(group);
            try {
                return connection(coordinator.getHost(), coordinator.getPort()).request(apiKey, request);
            } catch (IOException e) {
                boolean retriable = !(e instanceof BrokerErrorException be)
                        || be.getErrorCode() == KafkaFrame.ERROR_NOT_COORDINATOR;
                coordinators.remove(group);
                if (!retriable || attempt >= 1) {
                    throw e;
                }
            }
        }
    }

    /** Registers (or finds) a schema for the topic; returns its registry ID. Fails if incompatible. */
    public int registerSchema(String topic, String schemaJson) throws IOException {
        byte[] schemaBytes = schemaJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return connection(bootstrapBroker, bootstrapPort).request(KafkaFrame.API_KEY_REGISTER_SCHEMA, payload(o -> {
            o.writeUTF(clientId);
            o.writeUTF(topic);
            o.writeInt(schemaBytes.length);
            o.write(schemaBytes);
        })).readInt();
    }

    /** NONE, BACKWARD (default), FORWARD or FULL. */
    public void setSchemaCompatibility(String topic, String mode) throws IOException {
        connection(bootstrapBroker, bootstrapPort).request(KafkaFrame.API_KEY_SET_SCHEMA_COMPATIBILITY, payload(o -> {
            o.writeUTF(clientId);
            o.writeUTF(topic);
            o.writeUTF(mode);
        }));
    }

    public String getSchema(int schemaId) throws IOException {
        DataInputStream in = connection(bootstrapBroker, bootstrapPort).request(KafkaFrame.API_KEY_GET_SCHEMA, payload(o -> {
            o.writeUTF(clientId);
            o.writeInt(schemaId);
        }));
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /** GDPR erasure: every broker destroys the entity's data key; its PII fields read as [REDACTED]. */
    public void forgetEntity(String entityId) throws IOException {
        connection(bootstrapBroker, bootstrapPort).request(KafkaFrame.API_KEY_FORGET_ENTITY, payload(o -> {
            o.writeUTF(clientId);
            o.writeUTF(entityId);
        }));
    }

    /** Assigns consecutive offsets starting at {@code startOffset}; for tests and simple mocks. */
    public static List<FetchedRecord> contiguous(long startOffset, List<byte[]> payloads) {
        List<FetchedRecord> records = new ArrayList<>(payloads.size());
        for (int i = 0; i < payloads.size(); i++) {
            records.add(new FetchedRecord(startOffset + i, payloads.get(i)));
        }
        return records;
    }

    private List<PartitionLayout> layout(String topic) throws IOException {
        List<PartitionLayout> layout = topicLayouts.get(topic);
        if (layout == null) {
            refreshMetadata();
            layout = topicLayouts.get(topic);
            if (layout == null) {
                throw new IOException("Topic not found: " + topic);
            }
        }
        return layout;
    }

    private static final int LEADER_RETRIES = 10;
    private static final long LEADER_RETRY_BACKOFF_MS = 200;

    /** Sends to the partition leader; on a leadership error refreshes metadata and retries. */
    private DataInputStream leaderRequest(String topic, int partition, short apiKey, byte[] request) throws IOException {
        for (int attempt = 0; ; attempt++) {
            try {
                return leaderConnection(topic, partition).request(apiKey, request);
            } catch (BrokerErrorException e) {
                boolean leadershipMoved = e.getErrorCode() == KafkaFrame.ERROR_NOT_LEADER
                        || e.getErrorCode() == KafkaFrame.ERROR_LEADER_NOT_AVAILABLE;
                if (!leadershipMoved || attempt >= LEADER_RETRIES) {
                    throw e;
                }
            } catch (IOException e) {
                if (attempt >= LEADER_RETRIES) {
                    throw e; // leader unreachable; metadata may name a new one shortly
                }
            }
            try {
                Thread.sleep(LEADER_RETRY_BACKOFF_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for partition leadership", ie);
            }
            refreshMetadata();
        }
    }

    private BrokerConnection leaderConnection(String topic, int partition) throws IOException {
        for (PartitionLayout p : layout(topic)) {
            if (p.partition() == partition) {
                BrokerInfo leader = clusterBrokers.get(p.leader());
                if (leader == null) {
                    throw new IOException("Leader broker " + p.leader() + " unknown for " + topic + "-" + partition);
                }
                return connection(leader.getHost(), leader.getPort());
            }
        }
        throw new IOException("Partition " + partition + " not found for topic " + topic);
    }

    private BrokerConnection connection(String host, int port) throws IOException {
        String key = host + ":" + port;
        BrokerConnection existing = connections.get(key);
        if (existing != null && existing.isActive()) {
            return existing;
        }
        BrokerConnection fresh = BrokerConnection.connect(host, port);
        BrokerConnection previous = connections.put(key, fresh);
        if (previous != null) {
            previous.close();
        }
        return fresh;
    }

    @FunctionalInterface
    interface PayloadWriter {
        void write(DataOutputStream out) throws IOException;
    }

    static byte[] payload(PayloadWriter writer) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        writer.write(out);
        out.flush();
        return bytes.toByteArray();
    }

    @Override
    public void close() {
        connections.values().forEach(BrokerConnection::close);
        connections.clear();
    }
}
