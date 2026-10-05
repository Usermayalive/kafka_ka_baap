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
        DataInputStream in = leaderConnection(topic, partition).request(KafkaFrame.API_KEY_PRODUCE, payload(o -> {
            o.writeUTF(clientId);
            o.writeUTF(topic);
            o.writeInt(partition);
            o.writeLong(KafkaFrame.NO_PRODUCER_ID);
            o.writeShort(0);
            o.writeInt(0);
            o.writeInt(message.length);
            o.write(message);
        }));
        in.readUTF();
        in.readInt();
        return in.readLong();
    }

    public List<FetchedRecord> fetchRecords(String topic, int partition, long offset, int maxBytes) throws IOException {
        DataInputStream in = leaderConnection(topic, partition).request(KafkaFrame.API_KEY_FETCH, payload(o -> {
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
