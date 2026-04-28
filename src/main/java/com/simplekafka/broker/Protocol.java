package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Protocol {

    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
    public static final byte REPLICATE = 0x05;
    public static final byte TOPIC_NOTIFICATION = 0x06;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = (byte) 0xFF;

    // 1. Encode Produce Request
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int capacity = 1 + 2 + topicBytes.length + 4 + 4 + message.length;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        
        buffer.put(PRODUCE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        
        buffer.flip();
        return buffer;
    }

    // 2. Encode Fetch Request
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int capacity = 1 + 2 + topicBytes.length + 4 + 8 + 4;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        
        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        
        buffer.flip();
        return buffer;
    }

    // 3. Encode Metadata Request
    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    // 4. Encode Create Topic Request
    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int capacity = 1 + 2 + topicBytes.length + 4 + 2;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        
        buffer.flip();
        return buffer;
    }

    // Encode Replicate Request
    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int capacity = 1 + 2 + topicBytes.length + 4 + 8 + 4 + message.length;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        
        buffer.put(REPLICATE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        
        buffer.flip();
        return buffer;
    }

    // Encode Topic Notification
    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int capacity = 1 + 2 + topicBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        
        buffer.flip();
        return buffer;
    }

    // Decode Produce Response
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte type = buffer.get();
        if (type == ERROR_RESPONSE) {
            String error = readString(buffer);
            return new ProduceResult(-1, error);
        } else if (type == PRODUCE_RESPONSE) {
            long offset = buffer.getLong();
            return new ProduceResult(offset, null);
        }
        return new ProduceResult(-1, "Invalid response type: " + type);
    }

    // Decode Fetch Response
    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte type = buffer.get();
        if (type == ERROR_RESPONSE) {
            String error = readString(buffer);
            return new FetchResult(null, error);
        } else if (type == FETCH_RESPONSE) {
            int messageCount = buffer.getInt();
            List<byte[]> messages = new ArrayList<>(messageCount);
            for (int i = 0; i < messageCount; i++) {
                long offset = buffer.getLong(); // read offset
                int size = buffer.getInt();     // read size
                byte[] message = new byte[size];
                buffer.get(message);
                messages.add(message);
            }
            return new FetchResult(messages, null);
        }
        return new FetchResult(null, "Invalid response type: " + type);
    }

    // Decode Metadata Response
    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte type = buffer.get();
        if (type == ERROR_RESPONSE) {
            String error = readString(buffer);
            return new MetadataResult(null, null, error);
        } else if (type == METADATA_RESPONSE) {
            // Read brokers
            int brokerCount = buffer.getInt();
            List<BrokerInfo> brokers = new ArrayList<>(brokerCount);
            for (int i = 0; i < brokerCount; i++) {
                int id = buffer.getInt();
                String host = readString(buffer);
                int port = buffer.getInt();
                brokers.add(new BrokerInfo(id, host, port));
            }
            
            // Read topics
            int topicCount = buffer.getInt();
            List<TopicMetadata> topics = new ArrayList<>(topicCount);
            for (int i = 0; i < topicCount; i++) {
                String topicName = readString(buffer);
                int partitionCount = buffer.getInt();
                List<PartitionMetadata> partitions = new ArrayList<>(partitionCount);
                for (int j = 0; j < partitionCount; j++) {
                    int partitionId = buffer.getInt();
                    int leaderId = buffer.getInt();
                    int replicaCount = buffer.getInt();
                    List<Integer> replicas = new ArrayList<>(replicaCount);
                    for (int k = 0; k < replicaCount; k++) {
                        replicas.add(buffer.getInt());
                    }
                    partitions.add(new PartitionMetadata(partitionId, leaderId, replicas));
                }
                topics.add(new TopicMetadata(topicName, partitions));
            }
            return new MetadataResult(brokers, topics, null);
        }
        return new MetadataResult(null, null, "Invalid response type: " + type);
    }

    // Error Handling
    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        byte[] errorBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + errorBytes.length);
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) errorBytes.length);
        buffer.put(errorBytes);
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // --- Result Classes ---

    public static class ProduceResult {
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long getOffset() { return offset; }
        public String getError() { return error; }
        public boolean hasError() { return error != null; }
    }

    public static class FetchResult {
        private final List<byte[]> messages;
        private final String error;

        public FetchResult(List<byte[]> messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public List<byte[]> getMessages() { return messages; }
        public String getError() { return error; }
        public boolean hasError() { return error != null; }
    }

    public static class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() { return brokers; }
        public List<TopicMetadata> getTopics() { return topics; }
        public String getError() { return error; }
        public boolean hasError() { return error != null; }
    }

    public static class TopicMetadata {
        private final String topicName;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String topicName, List<PartitionMetadata> partitions) {
            this.topicName = topicName;
            this.partitions = partitions;
        }

        public String getTopicName() { return topicName; }
        public List<PartitionMetadata> getPartitions() { return partitions; }
    }

    public static class PartitionMetadata {
        private final int partitionId;
        private final int leaderBrokerId;
        private final List<Integer> replicaBrokerIds;

        public PartitionMetadata(int partitionId, int leaderBrokerId, List<Integer> replicaBrokerIds) {
            this.partitionId = partitionId;
            this.leaderBrokerId = leaderBrokerId;
            this.replicaBrokerIds = replicaBrokerIds;
        }

        public int getPartitionId() { return partitionId; }
        public int getLeaderBrokerId() { return leaderBrokerId; }
        public List<Integer> getReplicaBrokerIds() { return replicaBrokerIds; }
    }
}
