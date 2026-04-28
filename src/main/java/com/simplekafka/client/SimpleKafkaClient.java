package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;
import com.simplekafka.broker.Protocol.MetadataResult;
import com.simplekafka.broker.Protocol.PartitionMetadata;
import com.simplekafka.broker.Protocol.TopicMetadata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleKafkaClient {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaClient.class.getName());
    
    private final String bootstrapBroker;
    private final int bootstrapPort;
    private final Map<String, TopicMetadata> topicMetadata;
    private final Map<Integer, BrokerInfo> clusterBrokers;

    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.topicMetadata = new ConcurrentHashMap<>();
        this.clusterBrokers = new ConcurrentHashMap<>();
    }

    public void initialize() throws IOException {
        refreshMetadata();
    }

    public void refreshMetadata() throws IOException {
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(bootstrapBroker, bootstrapPort))) {
            ByteBuffer request = Protocol.encodeMetadataRequest();
            while (request.hasRemaining()) {
                channel.write(request);
            }
            
            ByteBuffer responseBuffer = ByteBuffer.allocate(8192);
            int bytesRead = channel.read(responseBuffer);
            if (bytesRead > 0) {
                responseBuffer.flip();
                MetadataResult result = Protocol.decodeMetadataResponse(responseBuffer);
                if (result.hasError()) {
                    throw new IOException("Failed to fetch metadata: " + result.getError());
                }
                
                clusterBrokers.clear();
                if (result.getBrokers() != null) {
                    for (BrokerInfo broker : result.getBrokers()) {
                        clusterBrokers.put(broker.getId(), broker);
                    }
                }
                
                topicMetadata.clear();
                if (result.getTopics() != null) {
                    for (TopicMetadata tm : result.getTopics()) {
                        topicMetadata.put(tm.getTopicName(), tm);
                    }
                }
            }
        }
    }

    public int getNumPartitions(String topic) throws IOException {
        TopicMetadata metadata = topicMetadata.get(topic);
        if (metadata == null) {
            refreshMetadata();
            metadata = topicMetadata.get(topic);
            if (metadata == null) {
                throw new IOException("Topic not found: " + topic);
            }
        }
        return metadata.getPartitions().size();
    }

    public void createTopic(String topic, int numPartitions, short replicationFactor) throws IOException {
        for (BrokerInfo broker : clusterBrokers.values()) {
            try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(broker.getHost(), broker.getPort()))) {
                ByteBuffer request = Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor);
                while (request.hasRemaining()) {
                    channel.write(request);
                }
                
                ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
                int bytesRead = channel.read(responseBuffer);
                if (bytesRead > 0) {
                    responseBuffer.flip();
                    byte type = responseBuffer.get();
                    if (type == Protocol.ERROR_RESPONSE) {
                        continue; // try next broker
                    } else if (type == Protocol.CREATE_TOPIC_RESPONSE) {
                        return; // success
                    }
                }
            } catch (Exception e) {
                // Ignore and try next
            }
        }
        throw new IOException("Failed to create topic " + topic + ". Could not find controller.");
    }

    private BrokerInfo getLeaderBroker(String topic, int partition) throws IOException {
        TopicMetadata metadata = topicMetadata.get(topic);
        if (metadata == null) {
            refreshMetadata();
            metadata = topicMetadata.get(topic);
            if (metadata == null) {
                throw new IOException("Topic not found: " + topic);
            }
        }
        
        for (PartitionMetadata pm : metadata.getPartitions()) {
            if (pm.getPartitionId() == partition) {
                BrokerInfo leaderInfo = clusterBrokers.get(pm.getLeaderBrokerId());
                if (leaderInfo == null) {
                    throw new IOException("Leader broker not found for partition " + partition);
                }
                return leaderInfo;
            }
        }
        
        throw new IOException("Partition " + partition + " not found for topic " + topic);
    }

    public long send(String topic, int partition, byte[] message) throws IOException {
        BrokerInfo leader = getLeaderBroker(topic, partition);
        
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(leader.getHost(), leader.getPort()))) {
            ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, message);
            while (request.hasRemaining()) {
                channel.write(request);
            }
            
            ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
            int bytesRead = channel.read(responseBuffer);
            if (bytesRead > 0) {
                responseBuffer.flip();
                Protocol.ProduceResult result = Protocol.decodeProduceResponse(responseBuffer);
                if (result.hasError()) {
                    throw new IOException("Produce error: " + result.getError());
                }
                return result.getOffset();
            }
            throw new IOException("No response from broker");
        }
    }

    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        BrokerInfo leader = getLeaderBroker(topic, partition);
        
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(leader.getHost(), leader.getPort()))) {
            ByteBuffer request = Protocol.encodeFetchRequest(topic, partition, offset, maxBytes);
            while (request.hasRemaining()) {
                channel.write(request);
            }
            
            ByteBuffer responseBuffer = ByteBuffer.allocate(maxBytes + 1024); // room for headers
            int bytesRead = channel.read(responseBuffer);
            if (bytesRead > 0) {
                responseBuffer.flip();
                Protocol.FetchResult result = Protocol.decodeFetchResponse(responseBuffer);
                if (result.hasError()) {
                    throw new IOException("Fetch error: " + result.getError());
                }
                return result.getMessages();
            }
            return new ArrayList<>();
        }
    }
}
