package com.simplekafka.broker;


import com.google.gson.Gson;
import com.simplekafka.storage.WriteAheadLog.DurabilityMode;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleKafkaBroker {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = "kafka-data";
    
    private final int brokerId;

    public int getBrokerId() {
        return brokerId;
    }

    private final String brokerHost;
    private final int brokerPort;
    private final Map<String, List<Partition>> topics;
    private final ExecutorService executor;
    private final ServerSocketChannel serverChannel;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isController;
    private final Map<Integer, BrokerInfo> clusterMetadata;
    private final ZookeeperClient zkClient;
    private final Gson gson;
    private final ConcurrentHashMap<String, java.util.concurrent.CompletableFuture<Void>> topicCreationGuards = new ConcurrentHashMap<>();
    private volatile DurabilityMode durabilityMode = DurabilityMode.OS_PAGE_CACHE;

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(10);
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();
        this.gson = new Gson();
        
        // Initialize data directory
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        
        // Initialize ZooKeeper client
        this.zkClient = new ZookeeperClient("localhost:" + zkPort);
    }

    public void start() throws Exception {
        isRunning.set(true);
        serverChannel.bind(new InetSocketAddress(brokerHost, brokerPort));
        serverChannel.configureBlocking(false);

        LOGGER.info("Starting broker " + brokerId + " on " + brokerHost + ":" + brokerPort);
        
        zkClient.connect();
        registerWithZookeeper();
        electController();
        loadExistingTopics();

        // Start accepting connections in a separate thread
        new Thread(this::acceptConnections, "Accept-Thread").start();
    }

    public void stop() {
        isRunning.set(false);
        try {
            if (serverChannel != null) {
                serverChannel.close();
            }
            executor.shutdown();
            zkClient.close();
            LOGGER.info("Broker " + brokerId + " stopped");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error stopping broker", e);
        }
    }

    private void registerWithZookeeper() {
        try {
            JsonObject brokerData = new JsonObject();
            brokerData.addProperty("host", brokerHost);
            brokerData.addProperty("port", brokerPort);
            brokerData.add("endpoints", new JsonArray());
            
            String path = "/brokers/ids/" + brokerId;
            // Ensure parent directories exist
            zkClient.createPath("/brokers");
            zkClient.createPath("/brokers/ids");
            zkClient.createEphemeralNode(path, gson.toJson(brokerData));
            
            // Watch for cluster changes
            zkClient.watchChildren("/brokers/ids", this::onBrokersChanged);
            
            // Watch for topics
            zkClient.watchChildren("/topics", this::onTopicsChanged);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with Zookeeper", e);
        }
    }

    private void onBrokersChanged(List<String> brokerIds) {
        LOGGER.info("Brokers changed: " + brokerIds);
        try {
            Map<Integer, BrokerInfo> newMetadata = new ConcurrentHashMap<>();
            for (String idStr : brokerIds) {
                int id = Integer.parseInt(idStr);
                String data = zkClient.getData("/brokers/ids/" + id);
                JsonObject json = gson.fromJson(data, JsonObject.class);
                newMetadata.put(id, new BrokerInfo(id, json.get("host").getAsString(), json.get("port").getAsInt()));
            }
            clusterMetadata.clear();
            clusterMetadata.putAll(newMetadata);

            if (isController.get()) {
                rebalancePartitions();
            } else {
                electController();
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error processing broker changes", e);
        }
    }

    private void onTopicsChanged(List<String> topicNames) {
        for (String topic : topicNames) {
            if (!topics.containsKey(topic)) {
                loadTopic(topic);
            }
        }
    }

    private void electController() {
        try {
            String controllerPath = "/controller";
            
            boolean nodeExists = false;
            try {
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    nodeExists = false; 
                } else {
                    nodeExists = true;
                }
            } catch (Exception e) {
                nodeExists = false;
            }
            
            boolean becameController = false;
            if (!nodeExists) {
                JsonObject data = new JsonObject();
                data.addProperty("broker", brokerId);
                data.addProperty("timestamp", System.currentTimeMillis());
                becameController = zkClient.createEphemeralNode(controllerPath, gson.toJson(data));
            }
            
            if (becameController) {
                LOGGER.info("Broker " + brokerId + " became controller");
                isController.set(true);
                rebalancePartitions();
            } else {
                isController.set(false);
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed", e);
            new Thread(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }

    private void onControllerChange() {
        electController();
    }

    public volatile Runnable preTopicCreationHook = null;

    public void createTopic(String topic, int numPartitions, short replicationFactor) {
        if (!isController.get()) {
            LOGGER.warning("Only controller can create topics");
            return;
        }

        if (topics.containsKey(topic)) {
            LOGGER.info("Topic " + topic + " already exists on broker (idempotent no-op)");
            return;
        }

        java.util.concurrent.CompletableFuture<Void> myFuture = new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Void> existing = topicCreationGuards.putIfAbsent(topic, myFuture);
        if (existing != null) {
            // Wait for the creator thread to finish
            try {
                existing.join();
            } catch (Exception e) {
                LOGGER.warning("Wait for concurrent creation of topic " + topic + " failed: " + e.getMessage());
            }
            return;
        }

        try {
            if (topics.containsKey(topic)) {
                myFuture.complete(null);
                return;
            }

            if (preTopicCreationHook != null) {
                preTopicCreationHook.run();
            }

            JsonObject config = new JsonObject();
            config.addProperty("partitions", numPartitions);
            config.addProperty("replication_factor", replicationFactor);
            if (topic.endsWith(".dlq")) {
                config.addProperty("cleanup.policy", "compact");
            }
            zkClient.createPersistentNode("/topics/" + topic, gson.toJson(config));
            
            zkClient.createPath("/topics/" + topic + "/partitions");

            List<Partition> topicPartitions = new ArrayList<>();
            List<Integer> availableBrokers = new ArrayList<>(clusterMetadata.keySet());
            if (availableBrokers.isEmpty()) {
                availableBrokers.add(brokerId);
            }

            for (int i = 0; i < numPartitions; i++) {
                int leader = availableBrokers.get(i % availableBrokers.size());
                List<Integer> followers = new ArrayList<>();
                for (int j = 1; j < replicationFactor; j++) {
                    followers.add(availableBrokers.get((i + j) % availableBrokers.size()));
                }

                zkClient.createPath("/topics/" + topic + "/partitions/" + i);
                JsonObject state = new JsonObject();
                state.addProperty("leader", leader);
                JsonArray isr = new JsonArray();
                isr.add(leader);
                followers.forEach(isr::add);
                state.add("isr", isr);
                JsonArray replicas = new JsonArray();
                replicas.add(leader);
                followers.forEach(replicas::add);
                state.add("replicas", replicas);

                zkClient.createPersistentNode("/topics/" + topic + "/partitions/" + i + "/state", gson.toJson(state));
            }
            
            loadTopic(topic);
            LOGGER.info("Topic created and loaded: " + topic);

            // Auto-provision companion DLQ topic with 1 partition if eligible (excludes internal topics)
            if (com.simplekafka.client.DeadLetterRouter.isEligibleForDlq(topic)) {
                String dlqTopic = topic + ".dlq";
                LOGGER.info("Auto-provisioning companion DLQ topic: " + dlqTopic);
                createTopic(dlqTopic, 1, replicationFactor);
            }

            myFuture.complete(null);
        } catch (Exception e) {
            myFuture.completeExceptionally(e);
            topicCreationGuards.remove(topic);
            LOGGER.log(Level.SEVERE, "Failed to create topic " + topic, e);
            throw new RuntimeException("Failed to create topic: " + topic, e);
        }
    }

    private void loadExistingTopics() {
        // Simplified initialization
    }

    private void loadTopic(String topic) {
        int maxRetries = 5;
        for (int retry = 0; retry < maxRetries; retry++) {
            try {
                String configData = zkClient.getData("/topics/" + topic);
                JsonObject config = gson.fromJson(configData, JsonObject.class);
                int numPartitions = config.get("partitions").getAsInt();

                List<Partition> partList = topics.computeIfAbsent(topic, k -> new ArrayList<>());

                for (int i = 0; i < numPartitions; i++) {
                    String stateData = zkClient.getData("/topics/" + topic + "/partitions/" + i + "/state");
                    JsonObject state = gson.fromJson(stateData, JsonObject.class);
                    int leader = state.get("leader").getAsInt();
                    
                    List<Integer> replicas = new ArrayList<>();
                    state.getAsJsonArray("replicas").forEach(e -> replicas.add(e.getAsInt()));

                    if (leader == brokerId || replicas.contains(brokerId)) {
                        String baseDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
                        Partition partition = new Partition(i, baseDir, durabilityMode);
                        if (config.has("cleanup.policy") && "compact".equalsIgnoreCase(config.get("cleanup.policy").getAsString())) {
                            partition.setCompacted(true);
                        }
                        partition.setLeader(leader);
                        List<Integer> followers = new ArrayList<>(replicas);
                        followers.remove((Integer) leader);
                        partition.setFollowers(followers);
                        
                        if (partList.size() <= i) {
                            partList.add(partition);
                        } else {
                            partList.set(i, partition);
                        }
                    }
                }
                return; // successfully loaded
            } catch (Exception e) {
                if (retry == maxRetries - 1) {
                    LOGGER.log(Level.SEVERE, "Failed to load topic " + topic, e);
                    topics.remove(topic);
                } else {
                    try { Thread.sleep(500); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        }
    }

    public void compactTopic(String topic, int partitionId) throws IOException {
        List<Partition> parts = topics.get(topic);
        if (parts != null && partitionId < parts.size()) {
            CompactionKeyExtractor extractor = topic.endsWith(".dlq")
                    ? msg -> {
                        try {
                            return com.simplekafka.client.DeadLetterEnvelope.deserialize(msg).getDeduplicationKey();
                        } catch (Exception e) {
                            return java.util.UUID.nameUUIDFromBytes(msg).toString();
                        }
                    }
                    : msg -> java.util.UUID.nameUUIDFromBytes(msg).toString();
            parts.get(partitionId).compact(extractor);
        }
    }

    public Map<String, List<Partition>> getTopics() {
        return topics;
    }

    private void rebalancePartitions() {
        if (!isController.get()) return;
        LOGGER.info("Rebalancing partitions...");
        // Rebalancing logic to be implemented
    }

    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = readString(buffer);
        int partitionId = buffer.getInt();
        int messageLength = buffer.getInt();
        byte[] message = new byte[messageLength];
        buffer.get(message);

        List<Partition> parts = topics.get(topic);
        if (parts == null || partitionId >= parts.size()) {
            // Simplified atomic auto-create: delegates directly to atomic createTopic()
            if (topic.endsWith(".dlq") && isController.get()) {
                LOGGER.info("Auto-provisioning companion DLQ topic on first produce: " + topic);
                try {
                    createTopic(topic, 1, (short) 1);
                    parts = topics.get(topic);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Failed to auto-create DLQ topic: " + topic, e);
                }
            }
            if (parts == null || partitionId >= parts.size()) {
                Protocol.sendErrorResponse(clientChannel, "Topic/Partition not found");
                return;
            }
        }

        Partition partition = parts.get(partitionId);
        if (partition.getLeader() != brokerId) {
            Protocol.sendErrorResponse(clientChannel, "Not leader for partition");
            return;
        }

        long offset = partition.append(message);
        
        // Replicate to followers
        for (int follower : partition.getFollowers()) {
            executor.submit(() -> replicateToFollowers(topic, partition, message, offset, follower));
        }

        // Send Produce Response
        ByteBuffer response = ByteBuffer.allocate(1 + 8);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.flip();
        while (response.hasRemaining()) {
            clientChannel.write(response);
        }
    }

    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = readString(buffer);
        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();

        List<Partition> parts = topics.get(topic);
        if (parts == null || partitionId >= parts.size()) {
            Protocol.sendErrorResponse(clientChannel, "Topic/Partition not found");
            return;
        }

        Partition partition = parts.get(partitionId);
        List<byte[]> messages = partition.readMessages(offset, maxBytes);

        // Calculate response size
        int totalSize = 1 + 4; // Type + Count
        for (byte[] msg : messages) {
            totalSize += 8 + 4 + msg.length; // offset + size + data
        }

        ByteBuffer response = ByteBuffer.allocate(totalSize);
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());
        
        long currOffset = offset;
        for (byte[] msg : messages) {
            response.putLong(currOffset++);
            response.putInt(msg.length);
            response.put(msg);
        }
        
        response.flip();
        while (response.hasRemaining()) {
            clientChannel.write(response);
        }
    }

    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        int brokerCount = clusterMetadata.size();
        ByteBuffer response = ByteBuffer.allocate(8192);
        response.put(Protocol.METADATA_RESPONSE);
        response.putInt(brokerCount);
        for (BrokerInfo info : clusterMetadata.values()) {
            response.putInt(info.getId());
            byte[] hostBytes = info.getHost().getBytes(StandardCharsets.UTF_8);
            response.putShort((short) hostBytes.length);
            response.put(hostBytes);
            response.putInt(info.getPort());
        }
        
        try {
            List<String> topicList = zkClient.getChildren("/topics");
            response.putInt(topicList.size());
            for (String topicName : topicList) {
                byte[] nameBytes = topicName.getBytes(StandardCharsets.UTF_8);
                response.putShort((short) nameBytes.length);
                response.put(nameBytes);
                
                String configData = zkClient.getData("/topics/" + topicName);
                JsonObject config = gson.fromJson(configData, JsonObject.class);
                int numPartitions = config.get("partitions").getAsInt();
                
                response.putInt(numPartitions);
                for (int i = 0; i < numPartitions; i++) {
                    String stateData = zkClient.getData("/topics/" + topicName + "/partitions/" + i + "/state");
                    JsonObject state = gson.fromJson(stateData, JsonObject.class);
                    int leader = state.get("leader").getAsInt();
                    JsonArray replicas = state.getAsJsonArray("replicas");
                    
                    response.putInt(i); // partitionId
                    response.putInt(leader); // leaderId
                    response.putInt(replicas.size()); // replicaCount
                    for (int j = 0; j < replicas.size(); j++) {
                        response.putInt(replicas.get(j).getAsInt());
                    }
                }
            }
        } catch (Exception e) {
            response.putInt(0); // topics count
        }
        response.flip();
        while (response.hasRemaining()) {
            clientChannel.write(response);
        }
    }

    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = readString(buffer);
        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();
        
        if (isController.get()) {
            createTopic(topic, numPartitions, replicationFactor);
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put(Protocol.CREATE_TOPIC_RESPONSE);
            response.flip();
            while (response.hasRemaining()) {
                clientChannel.write(response);
            }
        } else {
            Protocol.sendErrorResponse(clientChannel, "Not controller");
        }
    }

    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset, int followerId) {
        try {
            BrokerInfo followerInfo = clusterMetadata.get(followerId);
            if (followerInfo != null) {
                try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(followerInfo.getHost(), followerInfo.getPort()))) {
                    ByteBuffer req = Protocol.encodeReplicateRequest(topic, partition.getId(), offset, message);
                    while (req.hasRemaining()) {
                        channel.write(req);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to replicate to follower " + followerId, e);
        }
    }

    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = readString(buffer);
        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int messageLength = buffer.getInt();
        byte[] message = new byte[messageLength];
        buffer.get(message);

        List<Partition> parts = topics.get(topic);
        if (parts != null && partitionId < parts.size()) {
            Partition partition = parts.get(partitionId);
            partition.append(message);
        }
    }

    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = readString(buffer);
        loadTopic(topic);
    }

    private String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    clientChannel.configureBlocking(true); 
                    executor.submit(() -> handleClient(clientChannel));
                }
                Thread.sleep(10);
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);
                if (bytesRead > 0) {
                    buffer.flip();
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    clientChannel.close();
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Client disconnected or error", e);
        } finally {
            try {
                if (clientChannel.isOpen()) {
                    clientChannel.close();
                }
            } catch (IOException e) { }
        }
    }

    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();
        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    /**
     * Durability for partitions this broker opens from now on (see docs/durability.md).
     * Set before start(); partitions already open keep the mode they were created with.
     */
    public void setDurabilityMode(DurabilityMode durabilityMode) {
        this.durabilityMode = durabilityMode;
    }

    public DurabilityMode getDurabilityMode() {
        return durabilityMode;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: SimpleKafkaBroker <brokerId> <host> <port> <zkPort> [OS_PAGE_CACHE|PHYSICAL_FSYNC]");
            System.exit(1);
        }
        
        int brokerId = Integer.parseInt(args[0]);
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        int zkPort = Integer.parseInt(args[3]);
        
        SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, host, port, zkPort);
        if (args.length >= 5) {
            broker.setDurabilityMode(DurabilityMode.valueOf(args[4]));
        }
        broker.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));
        
        Thread.currentThread().join();
    }
}
