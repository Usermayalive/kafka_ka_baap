package com.simplekafka.broker;


import com.google.gson.Gson;
import com.simplekafka.coordinator.ConsumerOffsetStore;
import com.simplekafka.coordinator.GroupCoordinator;
import com.simplekafka.network.ClientQuotaManager;
import com.simplekafka.observability.AdminHttpServer;
import com.simplekafka.observability.AuditLog;
import com.simplekafka.observability.BrokerMetrics;
import com.simplekafka.network.IdempotenceManager;
import com.simplekafka.network.NettyBrokerServer;
import com.simplekafka.network.ProducerIdManager;
import com.simplekafka.storage.WriteAheadLog.DurabilityMode;
import com.simplekafka.tiered.S3RemoteStorageManager;
import com.simplekafka.tiered.TieredStorageConfig;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleKafkaBroker {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = System.getProperty("stormlog.data.dir", "kafka-data");
    
    private final int brokerId;

    public int getBrokerId() {
        return brokerId;
    }

    private final String brokerHost;
    private final int brokerPort;
    private final Map<String, List<Partition>> topics;
    private NettyBrokerServer nettyServer;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isController;
    private final Map<Integer, BrokerInfo> clusterMetadata;
    private final ZookeeperClient zkClient;
    private final Gson gson;
    private final ConcurrentHashMap<String, java.util.concurrent.CompletableFuture<Void>> topicCreationGuards = new ConcurrentHashMap<>();
    private volatile DurabilityMode durabilityMode = DurabilityMode.OS_PAGE_CACHE;
    private volatile TieredStorageConfig tieredStorage;
    private final ConsumerOffsetStore offsetStore = new ConsumerOffsetStore(p -> hostedPartition(ConsumerOffsetStore.TOPIC, p));
    private final GroupCoordinator groupCoordinator = new GroupCoordinator(this::partitionCount,
            (event, details) -> audit().record(event, details));
    private final BrokerMetrics metrics;
    private volatile AuditLog auditLog;
    private volatile AdminHttpServer adminServer;
    private volatile int adminPort = -1;

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();
        this.gson = new Gson();
        this.metrics = new BrokerMetrics(brokerId);
        metrics.gauge("stormlog.topics", "Topics with partitions hosted on this broker", topics::size);
        metrics.gauge("stormlog.consumer.groups", "Consumer groups coordinated by this broker",
                () -> groupCoordinator.groupIds().size());
        metrics.setLagSource(this::lagByGroupAndTopic);
        
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
        LOGGER.info("Starting broker " + brokerId + " on " + brokerHost + ":" + brokerPort);

        nettyServer = new NettyBrokerServer(brokerHost, brokerPort, this, new ClientQuotaManager(),
                new IdempotenceManager(),
                new ProducerIdManager((short) brokerId, new File(DATA_DIR + File.separator + brokerId, "producer_id_epoch")),
                NettyBrokerServer.DEFAULT_REQUEST_QUEUE_CAPACITY);
        nettyServer.start();
        int resolvedAdminPort = adminPort >= 0 ? adminPort : (brokerPort == 0 ? 0 : brokerPort + 1000);
        adminServer = new AdminHttpServer(brokerHost, resolvedAdminPort, metrics, this::partitionLag);
        LOGGER.info("Admin HTTP (/metrics, /admin/lag) on " + brokerHost + ":" + adminServer.port());

        zkClient.connect();
        registerWithZookeeper();
        electController();
        loadExistingTopics();
        offsetStore.loadAllHosted();
        audit().record("BROKER_STARTED", java.util.Map.of("host", brokerHost, "port", getPort()));
    }

    public void stop() {
        isRunning.set(false);
        try {
            if (nettyServer != null) {
                nettyServer.stop();
            }
            if (adminServer != null) {
                adminServer.close();
            }
            groupCoordinator.close();
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
            if (topic.endsWith(".dlq") || topic.equals(ConsumerOffsetStore.TOPIC)) {
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
            audit().record("TOPIC_CREATED", java.util.Map.of("topic", topic, "partitions", numPartitions,
                    "replicationFactor", replicationFactor));

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
                        boolean compacted = config.has("cleanup.policy")
                                && "compact".equalsIgnoreCase(config.get("cleanup.policy").getAsString());
                        // Compacted topics stay local: compaction rewrites whole partitions (Kafka has the same limit)
                        Partition partition = new Partition(i, baseDir, durabilityMode, compacted ? null : tieredStorage);
                        partition.setCompacted(compacted);
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

    /** Brokers currently registered in cluster metadata; just this broker when running standalone. */
    public List<BrokerInfo> liveBrokers() {
        if (clusterMetadata.isEmpty()) {
            return List.of(new BrokerInfo(brokerId, brokerHost, getPort()));
        }
        return new ArrayList<>(clusterMetadata.values());
    }

    public record PartitionDescription(int partitionId, int leader, List<Integer> replicas) {}

    public record TopicDescription(String name, List<PartitionDescription> partitions) {}

    /**
     * Cluster-wide topic layout from the metadata store, falling back to the partitions this
     * broker hosts when the store is unreachable (e.g. standalone/test brokers).
     */
    public List<TopicDescription> describeTopics() {
        try {
            List<TopicDescription> result = new ArrayList<>();
            for (String topicName : zkClient.getChildren("/topics")) {
                JsonObject config = gson.fromJson(zkClient.getData("/topics/" + topicName), JsonObject.class);
                int numPartitions = config.get("partitions").getAsInt();
                List<PartitionDescription> partitions = new ArrayList<>();
                for (int i = 0; i < numPartitions; i++) {
                    JsonObject state = gson.fromJson(
                            zkClient.getData("/topics/" + topicName + "/partitions/" + i + "/state"), JsonObject.class);
                    List<Integer> replicas = new ArrayList<>();
                    state.getAsJsonArray("replicas").forEach(e -> replicas.add(e.getAsInt()));
                    partitions.add(new PartitionDescription(i, state.get("leader").getAsInt(), replicas));
                }
                result.add(new TopicDescription(topicName, partitions));
            }
            return result;
        } catch (Exception e) {
            List<TopicDescription> local = new ArrayList<>();
            for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
                List<PartitionDescription> partitions = new ArrayList<>();
                for (Partition p : entry.getValue()) {
                    int leader = p.getLeader() == 0 ? brokerId : p.getLeader();
                    partitions.add(new PartitionDescription(p.getId(), leader, List.of(leader)));
                }
                local.add(new TopicDescription(entry.getKey(), partitions));
            }
            return local;
        }
    }

    public BrokerMetrics getMetrics() {
        return metrics;
    }

    /** Admin HTTP port; -1 (default) means broker port + 1000, 0 means ephemeral. Set before start(). */
    public void setAdminPort(int adminPort) {
        this.adminPort = adminPort;
    }

    public int getAdminPort() {
        return adminServer != null ? adminServer.port() : -1;
    }

    /**
     * The audit log, backed by this broker's local {@code __audit} partition (created on first
     * use and exposed as a topic so it can be consumed like any other).
     */
    public AuditLog audit() {
        AuditLog log = auditLog;
        if (log == null) {
            synchronized (this) {
                if (auditLog == null) {
                    auditLog = new AuditLog(brokerId, () -> {
                        Partition p = hostedPartition(AuditLog.TOPIC, 0);
                        if (p == null) {
                            synchronized (topics) {
                                p = hostedPartition(AuditLog.TOPIC, 0);
                                if (p == null) {
                                    p = new Partition(0, dataDir() + File.separator + AuditLog.TOPIC, durabilityMode);
                                    topics.put(AuditLog.TOPIC, new ArrayList<>(List.of(p)));
                                }
                            }
                        }
                        return p;
                    });
                }
                log = auditLog;
            }
        }
        return log;
    }

    /** Root directory for this broker's partitions; overridable for tests. */
    protected String dataDir() {
        return DATA_DIR + File.separator + brokerId;
    }

    /** Lag per (group, topic) over locally hosted partitions with a committed offset. */
    private List<BrokerMetrics.LagRow> lagByGroupAndTopic() {
        Map<String, Long> sums = new java.util.TreeMap<>();
        for (AdminHttpServer.PartitionLag row : partitionLag(null)) {
            sums.merge(row.group() + "\u0000" + row.topic(), row.lag(), Long::sum);
        }
        List<BrokerMetrics.LagRow> rows = new ArrayList<>();
        sums.forEach((key, lag) -> {
            String[] parts = key.split("\u0000", 2);
            rows.add(new BrokerMetrics.LagRow(parts[0], parts[1], lag));
        });
        return rows;
    }

    /** Per-partition lag for one group (or all groups when null), locally hosted partitions only. */
    public List<AdminHttpServer.PartitionLag> partitionLag(String group) {
        List<AdminHttpServer.PartitionLag> rows = new ArrayList<>();
        for (ConsumerOffsetStore.CommittedOffset c : offsetStore.snapshot()) {
            if (group != null && !group.equals(c.group())) {
                continue;
            }
            Partition p = hostedPartition(c.topic(), c.partition());
            if (p != null) {
                long end = p.getNextOffset();
                rows.add(new AdminHttpServer.PartitionLag(c.group(), c.topic(), c.partition(), c.offset(), end,
                        Math.max(0, end - c.offset())));
            }
        }
        rows.sort(java.util.Comparator.comparing(AdminHttpServer.PartitionLag::group)
                .thenComparing(AdminHttpServer.PartitionLag::topic)
                .thenComparingInt(AdminHttpServer.PartitionLag::partition));
        return rows;
    }

    public GroupCoordinator getGroupCoordinator() {
        return groupCoordinator;
    }

    /** Partition count of a topic from cluster metadata, or 0 if it doesn't exist. */
    public int partitionCount(String topic) {
        for (TopicDescription t : describeTopics()) {
            if (t.name().equals(topic)) {
                return t.partitions().size();
            }
        }
        return 0;
    }

    public ConsumerOffsetStore getOffsetStore() {
        return offsetStore;
    }

    /** This broker's replica of topic-partition, or null if it does not host it. */
    public Partition hostedPartition(String topic, int partitionId) {
        List<Partition> parts = topics.get(topic);
        if (parts == null) {
            return null;
        }
        for (Partition p : parts) {
            if (p.getId() == partitionId) {
                return p;
            }
        }
        return null;
    }

    /**
     * The broker coordinating a consumer group: the leader of its {@code __consumer_offsets}
     * partition. The controller creates the topic on first use. Returns null while unavailable.
     */
    public BrokerInfo coordinatorFor(String group) {
        int offsetsPartition = ConsumerOffsetStore.partitionFor(group);
        for (int attempt = 0; attempt < 2; attempt++) {
            for (TopicDescription topic : describeTopics()) {
                if (!topic.name().equals(ConsumerOffsetStore.TOPIC)) {
                    continue;
                }
                for (PartitionDescription p : topic.partitions()) {
                    if (p.partitionId() == offsetsPartition) {
                        for (BrokerInfo b : liveBrokers()) {
                            if (b.getId() == p.leader()) {
                                return b;
                            }
                        }
                        return null; // leader is down; no replication to fail over to
                    }
                }
            }
            if (attempt == 0 && isController()) {
                createTopic(ConsumerOffsetStore.TOPIC, ConsumerOffsetStore.PARTITIONS, (short) 1);
            } else {
                break;
            }
        }
        return null;
    }

    public boolean isController() {
        return isController.get();
    }

    /** Client-facing port: the Netty server's bound port once started (resolves port 0). */
    public int getPort() {
        return nettyServer != null ? nettyServer.getPort() : brokerPort;
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

    /** Remote tier for sealed segments of non-compacted topics opened from now on; null disables. */
    public void setTieredStorage(TieredStorageConfig tieredStorage) {
        this.tieredStorage = tieredStorage;
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
        String s3Endpoint = System.getenv("STORMLOG_S3_ENDPOINT");
        if (s3Endpoint != null && !s3Endpoint.isBlank()) {
            S3RemoteStorageManager s3 = S3RemoteStorageManager.create(s3Endpoint,
                    System.getenv().getOrDefault("STORMLOG_S3_REGION", "us-east-1"),
                    System.getenv("STORMLOG_S3_ACCESS_KEY"), System.getenv("STORMLOG_S3_SECRET_KEY"),
                    System.getenv().getOrDefault("STORMLOG_S3_BUCKET", "stormlog"));
            s3.ensureBucket().join();
            broker.setTieredStorage(new TieredStorageConfig(s3,
                    Integer.parseInt(System.getenv().getOrDefault("STORMLOG_LOCAL_RETAINED_SEGMENTS", "2")),
                    Integer.parseInt(System.getenv().getOrDefault("STORMLOG_REMOTE_CACHE_SEGMENTS", "8"))));
        }
        broker.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));
        
        Thread.currentThread().join();
    }
}
