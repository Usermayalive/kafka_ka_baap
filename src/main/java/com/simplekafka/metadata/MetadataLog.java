package com.simplekafka.metadata;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Drop-in replacement for the old {@code ZookeeperClient}: same methods and path layout, backed
 * by the Raft metadata quorum. Broker call sites are unchanged; underneath, path writes become
 * typed {@link MetadataCommand}s.
 *
 * <pre>
 * ZooKeeper call                                    Raft equivalent
 * createEphemeralNode("/brokers/ids/N", json)       REGISTER_BROKER + heartbeats (expiry -> UNREGISTER)
 * createEphemeralNode("/controller", ...)           true iff this node is the Raft leader
 * createPersistentNode("/topics/T", config) and     one atomic CREATE_TOPIC, submitted once the config
 *   ".../partitions/I/state" for each partition       and every partition's state have been written
 * getData / getChildren                             reads of the local metadata image
 * watchChildren / watchNode                         state-machine change / leader-change listeners
 * </pre>
 *
 * Reads are served from this node's applied image, so like ZooKeeper reads they may briefly
 * lag the leader.
 */
public class MetadataLog {
    private static final Logger LOGGER = Logger.getLogger(MetadataLog.class.getName());
    private static final Gson GSON = new Gson();

    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    public interface NodeCallback {
        void onNodeChanged();
    }

    private final MetadataQuorum quorum;
    private final long incarnation = System.currentTimeMillis() * 1_000 + ThreadLocalRandom.current().nextInt(1_000);
    private final Map<String, JsonObject> pendingTopicConfigs = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, JsonObject>> pendingPartitionStates = new ConcurrentHashMap<>();
    private final Set<String> watchedPaths = ConcurrentHashMap.newKeySet();
    private volatile Integer registeredBrokerId;

    public MetadataLog(MetadataQuorum quorum) {
        this.quorum = quorum;
    }

    public MetadataQuorum quorum() {
        return quorum;
    }

    public MetadataImage image() {
        return quorum.image();
    }

    public void connect() throws IOException {
        quorum.start();
    }

    public void createPath(String path) {
        // Paths are implicit in the typed image
    }

    public void createPersistentNode(String path, String data) throws IOException {
        String[] parts = split(path);
        if (parts.length == 2 && parts[0].equals("topics")) {
            pendingTopicConfigs.put(parts[1], GSON.fromJson(data, JsonObject.class));
            maybeSubmitTopic(parts[1]);
        } else if (parts.length == 5 && parts[0].equals("topics") && parts[2].equals("partitions") && parts[4].equals("state")) {
            pendingPartitionStates.computeIfAbsent(parts[1], k -> new ConcurrentHashMap<>())
                    .put(Integer.parseInt(parts[3]), GSON.fromJson(data, JsonObject.class));
            maybeSubmitTopic(parts[1]);
        } else {
            throw new UnsupportedOperationException("No metadata mapping for persistent node " + path);
        }
    }

    /** Submits CREATE_TOPIC once the config and every partition's state are known. */
    private void maybeSubmitTopic(String topic) throws IOException {
        JsonObject config = pendingTopicConfigs.get(topic);
        Map<Integer, JsonObject> states = pendingPartitionStates.getOrDefault(topic, Map.of());
        if (config == null || states.size() < config.get("partitions").getAsInt()) {
            return;
        }
        Map<String, String> configMap = new TreeMap<>();
        config.entrySet().forEach(e -> configMap.put(e.getKey(), e.getValue().getAsString()));
        List<MetadataImage.PartitionState> partitions = new ArrayList<>();
        for (int i = 0; i < states.size(); i++) {
            JsonObject s = states.get(i);
            int leader = s.get("leader").getAsInt();
            List<Integer> replicas = new ArrayList<>();
            s.getAsJsonArray("replicas").forEach(r -> replicas.add(r.getAsInt()));
            List<Integer> isr = new ArrayList<>(); // a new topic's replicas are all empty, hence in sync
            s.getAsJsonArray("isr").forEach(r -> isr.add(r.getAsInt()));
            partitions.add(new MetadataImage.PartitionState(leader, replicas, isr, 0));
        }
        pendingTopicConfigs.remove(topic);
        pendingPartitionStates.remove(topic);
        quorum.submit(MetadataCommand.createTopic(topic, configMap, partitions));
    }

    public boolean createEphemeralNode(String path, String data) throws IOException {
        String[] parts = split(path);
        if (parts.length == 1 && parts[0].equals("controller")) {
            return quorum.isLeader();
        }
        if (parts.length == 3 && parts[0].equals("brokers") && parts[1].equals("ids")) {
            int id = Integer.parseInt(parts[2]);
            JsonObject json = GSON.fromJson(data, JsonObject.class);
            // Heartbeat before registering: the leader's liveness clock starts when the
            // registration commits, but submit() returns only once this node has applied it, which
            // can take seconds when a restarted node is catching up on a divergent log.
            quorum.heartbeatAs(id);
            quorum.submit(MetadataCommand.registerBroker(id, json.get("host").getAsString(), json.get("port").getAsInt(), incarnation));
            registeredBrokerId = id;
            return true;
        }
        throw new UnsupportedOperationException("No metadata mapping for ephemeral node " + path);
    }

    public void watchChildren(String path, ChildrenCallback callback) {
        MetadataImage.Change trigger = switch (path) {
            case "/brokers/ids" -> MetadataImage.Change.BROKERS;
            case "/topics" -> MetadataImage.Change.TOPICS;
            default -> throw new UnsupportedOperationException("No metadata mapping for watch on " + path);
        };
        if (watchedPaths.add(path)) { // persistent watch: register once
            quorum.stateMachine().onChange(change -> {
                if (change == trigger) {
                    callback.onChildrenChanged(getChildren(path));
                }
            });
        }
        callback.onChildrenChanged(getChildren(path));
    }

    public void watchNode(String path, NodeCallback callback) {
        if (!path.equals("/controller")) {
            throw new UnsupportedOperationException("No metadata mapping for watch on " + path);
        }
        if (watchedPaths.add(path)) {
            quorum.stateMachine().onLeaderChange(leader -> callback.onNodeChanged());
        }
    }

    /** Partition leadership/ISR changes (no ZooKeeper equivalent was watched before). */
    public void onPartitionStateChange(Runnable callback) {
        quorum.stateMachine().onChange(change -> {
            if (change == MetadataImage.Change.PARTITION_STATE || change == MetadataImage.Change.TOPICS) {
                callback.run();
            }
        });
    }

    public String getData(String path) {
        String[] parts = split(path);
        MetadataImage image = quorum.image();
        if (parts.length == 1 && parts[0].equals("controller")) {
            JsonObject o = new JsonObject();
            o.addProperty("broker", quorum.leaderId());
            return o.toString();
        }
        if (parts.length == 3 && parts[0].equals("brokers") && parts[1].equals("ids")) {
            MetadataImage.BrokerRegistration b = image.brokers().get(Integer.parseInt(parts[2]));
            if (b == null) {
                throw new NoSuchElementException("No node " + path);
            }
            JsonObject o = new JsonObject();
            o.addProperty("host", b.host());
            o.addProperty("port", b.port());
            return o.toString();
        }
        if (parts.length >= 2 && parts[0].equals("topics")) {
            MetadataImage.TopicImage t = image.topicImage(parts[1]);
            if (t == null) {
                throw new NoSuchElementException("No node " + path);
            }
            if (parts.length == 2) {
                JsonObject o = new JsonObject();
                t.config().forEach((k, v) -> o.addProperty(k, v));
                o.addProperty("partitions", t.partitions().size());
                return o.toString();
            }
            if (parts.length == 5 && parts[2].equals("partitions") && parts[4].equals("state")) {
                MetadataImage.PartitionState p = t.partitions().get(Integer.parseInt(parts[3]));
                JsonObject o = new JsonObject();
                o.addProperty("leader", p.leader());
                o.add("isr", toJson(p.isr()));
                o.add("replicas", toJson(p.replicas()));
                o.addProperty("leader_epoch", p.leaderEpoch());
                return o.toString();
            }
        }
        throw new NoSuchElementException("No node " + path);
    }

    public List<String> getChildren(String path) {
        return switch (path) {
            case "/topics" -> new ArrayList<>(quorum.image().topics().keySet());
            case "/brokers/ids" -> quorum.image().brokers().keySet().stream().map(String::valueOf).toList();
            default -> throw new UnsupportedOperationException("No metadata mapping for children of " + path);
        };
    }

    public void electLeader(String topic, int partition, int leader) throws IOException {
        quorum.submit(MetadataCommand.electLeader(topic, partition, leader));
    }

    public void updateIsr(String topic, int partition, int leader, List<Integer> isr) throws IOException {
        quorum.submit(MetadataCommand.updateIsr(topic, partition, leader, isr));
    }

    /** Elects {@code leader} and sets the ISR in one step (the controller's failover). */
    public void electLeader(String topic, int partition, int leader, List<Integer> isr) throws IOException {
        MetadataCommand c = MetadataCommand.electLeader(topic, partition, leader);
        c.isr = isr;
        quorum.submit(c);
    }

    /** Graceful shutdown: unregister (as closing a ZooKeeper session deleted ephemeral nodes). */
    public void close() {
        Integer id = registeredBrokerId;
        if (id != null) {
            // Bounded: if this is the last quorum member up, no majority can commit it
            if (!quorum.trySubmit(MetadataCommand.unregisterBroker(id, incarnation), 2_000)) {
                LOGGER.fine("Unregister on shutdown did not commit; the leader will expire this broker");
            }
        }
        try {
            quorum.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error closing metadata quorum", e);
        }
    }

    private static String[] split(String path) {
        return Arrays.stream(path.split("/")).filter(s -> !s.isEmpty()).toArray(String[]::new);
    }

    private static JsonArray toJson(List<Integer> values) {
        JsonArray a = new JsonArray();
        values.forEach(a::add);
        return a;
    }
}
