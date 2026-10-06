package com.simplekafka.client;

import com.simplekafka.coordinator.GroupCoordinator.TopicPartition;
import com.simplekafka.network.KafkaFrame;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A consumer group member: subscribes to topics, lets the broker-side coordinator assign
 * partitions, and runs one {@link SimpleKafkaConsumer} per owned partition.
 *
 * Rebalances are cooperative: on each heartbeat that reports {@code needsSync}, the member
 * syncs, stops (and commits) only the partitions it lost, and starts only the ones it gained.
 * Partitions it keeps are never interrupted.
 *
 * Static membership is the default: the instance ID defaults to the client ID, so a restart
 * within the session timeout resumes the same assignment without a rebalance. {@link #close()}
 * therefore does not leave the group; use {@link #leaveGroup()} for a permanent departure.
 */
public class GroupConsumer implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(GroupConsumer.class.getName());

    private final SimpleKafkaClient client;
    private final String groupId;
    private final String instanceId;
    private final Set<String> topics;
    private final MessageHandler handler;
    private final int sessionTimeoutMs;
    private final long heartbeatIntervalMs;

    private final Map<TopicPartition, SimpleKafkaConsumer> owned = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile String memberId = "";
    private volatile int generation;
    private Thread heartbeatThread;

    public GroupConsumer(SimpleKafkaClient client, String groupId, String instanceId, Set<String> topics,
                         MessageHandler handler, int sessionTimeoutMs, long heartbeatIntervalMs) {
        this.client = client;
        this.groupId = groupId;
        this.instanceId = instanceId == null ? "" : instanceId;
        this.topics = new TreeSet<>(topics);
        this.handler = handler;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public void start() throws IOException {
        join();
        running.set(true);
        heartbeatThread = new Thread(this::heartbeatLoop, "group-heartbeat-" + groupId + "-" + instanceId);
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    public Set<TopicPartition> assignment() {
        return new TreeSet<>(owned.keySet());
    }

    public String memberId() {
        return memberId;
    }

    public int generation() {
        return generation;
    }

    private void join() throws IOException {
        DataInputStream in = client.coordinatorRequest(groupId, KafkaFrame.API_KEY_JOIN_GROUP, SimpleKafkaClient.payload(o -> {
            o.writeUTF("group-consumer");
            o.writeUTF(groupId);
            o.writeUTF(memberId);
            o.writeUTF(instanceId);
            o.writeInt(sessionTimeoutMs);
            o.writeInt(topics.size());
            for (String t : topics) {
                o.writeUTF(t);
            }
        }));
        memberId = in.readUTF();
        generation = in.readInt();
    }

    private void heartbeatLoop() {
        while (running.get()) {
            try {
                DataInputStream in = client.coordinatorRequest(groupId, KafkaFrame.API_KEY_HEARTBEAT, ownedRequest());
                generation = in.readInt();
                if (in.readBoolean()) {
                    sync();
                }
            } catch (BrokerErrorException e) {
                if (e.getErrorCode() == KafkaFrame.ERROR_UNKNOWN_MEMBER) {
                    // Expired or replaced: our partitions may already be someone else's
                    LOGGER.warning("Group " + groupId + ": member " + memberId + " fenced; revoking all and rejoining");
                    revoke(new TreeSet<>(owned.keySet()));
                    memberId = "";
                    tryRejoin();
                } else {
                    LOGGER.log(Level.WARNING, "Heartbeat failed for group " + groupId, e);
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Heartbeat failed for group " + groupId, e);
            }
            try {
                Thread.sleep(heartbeatIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void tryRejoin() {
        try {
            join();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Rejoin of group " + groupId + " failed; will retry", e);
        }
    }

    private void sync() throws IOException {
        DataInputStream in = client.coordinatorRequest(groupId, KafkaFrame.API_KEY_SYNC_GROUP, ownedRequest());
        generation = in.readInt();
        Set<TopicPartition> assigned = new TreeSet<>();
        for (int n = in.readInt(); n > 0; n--) {
            assigned.add(new TopicPartition(in.readUTF(), in.readInt()));
        }
        Set<TopicPartition> lost = new TreeSet<>(owned.keySet());
        lost.removeAll(assigned);
        Set<TopicPartition> gained = new TreeSet<>(assigned);
        gained.removeAll(owned.keySet());
        revoke(lost);
        for (TopicPartition tp : gained) {
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(client, tp.topic(), tp.partition(), groupId);
            consumer.startConsuming(handler);
            owned.put(tp, consumer);
        }
        if (!lost.isEmpty() || !gained.isEmpty()) {
            LOGGER.info("Group " + groupId + " member " + memberId + " gen " + generation + ": +" + gained + " -" + lost);
        }
    }

    /** Stops consumption and commits progress before the partition is reported as released. */
    private void revoke(Set<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            SimpleKafkaConsumer consumer = owned.remove(tp);
            if (consumer != null) {
                consumer.stopConsuming();
                try {
                    consumer.commitSync();
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Final commit for " + tp + " failed; the next owner may reprocess", e);
                }
            }
        }
    }

    private byte[] ownedRequest() throws IOException {
        Set<TopicPartition> snapshot = new TreeSet<>(owned.keySet());
        return SimpleKafkaClient.payload(o -> {
            o.writeUTF("group-consumer");
            o.writeUTF(groupId);
            o.writeUTF(memberId);
            o.writeInt(snapshot.size());
            for (TopicPartition tp : snapshot) {
                o.writeUTF(tp.topic());
                o.writeInt(tp.partition());
            }
        });
    }

    /** Stops consuming and commits, but stays in the group until the session times out (static membership). */
    @Override
    public void close() {
        running.set(false);
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
            try {
                heartbeatThread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        revoke(new TreeSet<>(owned.keySet()));
    }

    /** Leaves permanently: partitions are reassigned immediately. */
    public void leaveGroup() throws IOException {
        close();
        client.coordinatorRequest(groupId, KafkaFrame.API_KEY_LEAVE_GROUP, SimpleKafkaClient.payload(o -> {
            o.writeUTF("group-consumer");
            o.writeUTF(groupId);
            o.writeUTF(memberId);
        }));
    }

    /** Simulates a crash: stops heartbeating and consuming without committing or leaving. */
    void abandonForTest() {
        running.set(false);
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        owned.values().forEach(SimpleKafkaConsumer::stopConsuming);
        owned.clear();
    }
}
