package com.simplekafka.broker;

import com.simplekafka.client.BrokerConnection;
import com.simplekafka.client.BrokerErrorException;
import com.simplekafka.metadata.MetadataImage;
import com.simplekafka.network.IdempotenceManager;
import com.simplekafka.network.KafkaFrame;
import com.simplekafka.network.ProducerState;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Data replication, Kafka-style: followers pull from the leader.
 *
 * <ul>
 *   <li><b>Followers</b> run one fetcher per followed partition. Each fetch carries the
 *       follower's log end (which acknowledges everything before it) and its latest leader
 *       epoch. If the leader reports that the follower's log diverged (a tail written under an
 *       epoch the new leader never saw), the follower truncates to the divergence point first.
 *       Records are appended at the leader's offsets, byte-for-byte (PII stays encrypted).</li>
 *   <li><b>Leaders</b> track each follower's log end. The high watermark is the minimum log end
 *       across the in-sync replicas (ISR); consumers read only below it and acks=all produces
 *       complete once it passes their offset. A follower that hasn't caught up for
 *       {@link #REPLICA_LAG_MAX_MS} is removed from the ISR, and re-added once it reaches the HW,
 *       both through the Raft metadata quorum.</li>
 *   <li>The controller elects new leaders only from the ISR, so a committed record (below the
 *       HW) survives any failure that leaves one in-sync replica.</li>
 * </ul>
 */
public class ReplicaManager implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(ReplicaManager.class.getName());

    static final long REPLICA_LAG_MAX_MS = Long.getLong("stormlog.replica.lag.max.ms", 10_000);
    static final int FETCH_MAX_BYTES = 1 << 20;
    static final int FETCH_MAX_WAIT_MS = 500;

    record TopicPartition(String topic, int partition) {}

    /** Leader-side view of one partition's followers. */
    final class LeaderState {
        final TopicPartition tp;
        final Partition partition;
        final int epoch;
        final List<Integer> replicas;
        volatile Set<Integer> isr;
        final Map<Integer, Long> followerLogEnd = new ConcurrentHashMap<>();
        final Map<Integer, Long> lastCaughtUpNanos = new ConcurrentHashMap<>();
        /** Leader log end when each follower last fetched (Kafka's lastFetchLeaderLogEndOffset). */
        final Map<Integer, Long> leaderLogEndAtLastFetch = new ConcurrentHashMap<>();
        final long becameLeaderNanos = System.nanoTime();
        volatile boolean isrChangeInFlight;

        LeaderState(TopicPartition tp, Partition partition, int epoch, List<Integer> replicas, Collection<Integer> isr) {
            this.tp = tp;
            this.partition = partition;
            this.epoch = epoch;
            this.replicas = List.copyOf(replicas);
            this.isr = Set.copyOf(isr);
        }

        void onFollowerFetch(int followerId, long followerLogEnd) {
            long leaderLogEnd = partition.getNextOffset();
            followerLogEnd = Math.min(followerLogEnd, leaderLogEnd);
            this.followerLogEnd.put(followerId, followerLogEnd);
            // Caught up = reached where the leader was at this follower's previous fetch. Comparing
            // with the current log end would evict healthy followers under steady writes, since
            // the leader keeps appending between fetches.
            Long previousLeaderLogEnd = leaderLogEndAtLastFetch.put(followerId, leaderLogEnd);
            if (followerLogEnd >= leaderLogEnd || (previousLeaderLogEnd != null && followerLogEnd >= previousLeaderLogEnd)) {
                lastCaughtUpNanos.put(followerId, System.nanoTime());
            }
            advanceHighWatermark();
            if (!isr.contains(followerId) && followerLogEnd >= partition.getHighWatermark()) {
                Set<Integer> expanded = new TreeSet<>(isr);
                expanded.add(followerId);
                proposeIsr(this, expanded, "follower " + followerId + " caught up to the high watermark");
            }
        }

        /** HW = min log end over the ISR (the leader's own log end included). Never moves backwards. */
        void advanceHighWatermark() {
            long hw = partition.getNextOffset();
            for (int member : isr) {
                if (member != brokerId) {
                    hw = Math.min(hw, followerLogEnd.getOrDefault(member, 0L));
                }
            }
            if (hw > partition.getHighWatermark()) {
                partition.setHighWatermark(hw);
            }
        }
    }

    private final int brokerId;
    private final SimpleKafkaBroker broker;
    private final Map<TopicPartition, LeaderState> leaders = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Fetcher> fetchers = new ConcurrentHashMap<>();
    private final ExecutorService isrProposals = Executors.newSingleThreadExecutor(daemon("isr-proposals"));
    private final ScheduledExecutorService isrShrinker = Executors.newSingleThreadScheduledExecutor(daemon("isr-shrinker"));
    private volatile Consumer<TopicPartition> onBecomeLeader = tp -> {};
    private volatile boolean fetchingPausedForTest;

    ReplicaManager(SimpleKafkaBroker broker) {
        this.broker = broker;
        this.brokerId = broker.getBrokerId();
        isrShrinker.scheduleWithFixedDelay(this::shrinkLaggingIsrs, 1, 1, TimeUnit.SECONDS);
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }

    /** Hook for state that must be rebuilt when this broker takes over a partition (e.g. offsets). */
    void onBecomeLeader(Consumer<TopicPartition> hook) {
        this.onBecomeLeader = hook;
    }

    /** Reconciles leader/follower roles with the metadata image. Runs on the controller-events thread. */
    synchronized void onMetadataUpdate(MetadataImage image) {
        Set<TopicPartition> seen = new HashSet<>();
        for (MetadataImage.TopicImage topic : image.topics().values()) {
            for (int i = 0; i < topic.partitions().size(); i++) {
                MetadataImage.PartitionState state = topic.partitions().get(i);
                if (!state.replicas().contains(brokerId)) {
                    continue;
                }
                Partition partition = broker.hostedPartition(topic.name(), i);
                if (partition == null) {
                    continue;
                }
                TopicPartition tp = new TopicPartition(topic.name(), i);
                seen.add(tp);
                if (state.leader() == brokerId) {
                    becomeLeader(tp, partition, state);
                } else if (state.leader() >= 0) {
                    becomeFollower(tp, partition, state);
                } else {
                    stopFetcher(tp);
                    leaders.remove(tp);
                    partition.setAppendListener(null);
                }
            }
        }
        for (TopicPartition tp : new ArrayList<>(fetchers.keySet())) {
            if (!seen.contains(tp)) {
                stopFetcher(tp);
            }
        }
        leaders.keySet().retainAll(seen);
    }

    private void becomeLeader(TopicPartition tp, Partition partition, MetadataImage.PartitionState state) {
        LeaderState current = leaders.get(tp);
        if (current != null && current.epoch == state.leaderEpoch()) {
            current.isr = Set.copyOf(state.isr()); // ISR change committed
            current.isrChangeInFlight = false;
            current.advanceHighWatermark();
            return;
        }
        stopFetcher(tp);
        partition.leaderEpochs().assign(state.leaderEpoch(), partition.getNextOffset());
        if (state.replicas().size() > 1) {
            // Our HW as last learned from the previous leader (0 if we never replicated); it
            // advances as followers fetch. Records above it may still be uncommitted.
            partition.setHighWatermark(partition.isReplicated() ? partition.getHighWatermark() : 0);
        }
        partition.setTieringEnabled(true);
        LeaderState leader = new LeaderState(tp, partition, state.leaderEpoch(), state.replicas(), state.isr());
        leaders.put(tp, leader);
        partition.setAppendListener(leader::advanceHighWatermark);
        leader.advanceHighWatermark();
        LOGGER.info("Broker " + brokerId + " is leader of " + tp.topic() + "-" + tp.partition()
                + " at epoch " + state.leaderEpoch() + " (ISR " + state.isr() + ", log end " + partition.getNextOffset() + ")");
        onBecomeLeader.accept(tp);
    }

    private void becomeFollower(TopicPartition tp, Partition partition, MetadataImage.PartitionState state) {
        leaders.remove(tp);
        partition.setAppendListener(null);
        partition.setTieringEnabled(false);
        Fetcher existing = fetchers.get(tp);
        if (existing != null && existing.leaderId == state.leader() && existing.leaderEpoch == state.leaderEpoch()) {
            return;
        }
        stopFetcher(tp);
        Fetcher fetcher = new Fetcher(tp, partition, state.leader(), state.leaderEpoch());
        fetchers.put(tp, fetcher);
        fetcher.thread = Thread.ofVirtual().name("replica-fetcher-" + tp.topic() + "-" + tp.partition()).start(fetcher);
        LOGGER.info("Broker " + brokerId + " follows broker " + state.leader() + " for " + tp.topic() + "-" + tp.partition()
                + " at epoch " + state.leaderEpoch());
    }

    private void stopFetcher(TopicPartition tp) {
        Fetcher f = fetchers.remove(tp);
        if (f != null) {
            f.running = false;
            if (f.thread != null) {
                f.thread.interrupt();
            }
            f.closeConnection();
        }
    }

    // ---------------------------------------------------------------------------------
    // Leader side: replica fetch handling, ISR management
    // ---------------------------------------------------------------------------------

    public record FetchRequest(int replicaId, String topic, int partition, int leaderEpoch, long fetchOffset,
                               int lastFetchedEpoch, int maxBytes, int maxWaitMs) {}

    /**
     * Request: [replicaId:4][topic][partition:4][leaderEpoch:4][fetchOffset:8][lastFetchedEpoch:4][maxBytes:4][maxWaitMs:4]
     * Response: [error][leaderEpoch:4][highWatermark:8][divergingEndOffset:8 (-1 = none)]
     *           [epochCount:4]{[epoch:4][startOffset:8]}[recordCount:4]{[offset:8][len:4][stored bytes]}
     */
    public KafkaFrame handleReplicaFetch(KafkaFrame frame) throws IOException, InterruptedException {
        DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(frame.getPayload()));
        FetchRequest req = new FetchRequest(in.readInt(), in.readUTF(), in.readInt(), in.readInt(), in.readLong(),
                in.readInt(), in.readInt(), in.readInt());
        LeaderState leader = leaders.get(new TopicPartition(req.topic(), req.partition()));
        if (leader == null || req.leaderEpoch() > leader.epoch) {
            return error(frame, KafkaFrame.ERROR_NOT_LEADER, "Broker " + brokerId + " is not the leader of "
                    + req.topic() + "-" + req.partition() + (leader == null ? "" : " at epoch " + req.leaderEpoch()));
        }
        Partition partition = leader.partition;

        // Divergence: the follower's latest epoch ended earlier in our log than its log end
        if (req.lastFetchedEpoch() >= 0 && req.lastFetchedEpoch() < leader.epoch) {
            long epochEnd = partition.leaderEpochs().endOffsetFor(req.lastFetchedEpoch(), partition.getNextOffset());
            long truncateTo = epochEnd >= 0 ? epochEnd : partition.getHighWatermark();
            if (truncateTo < req.fetchOffset()) {
                return response(frame, leader, truncateTo, List.of(), List.of(), List.of());
            }
        }

        leader.onFollowerFetch(req.replicaId(), req.fetchOffset());
        if (req.fetchOffset() >= partition.getNextOffset()) {
            partition.awaitAppendBeyond(req.fetchOffset(), Math.min(req.maxWaitMs(), FETCH_MAX_WAIT_MS));
        }
        // Producer state and log end are captured together; only records below that log end are
        // sent, so every idempotent record in this response has its producer's state in it too
        IdempotenceManager.ReplicationSnapshot producers = broker.getIdempotenceManager()
                .snapshotForReplication(req.topic(), req.partition(), partition::getNextOffset);
        List<Partition.Record> records = partition.readRecords(req.fetchOffset(),
                Math.min(req.maxBytes(), FETCH_MAX_BYTES), producers.logEndOffset());
        return response(frame, leader, -1, partition.leaderEpochs().entries(), records, producers.states());
    }

    private KafkaFrame response(KafkaFrame frame, LeaderState leader, long divergingEnd,
                                List<LeaderEpochCache.Entry> epochs, List<Partition.Record> records,
                                List<ProducerState> producers) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeShort(KafkaFrame.ERROR_NONE);
        out.writeInt(leader.epoch);
        out.writeLong(leader.partition.getHighWatermark());
        out.writeLong(divergingEnd);
        out.writeInt(epochs.size());
        for (LeaderEpochCache.Entry e : epochs) {
            out.writeInt(e.epoch());
            out.writeLong(e.startOffset());
        }
        out.writeInt(records.size());
        for (Partition.Record r : records) {
            out.writeLong(r.offset());
            out.writeInt(r.payload().length);
            out.write(r.payload());
        }
        out.writeInt(producers.size());
        for (ProducerState p : producers) {
            p.writeTo(out);
        }
        return new KafkaFrame(frame.getApiKey(), frame.getApiVersion(), frame.getCorrelationId(), bytes.toByteArray());
    }

    private static KafkaFrame error(KafkaFrame frame, short code, String message) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeShort(code);
        out.writeUTF(message);
        return new KafkaFrame(frame.getApiKey(), frame.getApiVersion(), frame.getCorrelationId(), bytes.toByteArray());
    }

    private void shrinkLaggingIsrs() {
        long now = System.nanoTime();
        for (LeaderState leader : leaders.values()) {
            Set<Integer> keep = new TreeSet<>();
            List<Integer> dropped = new ArrayList<>();
            for (int member : leader.isr) {
                long last = leader.lastCaughtUpNanos.getOrDefault(member, leader.becameLeaderNanos);
                if (member == brokerId || now - last <= TimeUnit.MILLISECONDS.toNanos(REPLICA_LAG_MAX_MS)) {
                    keep.add(member);
                } else {
                    dropped.add(member);
                }
            }
            if (!dropped.isEmpty()) {
                proposeIsr(leader, keep, "followers " + dropped + " not caught up for > " + REPLICA_LAG_MAX_MS + " ms");
            }
        }
    }

    private void proposeIsr(LeaderState leader, Set<Integer> isr, String reason) {
        if (leader.isrChangeInFlight) {
            return;
        }
        leader.isrChangeInFlight = true;
        isrProposals.execute(() -> {
            try {
                LOGGER.info("ISR of " + leader.tp.topic() + "-" + leader.tp.partition() + ": " + leader.isr + " -> " + isr
                        + " (" + reason + ")");
                broker.getMetadataLog().updateIsr(leader.tp.topic(), leader.tp.partition(), brokerId, new ArrayList<>(isr));
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "ISR change for " + leader.tp + " rejected or failed", e);
                leader.isrChangeInFlight = false; // retried on the next fetch / shrink check
            }
        });
    }

    /** Current in-sync replicas of a partition this broker leads, or null if it isn't the leader. */
    public Set<Integer> isrOf(String topic, int partition) {
        LeaderState leader = leaders.get(new TopicPartition(topic, partition));
        return leader == null ? null : leader.isr;
    }

    // ---------------------------------------------------------------------------------
    // Follower side
    // ---------------------------------------------------------------------------------

    final class Fetcher implements Runnable {
        final TopicPartition tp;
        final Partition partition;
        final int leaderId;
        final int leaderEpoch;
        volatile boolean running = true;
        volatile Thread thread;
        /**
         * Each fetcher has its own connection. The leader handles a connection's requests in order,
         * so on a shared connection a fetch for a busy partition would wait behind 500 ms
         * long-polls for idle ones. (Kafka instead batches all partitions into one fetch.)
         */
        private BrokerConnection connection;

        void closeConnection() {
            BrokerConnection c = connection;
            connection = null;
            if (c != null) {
                c.close();
            }
        }

        Fetcher(TopicPartition tp, Partition partition, int leaderId, int leaderEpoch) {
            this.tp = tp;
            this.partition = partition;
            this.leaderId = leaderId;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    fetchOnce();
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    if (!running) {
                        return;
                    }
                    if (!(e instanceof BrokerErrorException)) {
                        closeConnection();
                    }
                    LOGGER.log(Level.FINE, "Replica fetch of " + tp + " from broker " + leaderId + " failed", e);
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }
        }

        private void fetchOnce() throws IOException, InterruptedException {
            while (fetchingPausedForTest) {
                Thread.sleep(50);
            }
            if (connection == null || !connection.isActive()) {
                connection = connectTo(leaderId);
            }
            BrokerConnection conn = connection;
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(brokerId);
            out.writeUTF(tp.topic());
            out.writeInt(tp.partition());
            out.writeInt(leaderEpoch);
            out.writeLong(partition.getNextOffset());
            out.writeInt(partition.leaderEpochs().latestEpoch());
            out.writeInt(FETCH_MAX_BYTES);
            out.writeInt(FETCH_MAX_WAIT_MS);
            DataInputStream in = conn.request(KafkaFrame.API_KEY_REPLICA_FETCH, bytes.toByteArray());
            if (!running || fetchingPausedForTest) {
                return; // a fetch in flight when paused is discarded, as if it never arrived
            }
            in.readInt(); // leader epoch
            long leaderHw = in.readLong();
            long divergingEnd = in.readLong();
            List<LeaderEpochCache.Entry> epochs = new ArrayList<>();
            for (int n = in.readInt(); n > 0; n--) {
                epochs.add(new LeaderEpochCache.Entry(in.readInt(), in.readLong()));
            }
            if (divergingEnd >= 0) {
                LOGGER.warning("Broker " + brokerId + ": " + tp.topic() + "-" + tp.partition()
                        + " diverged from leader " + leaderId + "; truncating " + partition.getNextOffset() + " -> " + divergingEnd);
                partition.truncateTo(divergingEnd);
                broker.getIdempotenceManager().truncate(tp.topic(), tp.partition(), divergingEnd);
                return;
            }
            for (int n = in.readInt(); n > 0; n--) {
                long offset = in.readLong();
                byte[] payload = new byte[in.readInt()];
                in.readFully(payload);
                partition.appendReplicated(offset, payload);
            }
            List<ProducerState> producers = new ArrayList<>();
            for (int n = in.readInt(); n > 0; n--) {
                producers.add(ProducerState.readFrom(in));
            }
            if (!producers.isEmpty()) {
                IdempotenceManager idempotence = broker.getIdempotenceManager();
                idempotence.registerPartitionDir(tp.topic(), tp.partition(), new java.io.File(partition.getBaseDir()));
                idempotence.applyReplicated(tp.topic(), tp.partition(), producers, partition.getNextOffset());
            }
            for (LeaderEpochCache.Entry e : epochs) {
                if (e.startOffset() < partition.getNextOffset()) { // epochs we now hold records of
                    partition.leaderEpochs().assign(e.epoch(), e.startOffset());
                }
            }
            partition.setHighWatermark(Math.min(leaderHw, partition.getNextOffset()));
        }
    }

    private BrokerConnection connectTo(int targetBrokerId) throws IOException {
        MetadataImage.BrokerRegistration target = broker.getMetadataLog().image().brokers().get(targetBrokerId);
        if (target == null) {
            throw new IOException("Leader broker " + targetBrokerId + " is not registered");
        }
        return BrokerConnection.connect(target.host(), target.port());
    }

    /** Test hook: stop pulling from leaders (simulates a slow or partitioned follower). */
    void setFetchingPausedForTest(boolean paused) {
        fetchingPausedForTest = paused;
    }

    @Override
    public void close() {
        new ArrayList<>(fetchers.keySet()).forEach(this::stopFetcher);
        isrShrinker.shutdownNow();
        isrProposals.shutdownNow();
    }
}
