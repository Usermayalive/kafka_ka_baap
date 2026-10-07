package com.simplekafka.network;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages partition-level sequence numbers, 5-element sliding window deduplication,
 * epoch fencing, and batched snapshot persistence to .producer_state on disk.
 *
 * LIFECYCLE & PERSISTENCE POLICY:
 * - registerPartitionDir() is invoked when a partition is initialized during broker boot or lazily on first access.
 * - Snapshots are batched to avoid un-batching group commits: flushed every SNAPSHOT_INTERVAL (default 100) accepted
 *   sequences per partition, or explicitly via flushSnapshot() on shutdown or partition close.
 * - Snapshots use .tmp + StandardCopyOption.ATOMIC_MOVE for crash-safety.
 */
public class IdempotenceManager {
    private static final Logger LOGGER = Logger.getLogger(IdempotenceManager.class.getName());
    public static final short SNAPSHOT_MAGIC = 0x5053; // 'PS' (Producer Snapshot)
    public static final byte SNAPSHOT_VERSION = 1;
    public static final String SNAPSHOT_FILENAME = ".producer_state";
    public static final int DEFAULT_SNAPSHOT_INTERVAL = 100;

    @FunctionalInterface
    public interface AppendAction {
        long execute() throws Exception;
    }

    public record IdempotentResult(boolean isDuplicate, long offset) {}

    private final int snapshotInterval;

    // Key: topic + "-" + partition -> (producerId -> ProducerState)
    private final Map<String, Map<Long, ProducerState>> partitionStates = new ConcurrentHashMap<>();
    // Key: topic + "-" + partition -> partition directory on disk
    private final Map<String, File> partitionDirectories = new ConcurrentHashMap<>();
    // Key: topic + "-" + partition -> unpersisted update counter for batched snapshotting
    private final Map<String, AtomicInteger> unpersistedUpdates = new ConcurrentHashMap<>();

    public IdempotenceManager() {
        this(DEFAULT_SNAPSHOT_INTERVAL);
    }

    public IdempotenceManager(int snapshotInterval) {
        this.snapshotInterval = Math.max(1, snapshotInterval);
    }

    /**
     * Idempotent: the snapshot is loaded only on first registration. The produce path calls this
     * on every request, and reloading would overwrite newer in-memory state with the last
     * snapshot, rejecting the producer's next sequence as a gap.
     */
    public void registerPartitionDir(String topic, int partition, File partitionDir) {
        String key = topic + "-" + partition;
        if (partitionDirectories.putIfAbsent(key, partitionDir) == null) {
            loadSnapshot(key, partitionDir);
        }
    }

    public IdempotentResult verifyAndApply(
            String topic,
            int partition,
            long producerId,
            short epoch,
            int sequenceNumber,
            AppendAction appendAction
    ) throws Exception {
        return verifyAndApply(topic, partition, producerId, epoch, sequenceNumber, 1, appendAction);
    }

    /**
     * Batch form: the batch covers sequences [firstSequence, firstSequence + count - 1] and
     * {@code appendAction} appends all its records contiguously, returning the offset of the
     * FIRST. The dedup window is keyed by a batch's last sequence (mapping to its last record's
     * offset), so a retried batch returns its original base offset. A single record is a batch of 1.
     */
    public IdempotentResult verifyAndApply(
            String topic,
            int partition,
            long producerId,
            short epoch,
            int firstSequence,
            int count,
            AppendAction appendAction
    ) throws Exception {
        String partitionKey = topic + "-" + partition;
        Map<Long, ProducerState> pMap = partitionStates.computeIfAbsent(partitionKey, k -> new ConcurrentHashMap<>());
        int lastSequence = firstSequence + count - 1;

        synchronized (pMap) {
            ProducerState existing = pMap.get(producerId);

            if (existing == null || epoch > existing.epoch()) {
                // A new producer, or an epoch bump (failover / restart), must start at sequence 0
                if (firstSequence != 0) {
                    throw new OutOfOrderSequenceException("Expected sequence number 0 for "
                            + (existing == null ? "new producer " + producerId : "epoch bump to " + epoch)
                            + ", but received: " + firstSequence);
                }
                long baseOffset = appendAction.execute();
                pMap.put(producerId, new ProducerState(producerId, epoch, lastSequence, baseOffset + count - 1,
                        System.currentTimeMillis()));
                maybePersistSnapshot(partitionKey);
                return new IdempotentResult(false, baseOffset);
            }

            // Zombie Producer Fencing
            if (epoch < existing.epoch()) {
                throw new ProducerFencedException("Producer " + producerId + " epoch " + epoch +
                        " is fenced by active broker epoch " + existing.epoch());
            }

            // Same epoch: a retry of a recent batch returns its original offsets
            if (existing.hasSequenceInWindow(lastSequence)) {
                return new IdempotentResult(true, existing.getOffsetForSequence(lastSequence) - (count - 1));
            }

            if (firstSequence == existing.lastSequenceNumber() + 1) {
                long baseOffset = appendAction.execute();
                pMap.put(producerId, existing.advance(lastSequence, baseOffset + count - 1, System.currentTimeMillis()));
                maybePersistSnapshot(partitionKey);
                return new IdempotentResult(false, baseOffset);
            }

            if (firstSequence > existing.lastSequenceNumber() + 1) {
                throw new OutOfOrderSequenceException("Sequence gap for producer " + producerId +
                        ": expected " + (existing.lastSequenceNumber() + 1) + ", but received: " + firstSequence);
            }

            // Already processed, but too old for the dedup window
            throw new DuplicateSequenceException("Duplicate sequence " + firstSequence + " for producer " + producerId +
                    " is too old and fell off the " + ProducerState.WINDOW_SIZE + "-batch window (last seen: " +
                    existing.lastSequenceNumber() + ")");
        }
    }

    private void maybePersistSnapshot(String partitionKey) {
        AtomicInteger count = unpersistedUpdates.computeIfAbsent(partitionKey, k -> new AtomicInteger(0));
        if (count.incrementAndGet() >= snapshotInterval) {
            count.set(0);
            persistSnapshot(partitionKey);
        }
    }

    public void flushSnapshot(String topic, int partition) {
        String key = topic + "-" + partition;
        AtomicInteger count = unpersistedUpdates.get(key);
        if (count != null) {
            count.set(0);
        }
        persistSnapshot(key);
    }

    public void flushSnapshots() {
        for (String partitionKey : partitionDirectories.keySet()) {
            AtomicInteger count = unpersistedUpdates.get(partitionKey);
            if (count != null) {
                count.set(0);
            }
            persistSnapshot(partitionKey);
        }
    }

    public void persistSnapshot(String partitionKey) {
        File dir = partitionDirectories.get(partitionKey);
        if (dir == null || !dir.exists()) {
            return;
        }

        Map<Long, ProducerState> pMap = partitionStates.get(partitionKey);
        if (pMap == null) return;

        File tmpFile = new File(dir, SNAPSHOT_FILENAME + ".tmp");
        File targetFile = new File(dir, SNAPSHOT_FILENAME);

        try {
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpFile)))) {
                dos.writeShort(SNAPSHOT_MAGIC);
                dos.writeByte(SNAPSHOT_VERSION);
                dos.writeInt(pMap.size());
                for (ProducerState state : pMap.values()) {
                    state.writeTo(dos);
                }
                dos.flush();
            }

            Files.move(tmpFile.toPath(), targetFile.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to persist .producer_state snapshot for " + partitionKey, e);
        }
    }

    public void loadSnapshot(String partitionKey, File dir) {
        if (dir == null || !dir.exists()) return;
        File snapshotFile = new File(dir, SNAPSHOT_FILENAME);
        if (!snapshotFile.exists()) return;

        Map<Long, ProducerState> pMap = partitionStates.computeIfAbsent(partitionKey, k -> new ConcurrentHashMap<>());

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(snapshotFile)))) {
            short magic = dis.readShort();
            if (magic != SNAPSHOT_MAGIC) {
                LOGGER.warning("Corrupt producer state snapshot magic: 0x" + Integer.toHexString(magic));
                return;
            }
            byte version = dis.readByte();
            if (version > SNAPSHOT_VERSION) {
                LOGGER.severe("Cannot load .producer_state: snapshot version " + version +
                        " is newer than supported version " + SNAPSHOT_VERSION);
                return;
            } else if (version < 1) {
                LOGGER.warning("Unsupported legacy producer state snapshot version: " + version);
                return;
            }
            int count = dis.readInt();
            for (int i = 0; i < count; i++) {
                ProducerState state = ProducerState.readFrom(dis);
                pMap.put(state.producerId(), state);
            }
            LOGGER.info("Recovered " + count + " producer state(s) from " + snapshotFile.getAbsolutePath());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to load .producer_state snapshot from " + snapshotFile.getAbsolutePath(), e);
        }
    }

    /** Producer states plus the log end they're consistent with (taken together). */
    public record ReplicationSnapshot(java.util.List<ProducerState> states, long logEndOffset) {}

    /**
     * Leader side of producer-state replication. Taken under the same lock idempotent appends
     * hold, so every idempotent record below {@code logEndOffset} has its producer's update in
     * {@code states}. The replica fetch then returns only records below that log end.
     */
    public ReplicationSnapshot snapshotForReplication(String topic, int partition, java.util.function.LongSupplier logEnd) {
        Map<Long, ProducerState> pMap = partitionStates.computeIfAbsent(topic + "-" + partition, k -> new ConcurrentHashMap<>());
        synchronized (pMap) {
            return new ReplicationSnapshot(new java.util.ArrayList<>(pMap.values()), logEnd.getAsLong());
        }
    }

    /**
     * Follower side: adopt the leader's producer states that this replica's log now covers
     * (last offset below {@code followerLogEnd}). Entries not yet covered are re-sent with the
     * next fetch, so producer state never claims a record this replica doesn't have.
     */
    public void applyReplicated(String topic, int partition, java.util.List<ProducerState> states, long followerLogEnd) {
        String key = topic + "-" + partition;
        Map<Long, ProducerState> pMap = partitionStates.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        boolean changed = false;
        synchronized (pMap) {
            for (ProducerState state : states) {
                if (state.lastOffset() < followerLogEnd) {
                    ProducerState current = pMap.get(state.producerId());
                    if (current == null || state.epoch() > current.epoch()
                            || (state.epoch() == current.epoch() && state.lastSequenceNumber() > current.lastSequenceNumber())) {
                        pMap.put(state.producerId(), state);
                        changed = true;
                    }
                }
            }
        }
        if (changed) {
            maybePersistSnapshot(key);
        }
    }

    /**
     * After a log truncation: forget producer state for records that no longer exist, so a
     * retry is never "deduplicated" against a truncated record. The leader's states (consistent
     * with its log) re-populate them on subsequent fetches.
     */
    public void truncate(String topic, int partition, long truncatedFrom) {
        String key = topic + "-" + partition;
        Map<Long, ProducerState> pMap = partitionStates.get(key);
        if (pMap == null) {
            return;
        }
        boolean changed;
        synchronized (pMap) {
            changed = pMap.values().removeIf(state -> state.lastOffset() >= truncatedFrom);
        }
        if (changed) {
            maybePersistSnapshot(key);
        }
    }

    public ProducerState getProducerState(String topic, int partition, long producerId) {
        Map<Long, ProducerState> pMap = partitionStates.get(topic + "-" + partition);
        return pMap != null ? pMap.get(producerId) : null;
    }
}
