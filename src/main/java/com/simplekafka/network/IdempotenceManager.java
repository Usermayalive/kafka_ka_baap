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

    public void registerPartitionDir(String topic, int partition, File partitionDir) {
        String key = topic + "-" + partition;
        partitionDirectories.put(key, partitionDir);
        loadSnapshot(key, partitionDir);
    }

    public IdempotentResult verifyAndApply(
            String topic,
            int partition,
            long producerId,
            short epoch,
            int sequenceNumber,
            AppendAction appendAction
    ) throws Exception {
        String partitionKey = topic + "-" + partition;
        Map<Long, ProducerState> pMap = partitionStates.computeIfAbsent(partitionKey, k -> new ConcurrentHashMap<>());

        synchronized (pMap) {
            ProducerState existing = pMap.get(producerId);

            if (existing == null) {
                // First sequence number from a new producer MUST be 0
                if (sequenceNumber != 0) {
                    throw new OutOfOrderSequenceException("Expected sequence number 0 for new producer " +
                            producerId + ", but received: " + sequenceNumber);
                }
                long offset = appendAction.execute();
                ProducerState newState = new ProducerState(producerId, epoch, 0, offset, System.currentTimeMillis());
                pMap.put(producerId, newState);
                maybePersistSnapshot(partitionKey);
                return new IdempotentResult(false, offset);
            }

            // Zombie Producer Fencing
            if (epoch < existing.epoch()) {
                throw new ProducerFencedException("Producer " + producerId + " epoch " + epoch +
                        " is fenced by active broker epoch " + existing.epoch());
            }

            // Epoch Bump (e.g. producer failover / restart)
            if (epoch > existing.epoch()) {
                if (sequenceNumber != 0) {
                    throw new OutOfOrderSequenceException("Expected sequence number 0 after epoch bump to " +
                            epoch + ", but received: " + sequenceNumber);
                }
                long offset = appendAction.execute();
                ProducerState newState = new ProducerState(producerId, epoch, 0, offset, System.currentTimeMillis());
                pMap.put(producerId, newState);
                maybePersistSnapshot(partitionKey);
                return new IdempotentResult(false, offset);
            }

            // Same Epoch: Check 5-Element Sliding Window for Deduplication
            if (existing.hasSequenceInWindow(sequenceNumber)) {
                // DUPLICATE DETECTED within sliding window!
                // Skip re-appending to storage log; return cached offset
                return new IdempotentResult(true, existing.getOffsetForSequence(sequenceNumber));
            }

            if (sequenceNumber == existing.lastSequenceNumber() + 1) {
                // Expected sequential record
                long offset = appendAction.execute();
                ProducerState updated = existing.advance(sequenceNumber, offset, System.currentTimeMillis());
                pMap.put(producerId, updated);
                maybePersistSnapshot(partitionKey);
                return new IdempotentResult(false, offset);
            }

            if (sequenceNumber > existing.lastSequenceNumber() + 1) {
                // SEQUENCE GAP DETECTED
                throw new OutOfOrderSequenceException("Sequence gap for producer " + producerId +
                        ": expected " + (existing.lastSequenceNumber() + 1) + ", but received: " + sequenceNumber);
            }

            // sequenceNumber <= existing.lastSequenceNumber() and NOT in window -> Out-of-window duplicate!
            throw new DuplicateSequenceException("Duplicate sequence " + sequenceNumber + " for producer " + producerId +
                    " is too old and fell off the 5-element sliding window (last seen: " +
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

    public ProducerState getProducerState(String topic, int partition, long producerId) {
        Map<Long, ProducerState> pMap = partitionStates.get(topic + "-" + partition);
        return pMap != null ? pMap.get(producerId) : null;
    }
}
