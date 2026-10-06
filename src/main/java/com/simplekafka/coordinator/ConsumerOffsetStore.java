package com.simplekafka.coordinator;

import com.simplekafka.broker.Partition;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Committed consumer offsets, persisted in the compacted internal topic {@code __consumer_offsets}.
 *
 * A group's offsets live in partition {@code floorMod(group.hashCode(), partitions)}; the broker
 * leading that partition is the group's coordinator. Each record's compaction key is
 * (group, topic, partition), so compaction keeps only the latest commit per key. The in-memory
 * view of a partition is rebuilt by replaying it the first time this broker serves it.
 *
 * Record: [magic:1][key UTF][offset:8][commitTimestamp:8][metadata UTF]
 */
public class ConsumerOffsetStore {
    private static final Logger LOGGER = Logger.getLogger(ConsumerOffsetStore.class.getName());

    public static final String TOPIC = "__consumer_offsets";
    public static final int PARTITIONS = 4;
    private static final byte MAGIC = 1;
    /** Compact a partition after this many commits since the last compaction. */
    static final long COMPACT_EVERY_COMMITS = 10_000;

    public record OffsetAndMetadata(long offset, long commitTimestamp, String metadata) {}

    private final IntFunction<Partition> localPartition;
    private final Map<String, OffsetAndMetadata> committed = new ConcurrentHashMap<>();
    private final Map<Integer, Boolean> loaded = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> commitsSinceCompaction = new ConcurrentHashMap<>();

    /**
     * @param localPartition returns this broker's replica of {@code __consumer_offsets-N}, or
     *                       null if this broker does not host it
     */
    public ConsumerOffsetStore(IntFunction<Partition> localPartition) {
        this.localPartition = localPartition;
    }

    public static int partitionFor(String group) {
        return Math.floorMod(group.hashCode(), PARTITIONS);
    }

    static String key(String group, String topic, int partition) {
        return group + '\u0000' + topic + '\u0000' + partition;
    }

    public void commit(String group, String topic, int partition, long offset, String metadata) throws IOException {
        int p = partitionFor(group);
        Partition log = hosted(p);
        ensureLoaded(p, log);
        String key = key(group, topic, partition);
        OffsetAndMetadata value = new OffsetAndMetadata(offset, System.currentTimeMillis(), metadata == null ? "" : metadata);
        log.append(encode(key, value));
        committed.put(key, value);

        if (commitsSinceCompaction.computeIfAbsent(p, k -> new AtomicLong()).incrementAndGet() >= COMPACT_EVERY_COMMITS) {
            commitsSinceCompaction.get(p).set(0);
            try {
                log.compact(ConsumerOffsetStore::compactionKey);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Compaction of " + TOPIC + "-" + p + " failed", e);
            }
        }
    }

    /** @return the committed position, or null if the group never committed for this partition */
    public OffsetAndMetadata fetch(String group, String topic, int partition) throws IOException {
        int p = partitionFor(group);
        ensureLoaded(p, hosted(p));
        return committed.get(key(group, topic, partition));
    }

    private Partition hosted(int p) throws IOException {
        Partition log = localPartition.apply(p);
        if (log == null) {
            throw new IOException(TOPIC + "-" + p + " is not hosted on this broker");
        }
        return log;
    }

    private void ensureLoaded(int p, Partition log) throws IOException {
        if (loaded.containsKey(p)) {
            return;
        }
        synchronized (loaded) {
            if (loaded.containsKey(p)) {
                return;
            }
            long next = 0;
            int records = 0;
            while (next < log.getNextOffset()) {
                List<Partition.Record> batch = log.readRecords(next, 4 * 1024 * 1024);
                if (batch.isEmpty()) {
                    break;
                }
                for (Partition.Record r : batch) {
                    Decoded d = decode(r.payload());
                    committed.put(d.key(), d.value());
                    records++;
                }
                next = batch.get(batch.size() - 1).offset() + 1;
            }
            loaded.put(p, Boolean.TRUE);
            LOGGER.info("Loaded " + records + " offset commits from " + TOPIC + "-" + p);
        }
    }

    private record Decoded(String key, OffsetAndMetadata value) {}

    static byte[] encode(String key, OffsetAndMetadata value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte(MAGIC);
        out.writeUTF(key);
        out.writeLong(value.offset());
        out.writeLong(value.commitTimestamp());
        out.writeUTF(value.metadata());
        return bytes.toByteArray();
    }

    private static Decoded decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        byte magic = in.readByte();
        if (magic != MAGIC) {
            throw new IOException("Unknown " + TOPIC + " record version " + magic);
        }
        String key = in.readUTF();
        return new Decoded(key, new OffsetAndMetadata(in.readLong(), in.readLong(), in.readUTF()));
    }

    static String compactionKey(byte[] payload) throws IOException {
        return decode(payload).key();
    }
}
