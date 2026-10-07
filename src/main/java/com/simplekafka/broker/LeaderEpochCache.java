package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Per-replica history of leader epochs: (epoch, offset of the first record written in that epoch).
 * This is how a follower finds where its log diverges from a new leader's (Kafka KIP-101/279):
 * it asks the leader where its own latest epoch ended and truncates anything past that point.
 * Truncating to the high watermark instead (Kafka's original approach) can lose committed data
 * or leave replicas with different records at the same offset.
 *
 * Leaders add an entry when they take over; followers copy the leader's entries as they
 * replicate. Persisted as "epoch startOffset" lines.
 */
public class LeaderEpochCache {
    private static final Logger LOGGER = Logger.getLogger(LeaderEpochCache.class.getName());
    static final String FILE = ".leader_epochs";

    public record Entry(int epoch, long startOffset) {}

    private final File file;
    private final List<Entry> entries = new ArrayList<>();

    LeaderEpochCache(File file) {
        this.file = file;
        if (file.exists()) {
            try {
                for (String line : Files.readAllLines(file.toPath())) {
                    String[] parts = line.trim().split(" ");
                    if (parts.length == 2) {
                        entries.add(new Entry(Integer.parseInt(parts[0]), Long.parseLong(parts[1])));
                    }
                }
            } catch (IOException | NumberFormatException e) {
                LOGGER.log(Level.WARNING, "Ignoring unreadable leader epoch cache " + file, e);
                entries.clear();
            }
        }
    }

    /** Records that {@code epoch} starts at {@code startOffset}. Ignores epochs not newer than the latest. */
    public synchronized void assign(int epoch, long startOffset) {
        if (!entries.isEmpty()) {
            Entry latest = entries.get(entries.size() - 1);
            if (epoch <= latest.epoch()) {
                return;
            }
            if (startOffset < latest.startOffset()) {
                startOffset = latest.startOffset(); // an epoch with no records of its own
            }
        }
        entries.add(new Entry(epoch, startOffset));
        persist();
    }

    public synchronized int latestEpoch() {
        return entries.isEmpty() ? -1 : entries.get(entries.size() - 1).epoch();
    }

    /**
     * End (exclusive) of {@code epoch} in this log: the start of the next newer epoch, or the
     * log end if {@code epoch} is the latest. -1 if this log has no record of an epoch that old
     * (the follower then truncates to its high watermark, the conservative fallback).
     */
    public synchronized long endOffsetFor(int epoch, long logEndOffset) {
        if (entries.isEmpty() || epoch < entries.get(0).epoch()) {
            return -1;
        }
        for (Entry e : entries) {
            if (e.epoch() > epoch) {
                return e.startOffset();
            }
        }
        return logEndOffset;
    }

    /** Entries starting at or after {@code offset}, for replicating to followers. */
    public synchronized List<Entry> entriesFrom(long offset) {
        List<Entry> result = new ArrayList<>();
        for (Entry e : entries) {
            if (e.startOffset() >= offset) {
                result.add(e);
            }
        }
        return result;
    }

    /** Drops epochs that started at or after {@code offset} (after a log truncation). */
    public synchronized void truncateFromEnd(long offset) {
        if (entries.removeIf(e -> e.startOffset() >= offset)) {
            persist();
        }
    }

    public synchronized List<Entry> entries() {
        return List.copyOf(entries);
    }

    private void persist() {
        try {
            file.getParentFile().mkdirs();
            StringBuilder out = new StringBuilder();
            entries.forEach(e -> out.append(e.epoch()).append(' ').append(e.startOffset()).append('\n'));
            File tmp = new File(file.getParentFile(), file.getName() + ".tmp");
            Files.writeString(tmp.toPath(), out.toString());
            Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to persist leader epoch cache " + file, e);
        }
    }
}
