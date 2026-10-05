package com.simplekafka.network;

import java.io.*;
import java.util.*;

/**
 * Snapshot of a producer's current sequence state on a specific partition,
 * including a sliding window of recent sequences for in-flight retry deduplication.
 */
public record ProducerState(
        long producerId,
        short epoch,
        int lastSequenceNumber,
        long lastOffset,
        long lastTimestampMs,
        Map<Integer, Long> recentSequences
) {
    public static final int WINDOW_SIZE = 5;

    public ProducerState(long producerId, short epoch, int lastSequenceNumber, long lastOffset, long lastTimestampMs) {
        this(producerId, epoch, lastSequenceNumber, lastOffset, lastTimestampMs,
                Collections.singletonMap(lastSequenceNumber, lastOffset));
    }

    public ProducerState {
        // Guarantee unmodifiable linked map preserving insertion order
        recentSequences = Collections.unmodifiableMap(new LinkedHashMap<>(recentSequences));
    }

    /**
     * Creates a new ProducerState advanced with the next sequence and its assigned log offset.
     * Enforces the sliding window bound (retaining at most WINDOW_SIZE entries).
     */
    public ProducerState advance(int nextSequenceNumber, long nextOffset, long nowMs) {
        Map<Integer, Long> nextWindow = new LinkedHashMap<>(this.recentSequences);
        nextWindow.put(nextSequenceNumber, nextOffset);
        while (nextWindow.size() > WINDOW_SIZE) {
            Iterator<Integer> it = nextWindow.keySet().iterator();
            if (it.hasNext()) {
                it.next();
                it.remove();
            }
        }
        return new ProducerState(producerId, epoch, nextSequenceNumber, nextOffset, nowMs, nextWindow);
    }

    public boolean hasSequenceInWindow(int seq) {
        return recentSequences.containsKey(seq);
    }

    public Long getOffsetForSequence(int seq) {
        return recentSequences.get(seq);
    }

    public void writeTo(DataOutputStream dos) throws IOException {
        dos.writeLong(producerId);
        dos.writeShort(epoch);
        dos.writeInt(lastSequenceNumber);
        dos.writeLong(lastOffset);
        dos.writeLong(lastTimestampMs);
        dos.writeInt(recentSequences.size());
        for (Map.Entry<Integer, Long> entry : recentSequences.entrySet()) {
            dos.writeInt(entry.getKey());
            dos.writeLong(entry.getValue());
        }
    }

    public static ProducerState readFrom(DataInputStream dis) throws IOException {
        long pid = dis.readLong();
        short epoch = dis.readShort();
        int lastSeq = dis.readInt();
        long lastOffset = dis.readLong();
        long timestamp = dis.readLong();
        int windowSize = dis.readInt();
        Map<Integer, Long> window = new LinkedHashMap<>();
        for (int i = 0; i < windowSize; i++) {
            int seq = dis.readInt();
            long off = dis.readLong();
            window.put(seq, off);
        }
        return new ProducerState(pid, epoch, lastSeq, lastOffset, timestamp, window);
    }
}
