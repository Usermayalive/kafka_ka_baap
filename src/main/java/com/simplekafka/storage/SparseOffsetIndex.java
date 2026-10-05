package com.simplekafka.storage;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Sparse offset index mapping logical record offsets to physical byte positions in a segment log.
 * Entries are recorded approximately every 4KB (INDEX_INTERVAL_BYTES).
 *
 * DESIGN RATIONALE:
 * - Startup: Uses Java 25 Foreign Function & Memory (FFM) MemorySegment with Arena.ofConfined()
 *   to parse persisted .sindex files directly from disk with deterministic, immediate unmapping
 *   (bypassing the MappedByteBuffer GC cleaner file-handle leak bug).
 * - Runtime: Cached in an in-memory List for fast binary search, completely avoiding page faults
 *   on index scans.
 */
public class SparseOffsetIndex {
    public static final int INDEX_INTERVAL_BYTES = 4096;
    public static final int ENTRY_SIZE = 16; // relativeOffset: long (8 bytes) + physicalPosition: long (8 bytes)

    public static final ValueLayout.OfLong BIG_ENDIAN_LONG = ValueLayout.JAVA_LONG.withOrder(java.nio.ByteOrder.BIG_ENDIAN);

    public record IndexEntry(long relativeOffset, long physicalPosition) {}

    private final List<IndexEntry> entries;
    private final File indexFile;
    private long bytesSinceLastIndex = 0;
    private long lastIndexedOffset = -1;

    public SparseOffsetIndex(List<IndexEntry> initialEntries, File indexFile) {
        this.entries = Collections.synchronizedList(new ArrayList<>(initialEntries));
        this.indexFile = indexFile;
        if (!this.entries.isEmpty()) {
            this.lastIndexedOffset = this.entries.get(this.entries.size() - 1).relativeOffset();
            this.bytesSinceLastIndex = this.entries.get(this.entries.size() - 1).physicalPosition();
        }
    }

    /**
     * Loads the sparse index from disk using Java 25 FFM MemorySegment.
     *
     * Arena.ofConfined() is thread-safe here because parsing executes synchronously
     * within the calling thread during broker startup. If this method is ever refactored
     * into an asynchronous pipeline or multi-threaded worker pool, switch to Arena.ofShared().
     */
    public static SparseOffsetIndex loadFromFile(File indexFile) throws IOException {
        if (!indexFile.exists() || indexFile.length() == 0) {
            return new SparseOffsetIndex(new ArrayList<>(), indexFile);
        }

        try (Arena arena = Arena.ofConfined()) {
            try (FileChannel channel = FileChannel.open(indexFile.toPath(), StandardOpenOption.READ)) {
                long size = channel.size();
                MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);

                int numEntries = (int) (size / ENTRY_SIZE);
                List<IndexEntry> loaded = new ArrayList<>(numEntries);
                for (long offset = 0; offset < size; offset += ENTRY_SIZE) {
                    long relOffset = segment.get(BIG_ENDIAN_LONG, offset);
                    long position = segment.get(BIG_ENDIAN_LONG, offset + 8);
                    loaded.add(new IndexEntry(relOffset, position));
                }
                return new SparseOffsetIndex(loaded, indexFile);
            }
        } // Arena closes deterministically here, releasing file handles immediately without GC delay
    }

    public synchronized void maybeAddEntry(long offset, long physicalPosition) {
        if (entries.isEmpty() || (physicalPosition - bytesSinceLastIndex) >= INDEX_INTERVAL_BYTES) {
            entries.add(new IndexEntry(offset, physicalPosition));
            bytesSinceLastIndex = physicalPosition;
            lastIndexedOffset = offset;
        }
    }

    public synchronized long lookupPosition(long targetOffset) {
        if (entries.isEmpty()) {
            return 0;
        }
        int low = 0;
        int high = entries.size() - 1;
        long bestPos = 0;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            IndexEntry entry = entries.get(mid);
            if (entry.relativeOffset() <= targetOffset) {
                bestPos = entry.physicalPosition();
                low = mid + 1; // Try to find a closer match forward
            } else {
                high = mid - 1;
            }
        }
        return bestPos;
    }

    public synchronized long lookupOffsetForPosition(long physicalPosition) {
        for (IndexEntry e : entries) {
            if (e.physicalPosition() == physicalPosition) {
                return e.relativeOffset();
            }
        }
        return 0;
    }

    public synchronized void flush() throws IOException {
        try (FileChannel ch = FileChannel.open(
                indexFile.toPath(),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.allocate(entries.size() * ENTRY_SIZE);
            for (IndexEntry e : entries) {
                buf.putLong(e.relativeOffset());
                buf.putLong(e.physicalPosition());
            }
            buf.flip();
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(false);
        }
    }

    public static SparseOffsetIndex loadOrRebuild(File indexFile, File logFile, long baseOffset) throws IOException {
        if (indexFile.exists() && indexFile.length() > 0) {
            return loadFromFile(indexFile);
        }
        return rebuildFromLog(logFile, baseOffset, indexFile);
    }

    public static SparseOffsetIndex rebuildFromLog(File logFile, long baseOffset, File indexFile) throws IOException {
        SparseOffsetIndex index = new SparseOffsetIndex(new ArrayList<>(), indexFile);
        if (!logFile.exists() || logFile.length() == 0) {
            return index;
        }

        try (FileChannel ch = FileChannel.open(logFile.toPath(), StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(4);
            long pos = 0;
            long offset = baseOffset;
            long size = ch.size();

            while (pos < size) {
                header.clear();
                int read = ch.read(header, pos);
                if (read < 4) break;
                header.flip();
                int len = header.getInt();
                index.maybeAddEntry(offset, pos);
                pos += 4 + len;
                offset++;
            }
        }
        index.flush();
        return index;
    }

    public synchronized long getLastIndexedOffset() {
        return lastIndexedOffset;
    }

    public synchronized int getEntryCount() {
        return entries.size();
    }
}
