package com.simplekafka.broker;

import com.simplekafka.storage.WriteAheadLog.DurabilityMode;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A single partition log: 1MB segments of [length:4][payload] records plus a dense
 * (offset, position) index that preserves offset gaps left by compaction.
 *
 * DURABILITY (see docs/durability.md):
 * - OS_PAGE_CACHE (default, Kafka's default): append() returns once the record is in the
 *   kernel page cache. Survives a broker process crash; an OS crash / power loss can lose
 *   the unflushed tail, which is truncated back to a clean record boundary on restart.
 * - PHYSICAL_FSYNC: append() returns only after the record is fsynced. Concurrent appenders
 *   share one log+index force() (group commit), so throughput scales with concurrency
 *   instead of being capped at one fsync pair per message.
 */
public class Partition {
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final long MAX_SEGMENT_SIZE = 1024 * 1024; // 1MB
    private static final int INDEX_ENTRY_BYTES = 16;

    private static final String COMPACT_LOCK = ".compact.lock";
    private static final String COMPACT_STAGING = ".compact_staging";
    private static final String COMPACT_READY = ".compact_ready";
    private static final String META_FILE = ".partition.meta";
    private static final short META_MAGIC = 0x504D; // 'PM'
    private static final byte META_VERSION = 1;

    private final int id;
    private final DurabilityMode durabilityMode;
    private int leader;
    private List<Integer> followers;
    private final String baseDir;
    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private RandomAccessFile activeIndexFile;
    private FileChannel activeIndexChannel;
    private final List<SegmentInfo> segments;

    // Group commit state (PHYSICAL_FSYNC only), guarded by flushMonitor
    private final Object flushMonitor = new Object();
    private long durableUpToOffset = -1;
    private boolean flushInProgress = false;

    public Partition(int id, String topicBaseDir) {
        this(id, topicBaseDir, DurabilityMode.OS_PAGE_CACHE);
    }

    public Partition(int id, String topicBaseDir, DurabilityMode durabilityMode) {
        this.id = id;
        this.durabilityMode = durabilityMode;
        this.baseDir = topicBaseDir + File.separator + "partition-" + id;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();
        this.followers = new ArrayList<>();
        initialize();
    }

    public static class SegmentInfo {
        long baseOffset;
        File logFile;
        File indexFile;
        long currentMessageCount;

        public SegmentInfo(long baseOffset, File logFile, File indexFile, long currentMessageCount) {
            this.baseOffset = baseOffset;
            this.logFile = logFile;
            this.indexFile = indexFile;
            this.currentMessageCount = currentMessageCount;
        }
    }

    private void initialize() {
        try {
            Path dirPath = Paths.get(baseDir);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }

            File dir = new File(baseDir);

            // Crash recovery for an interrupted compact(): roll forward if the compacted files
            // reached the commit point (.compact_ready), otherwise roll back (discard staging).
            File compactLock = new File(baseDir, COMPACT_LOCK);
            File compactStaging = new File(baseDir, COMPACT_STAGING);
            File compactReady = new File(baseDir, COMPACT_READY);
            if (compactReady.exists()) {
                LOGGER.warning("Partition " + id + ": completing interrupted compaction from " + COMPACT_READY);
                installCompactedFiles(compactReady);
            }
            if (compactStaging.exists()) {
                LOGGER.warning("Partition " + id + ": discarding uncommitted compaction staging files");
                deleteDirectory(compactStaging);
            }
            compactLock.delete();

            File[] files = dir.listFiles((d, name) -> name.endsWith(".log"));
            
            if (files != null && files.length > 0) {
                for (File file : files) {
                    String name = file.getName();
                    long baseOffset = Long.parseLong(name.substring(0, name.length() - 4));
                    File indexFile = new File(baseDir, String.format("%020d.index", baseOffset));
                    
                    // Recover message count from index size (dense indexing: 16 bytes per entry)
                    long messageCount = indexFile.exists() ? indexFile.length() / 16 : 0;
                    
                    segments.add(new SegmentInfo(baseOffset, file, indexFile, messageCount));
                }
                
                segments.sort(Comparator.comparingLong(s -> s.baseOffset));
                
                SegmentInfo lastSegment = segments.get(segments.size() - 1);
                // Index entries carry explicit offsets, so this stays correct across compaction gaps
                long lastOffset = truncateTornTail(lastSegment);
                long fromIndex = lastOffset >= 0 ? lastOffset + 1 : lastSegment.baseOffset;
                // .meta covers offsets compaction removed from the tail (e.g. tombstoned keys)
                nextOffset.set(Math.max(fromIndex, readMetaNextOffset()));
                openSegmentForAppend(lastSegment);
            } else {
                createNewSegment(0);
                nextOffset.set(readMetaNextOffset());
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);
            throw new RuntimeException("Failed to initialize partition", e);
        }
    }

    /**
     * Without a per-message fsync, an OS crash can persist the log and index tails
     * independently. Drops index entries whose record isn't fully in the log, then truncates
     * the log after the last indexed record so readMessages() never scans orphaned bytes.
     *
     * @return offset of the last intact record, or -1 if the segment is empty
     */
    private long truncateTornTail(SegmentInfo segment) throws IOException {
        try (RandomAccessFile log = new RandomAccessFile(segment.logFile, "rw");
             RandomAccessFile index = new RandomAccessFile(segment.indexFile, "rw")) {
            long entries = index.length() / INDEX_ENTRY_BYTES;
            long validLogEnd = 0;
            long lastOffset = -1;
            while (entries > 0) {
                index.seek((entries - 1) * INDEX_ENTRY_BYTES);
                long offset = index.readLong();
                long position = index.readLong();
                if (position >= 0 && position + 4 <= log.length()) {
                    log.seek(position);
                    long recordEnd = position + 4 + log.readInt();
                    if (recordEnd >= position + 4 && recordEnd <= log.length()) {
                        validLogEnd = recordEnd;
                        lastOffset = offset;
                        break;
                    }
                }
                entries--;
            }

            long validIndexEnd = entries * INDEX_ENTRY_BYTES;
            if (index.length() != validIndexEnd || log.length() != validLogEnd) {
                LOGGER.warning("Partition " + id + ": truncating torn tail of " + segment.logFile.getName() +
                        " (log " + log.length() + " -> " + validLogEnd +
                        " bytes, index " + index.length() + " -> " + validIndexEnd + " bytes)");
                index.setLength(validIndexEnd);
                log.setLength(validLogEnd);
            }
            segment.currentMessageCount = entries;
            return lastOffset;
        }
    }

    private void createNewSegment(long baseOffset) throws IOException {
        String logFileName = String.format("%020d.log", baseOffset);
        String indexFileName = String.format("%020d.index", baseOffset);
        
        File logFile = new File(baseDir, logFileName);
        File indexFile = new File(baseDir, indexFileName);
        
        logFile.createNewFile();
        indexFile.createNewFile();
        
        SegmentInfo newSegment = new SegmentInfo(baseOffset, logFile, indexFile, 0);
        segments.add(newSegment);
        openSegmentForAppend(newSegment);
    }

    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        // Rolling away from a segment: force it first so records still awaiting group commit
        // are durable before the channel the flush leader would force is closed.
        boolean forceOnClose = durabilityMode == DurabilityMode.PHYSICAL_FSYNC;
        if (activeLogChannel != null) {
            if (forceOnClose && activeLogChannel.isOpen()) {
                activeLogChannel.force(true);
            }
            activeLogChannel.close();
            activeLogFile.close();
        }
        if (activeIndexChannel != null) {
            if (forceOnClose && activeIndexChannel.isOpen()) {
                activeIndexChannel.force(true);
            }
            activeIndexChannel.close();
            activeIndexFile.close();
        }

        activeLogFile = new RandomAccessFile(segment.logFile, "rw");
        activeLogChannel = activeLogFile.getChannel();
        activeLogChannel.position(activeLogChannel.size());

        activeIndexFile = new RandomAccessFile(segment.indexFile, "rw");
        activeIndexChannel = activeIndexFile.getChannel();
        activeIndexChannel.position(activeIndexChannel.size());
    }

    public long append(byte[] message) throws IOException {
        long offset = appendToPageCache(message);
        if (durabilityMode == DurabilityMode.PHYSICAL_FSYNC) {
            awaitDurable(offset);
        }
        return offset;
    }

    private long appendToPageCache(byte[] message) throws IOException {
        lock.writeLock().lock();
        try {
            if (activeLogChannel.size() + message.length + 4 > MAX_SEGMENT_SIZE) {
                createNewSegment(nextOffset.get());
            }

            long currentPosition = activeLogChannel.position();
            long currentOffset = nextOffset.get();

            // Write to log file
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();

            while (buffer.hasRemaining()) {
                activeLogChannel.write(buffer);
            }

            // Update index file
            updateIndex(currentOffset, currentPosition);

            // Update segment info
            SegmentInfo activeSegment = segments.get(segments.size() - 1);
            activeSegment.currentMessageCount++;

            nextOffset.incrementAndGet();
            return currentOffset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateIndex(long offset, long position) throws IOException {
        ByteBuffer indexBuffer = ByteBuffer.allocate(16);
        indexBuffer.putLong(offset);
        indexBuffer.putLong(position);
        indexBuffer.flip();
        
        while (indexBuffer.hasRemaining()) {
            activeIndexChannel.write(indexBuffer);
        }
    }

    /**
     * Group commit: blocks until {@code offset} is fsynced. The first waiter becomes the flush
     * leader and forces log+index once, covering every record appended before the force began;
     * the rest wait for that flush instead of issuing their own.
     */
    private void awaitDurable(long offset) throws IOException {
        while (true) {
            synchronized (flushMonitor) {
                while (durableUpToOffset < offset && flushInProgress) {
                    try {
                        flushMonitor.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting for fsync", e);
                    }
                }
                if (durableUpToOffset >= offset) {
                    return;
                }
                flushInProgress = true;
            }

            long flushedUpTo = -1;
            IOException failure = null;
            // Read lock: excludes segment rolls and compaction (which close channels) while
            // forcing; appenders queue on the write lock and join the next flush.
            lock.readLock().lock();
            try {
                flushedUpTo = nextOffset.get() - 1;
                activeLogChannel.force(true);
                activeIndexChannel.force(true);
            } catch (IOException e) {
                failure = e;
            } finally {
                lock.readLock().unlock();
            }

            synchronized (flushMonitor) {
                flushInProgress = false;
                if (failure == null) {
                    durableUpToOffset = Math.max(durableUpToOffset, flushedUpTo);
                }
                flushMonitor.notifyAll();
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    /** A stored record with its offset; offsets are not contiguous after compaction. */
    public record Record(long offset, byte[] payload) {}

    public List<byte[]> readMessages(long offset, int maxBytes) throws IOException {
        List<Record> records = readRecords(offset, maxBytes);
        List<byte[]> payloads = new ArrayList<>(records.size());
        for (Record r : records) {
            payloads.add(r.payload());
        }
        return payloads;
    }

    /**
     * Reads records with offset >= {@code offset}, crossing segment boundaries, until
     * {@code maxBytes} is reached. Always returns at least one record if any exists, so a
     * record larger than maxBytes can't stall a consumer. Offsets come from the index, so
     * compaction gaps are reported faithfully.
     */
    public List<Record> readRecords(long offset, int maxBytes) throws IOException {
        lock.readLock().lock();
        try {
            if (offset >= nextOffset.get() || segments.isEmpty()) {
                return Collections.emptyList();
            }

            List<Record> records = new ArrayList<>();
            int bytesRead = 0;
            for (int s = findSegmentIndex(Math.max(offset, 0)); s < segments.size(); s++) {
                SegmentInfo segment = segments.get(s);
                try (RandomAccessFile index = new RandomAccessFile(segment.indexFile, "r");
                     RandomAccessFile log = new RandomAccessFile(segment.logFile, "r")) {
                    long entries = index.length() / INDEX_ENTRY_BYTES;
                    for (long e = firstEntryAtOrAfter(index, entries, offset); e < entries; e++) {
                        index.seek(e * INDEX_ENTRY_BYTES);
                        long recordOffset = index.readLong();
                        long position = index.readLong();
                        log.seek(position);
                        int size = log.readInt();
                        if (!records.isEmpty() && bytesRead + 4 + size > maxBytes) {
                            return records;
                        }
                        byte[] payload = new byte[size];
                        log.readFully(payload);
                        records.add(new Record(recordOffset, payload));
                        bytesRead += 4 + size;
                    }
                }
            }
            return records;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Offset the next append will receive (the log end offset). */
    public long getNextOffset() {
        return nextOffset.get();
    }

    /** Index of the last segment whose base offset is <= offset, or 0 if offset precedes all segments. */
    private int findSegmentIndex(long offset) {
        int left = 0;
        int right = segments.size() - 1;
        int result = 0;
        while (left <= right) {
            int mid = (left + right) >>> 1;
            if (segments.get(mid).baseOffset <= offset) {
                result = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return result;
    }

    /** Binary search over a dense index sorted by offset: first entry with offset >= target. */
    private static long firstEntryAtOrAfter(RandomAccessFile index, long entries, long target) throws IOException {
        long low = 0;
        long high = entries;
        while (low < high) {
            long mid = (low + high) >>> 1;
            index.seek(mid * INDEX_ENTRY_BYTES);
            if (index.readLong() < target) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private boolean compacted = false;
    private CompactionKeyExtractor compactionKeyExtractor;

    public boolean isCompacted() {
        return compacted;
    }

    public void setCompacted(boolean compacted) {
        this.compacted = compacted;
    }

    public void setCompactionKeyExtractor(CompactionKeyExtractor compactionKeyExtractor) {
        this.compactionKeyExtractor = compactionKeyExtractor;
    }

    public CompactionKeyExtractor getCompactionKeyExtractor() {
        return compactionKeyExtractor;
    }

    public record CompactedRecord(long offset, byte[] payload) {}

    /**
     * Default compaction using configured extractor or fallback message byte hash.
     */
    public void compact() throws IOException {
        if (compactionKeyExtractor != null) {
            compact(compactionKeyExtractor);
        } else {
            compact(msg -> java.util.UUID.nameUUIDFromBytes(msg).toString());
        }
    }

    /**
     * Compacts the partition log using a pluggable key extraction strategy.
     * Preserves original record offsets (supporting offset gaps), purges older duplicates,
     * and deletes tombstones (records with empty payload).
     */
    public void compact(CompactionKeyExtractor extractor) throws IOException {
        lock.writeLock().lock();
        try {
            List<CompactedRecord> allRecords = readAllRecordsWithOffsets();
            if (allRecords.isEmpty()) {
                return;
            }

            java.util.Map<String, CompactedRecord> latestByKey = new java.util.LinkedHashMap<>();
            for (CompactedRecord record : allRecords) {
                String key = null;
                try {
                    key = extractor.extractKey(record.payload());
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Error extracting compaction key for record at offset " + record.offset(), e);
                }
                if (key != null) {
                    // Tombstone: empty payload permanently deletes the key
                    if (record.payload() == null || record.payload().length == 0) {
                        latestByKey.remove(key);
                    } else {
                        latestByKey.put(key, record);
                    }
                }
            }

            // Crash safety: compacted files are built and fsynced in .compact_staging, then the
            // directory is atomically renamed to .compact_ready -- the commit point. A crash before
            // the rename rolls back (initialize() discards staging; the original files are untouched).
            // A crash after it rolls forward (initialize() re-runs installCompactedFiles(), which is
            // idempotent). Individual file swaps are not atomic as a group, which is why recovery
            // must never discard .compact_ready.
            File compactLock = new File(baseDir, COMPACT_LOCK);
            File compactStaging = new File(baseDir, COMPACT_STAGING);
            File compactReady = new File(baseDir, COMPACT_READY);
            deleteDirectory(compactStaging);
            compactStaging.mkdirs();
            compactLock.createNewFile();

            String stagedLogName = String.format("%020d.log", 0L);
            String stagedIndexName = String.format("%020d.index", 0L);
            File stagedLog = new File(compactStaging, stagedLogName);
            File stagedIndex = new File(compactStaging, stagedIndexName);

            try {
                try (RandomAccessFile sLogRaf = new RandomAccessFile(stagedLog, "rw");
                     FileChannel sLogCh = sLogRaf.getChannel();
                     RandomAccessFile sIdxRaf = new RandomAccessFile(stagedIndex, "rw");
                     FileChannel sIdxCh = sIdxRaf.getChannel()) {

                    long curPos = 0;
                    ByteBuffer idxBuf = ByteBuffer.allocate(16);
                    // Offset order, not key first-seen order: findPositionForOffset binary-searches the index
                    List<CompactedRecord> survivors = new ArrayList<>(latestByKey.values());
                    survivors.sort(Comparator.comparingLong(CompactedRecord::offset));
                    for (CompactedRecord record : survivors) {
                        byte[] msg = record.payload();
                        ByteBuffer logBuf = ByteBuffer.allocate(4 + msg.length);
                        logBuf.putInt(msg.length);
                        logBuf.put(msg);
                        logBuf.flip();
                        while (logBuf.hasRemaining()) {
                            sLogCh.write(logBuf);
                        }

                        idxBuf.clear();
                        idxBuf.putLong(record.offset());
                        idxBuf.putLong(curPos);
                        idxBuf.flip();
                        while (idxBuf.hasRemaining()) {
                            sIdxCh.write(idxBuf);
                        }
                        curPos += (4 + msg.length);
                    }
                    sLogCh.force(true);
                    sIdxCh.force(true);
                }

                // Compaction may drop the highest offsets (tombstoned keys); persist nextOffset
                // first so a restart never reissues them.
                writeMetaNextOffset(nextOffset.get());

                Files.move(compactStaging.toPath(), compactReady.toPath(), StandardCopyOption.ATOMIC_MOVE);
                fsyncDirectory(new File(baseDir));

                if (activeLogChannel != null) {
                    activeLogChannel.close();
                    activeLogFile.close();
                }
                if (activeIndexChannel != null) {
                    activeIndexChannel.close();
                    activeIndexFile.close();
                }

                installCompactedFiles(compactReady);

                segments.clear();
                File destLog = new File(baseDir, stagedLogName);
                File destIndex = new File(baseDir, stagedIndexName);
                long count = destIndex.length() / INDEX_ENTRY_BYTES;
                SegmentInfo newSeg = new SegmentInfo(0L, destLog, destIndex, count);
                segments.add(newSeg);
                openSegmentForAppend(newSeg);
                // nextOffset is deliberately unchanged: offsets are never reissued
            } finally {
                compactLock.delete();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Moves committed compaction output into the partition directory. Idempotent, so recovery
     * can re-run it after a crash at any point: segment files that compaction doesn't replace
     * are deleted, then each file still in {@code readyDir} is moved over its target.
     */
    private void installCompactedFiles(File readyDir) throws IOException {
        File dir = new File(baseDir);
        Set<String> compactedNames = Set.of(String.format("%020d.log", 0L), String.format("%020d.index", 0L));
        File[] oldFiles = dir.listFiles((d, name) ->
                (name.endsWith(".log") || name.endsWith(".index")) && !compactedNames.contains(name));
        if (oldFiles != null) {
            for (File f : oldFiles) {
                Files.deleteIfExists(f.toPath());
            }
        }
        File[] readyFiles = readyDir.listFiles();
        if (readyFiles != null) {
            for (File f : readyFiles) {
                Files.move(f.toPath(), new File(dir, f.getName()).toPath(),
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
        }
        Files.deleteIfExists(readyDir.toPath());
        fsyncDirectory(dir);
    }

    private void writeMetaNextOffset(long value) throws IOException {
        File tmp = new File(baseDir, META_FILE + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp.toPath(), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.allocate(2 + 1 + 8);
            buf.putShort(META_MAGIC);
            buf.put(META_VERSION);
            buf.putLong(value);
            buf.flip();
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        }
        Files.move(tmp.toPath(), new File(baseDir, META_FILE).toPath(),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /** @return persisted nextOffset floor, or 0 if there is no usable .meta file */
    private long readMetaNextOffset() {
        File meta = new File(baseDir, META_FILE);
        if (!meta.exists()) {
            return 0;
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(meta))) {
            short magic = in.readShort();
            byte version = in.readByte();
            if (magic != META_MAGIC || version != META_VERSION) {
                LOGGER.warning("Partition " + id + ": ignoring unrecognized " + META_FILE);
                return 0;
            }
            return in.readLong();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Partition " + id + ": failed to read " + META_FILE, e);
            return 0;
        }
    }

    private static void fsyncDirectory(File dir) {
        try (FileChannel ch = FileChannel.open(dir.toPath(), StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException e) {
            // Directory fsync isn't supported on every platform; renames stay atomic, just not yet durable
        }
    }

    private static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
        }
        dir.delete();
    }

    private List<CompactedRecord> readAllRecordsWithOffsets() throws IOException {
        List<CompactedRecord> list = new ArrayList<>();
        for (SegmentInfo seg : segments) {
            if (!seg.indexFile.exists() || !seg.logFile.exists()) continue;
            try (RandomAccessFile idx = new RandomAccessFile(seg.indexFile, "r");
                 RandomAccessFile log = new RandomAccessFile(seg.logFile, "r")) {
                long count = idx.length() / 16;
                for (long i = 0; i < count; i++) {
                    idx.seek(i * 16);
                    long off = idx.readLong();
                    long pos = idx.readLong();
                    log.seek(pos);
                    int len = log.readInt();
                    byte[] data = new byte[len];
                    log.readFully(data);
                    list.add(new CompactedRecord(off, data));
                }
            }
        }
        return list;
    }

    public int getId() { return id; }
    public String getBaseDir() { return baseDir; }
    public int getLeader() { return leader; }
    public void setLeader(int leader) { this.leader = leader; }
    public List<Integer> getFollowers() { return followers; }
    public void setFollowers(List<Integer> followers) { this.followers = followers; }
}
