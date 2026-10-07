package com.simplekafka.broker;

import com.simplekafka.storage.WriteAheadLog.DurabilityMode;
import com.simplekafka.tiered.RemoteSegmentId;
import com.simplekafka.tiered.TieredStorageConfig;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private final TieredStorageConfig tiered;
    private final String topicName;
    private final Set<CompletableFuture<Void>> pendingUploads = ConcurrentHashMap.newKeySet();
    private final Map<Long, CompletableFuture<Void>> inflightDownloads = new ConcurrentHashMap<>();
    // Access-ordered: least recently read cached remote segment first
    private final LinkedHashMap<Long, Boolean> remoteCache = new LinkedHashMap<>(16, 0.75f, true);
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
    // Replication: high watermark (-1 = not replicated, HW tracks the log end), append/HW signal,
    // and the leader-epoch cache used to find where a follower's log diverges from a new leader's
    private volatile long highWatermark = -1;
    private final Object replicationSignal = new Object();
    private long lastHwCheckpointNanos;
    private final LeaderEpochCache leaderEpochs;
    private volatile boolean tieringEnabled = true;
    private volatile Runnable appendListener;

    private final Object flushMonitor = new Object();
    private long durableUpToOffset = -1;
    private boolean flushInProgress = false;

    public Partition(int id, String topicBaseDir) {
        this(id, topicBaseDir, DurabilityMode.OS_PAGE_CACHE);
    }

    public Partition(int id, String topicBaseDir, DurabilityMode durabilityMode) {
        this(id, topicBaseDir, durabilityMode, null);
    }

    /**
     * @param tiered remote storage for sealed segments, or null to keep everything local.
     *               Tiered partitions cannot be compacted (as in Kafka).
     */
    public Partition(int id, String topicBaseDir, DurabilityMode durabilityMode, TieredStorageConfig tiered) {
        this.id = id;
        this.durabilityMode = durabilityMode;
        this.tiered = tiered;
        this.topicName = new File(topicBaseDir).getName();
        this.leaderEpochs = new LeaderEpochCache(new File(topicBaseDir + File.separator + "partition-" + id, LeaderEpochCache.FILE));
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

        // Tiered storage state, guarded by the partition lock
        boolean uploaded;
        boolean uploading;
        boolean local = true;

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
                
                loadRemoteManifest();
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
            long checkpointedHw = readHighWatermarkCheckpoint();
            if (checkpointedHw >= 0) {
                highWatermark = Math.min(checkpointedHw, nextOffset.get());
            }
            maybeUploadSealedSegments(); // sealed before a crash, never uploaded
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

    /** Appends and returns once the record meets this partition's durability mode. */
    public long append(byte[] message) throws IOException {
        long offset = appendBuffered(message);
        awaitDurable(offset);
        return offset;
    }

    /**
     * Appends to the page cache and returns the offset without waiting for fsync. The caller
     * must call {@link #awaitDurable(long)} before acknowledging the write. Lets callers that
     * hold their own lock (e.g. idempotence bookkeeping) release it before the fsync wait, so
     * concurrent producers still share one group commit.
     */
    public long appendBuffered(byte[] message) throws IOException {
        return appendAt(-1, message);
    }

    /**
     * Follower append at the offset the leader assigned. Offsets may skip (compaction gaps on
     * the leader) but never go backwards: divergent tails are removed with {@link #truncateTo}
     * first. Waits for this partition's durability mode before returning, so the follower's
     * next fetch offset (which acknowledges the record to the leader) is honest.
     */
    public long appendReplicated(long offset, byte[] message) throws IOException {
        long assigned = appendAt(offset, message);
        awaitDurable(assigned);
        return assigned;
    }

    private long appendAt(long requestedOffset, byte[] message) throws IOException {
        long currentOffset;
        lock.writeLock().lock();
        try {
            currentOffset = requestedOffset < 0 ? nextOffset.get() : requestedOffset;
            if (currentOffset < nextOffset.get()) {
                throw new IOException("Replicated offset " + currentOffset + " is below the log end " + nextOffset.get()
                        + " of " + topicName + "-" + id + "; truncate first");
            }
            writeRecordLocked(currentOffset, message);
        } finally {
            lock.writeLock().unlock();
        }
        afterAppend();
        return currentOffset;
    }

    /**
     * Appends a batch contiguously under one lock acquisition (no other producer's records can
     * interleave) and returns the first record's offset. Callers wait with {@link #awaitDurable}
     * on the last offset, so the whole batch shares a single group commit.
     */
    public long appendBufferedBatch(java.util.List<byte[]> messages) throws IOException {
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("Empty batch");
        }
        long baseOffset;
        lock.writeLock().lock();
        try {
            baseOffset = nextOffset.get();
            for (int i = 0; i < messages.size(); i++) {
                writeRecordLocked(baseOffset + i, messages.get(i));
            }
        } finally {
            lock.writeLock().unlock();
        }
        afterAppend();
        return baseOffset;
    }

    /** Write lock held. */
    private void writeRecordLocked(long offset, byte[] message) throws IOException {
        if (activeLogChannel.size() > 0 && activeLogChannel.size() + message.length + 4 > MAX_SEGMENT_SIZE) {
            createNewSegment(offset);
            maybeUploadSealedSegments();
        }
        long position = activeLogChannel.position();
        ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        while (buffer.hasRemaining()) {
            activeLogChannel.write(buffer);
        }
        updateIndex(offset, position);
        segments.get(segments.size() - 1).currentMessageCount++;
        nextOffset.set(offset + 1);
    }

    private void afterAppend() {
        synchronized (replicationSignal) {
            replicationSignal.notifyAll(); // wake replica fetches long-polling for new data
        }
        Runnable listener = appendListener;
        if (listener != null) {
            listener.run(); // leader: an ISR of one commits on append, without waiting for a fetch
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
    public void awaitDurable(long offset) throws IOException {
        if (durabilityMode != DurabilityMode.PHYSICAL_FSYNC) {
            return;
        }
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

            long flushedUpTo;
            FileChannel logChannel;
            FileChannel indexChannel;
            // Snapshot under the lock, force outside it: holding the lock across the ~ms fsync
            // would stall appenders, so each flush would cover only the one record queued
            // behind it instead of everything appended meanwhile.
            lock.readLock().lock();
            try {
                flushedUpTo = nextOffset.get() - 1;
                logChannel = activeLogChannel;
                indexChannel = activeIndexChannel;
            } finally {
                lock.readLock().unlock();
            }

            IOException failure = null;
            try {
                logChannel.force(true);
                indexChannel.force(true);
            } catch (ClosedChannelException e) {
                // The segment rolled (rolls force before closing in PHYSICAL_FSYNC mode) or was
                // replaced by compaction output that is fsynced before install. Either way the
                // snapshotted records are already on disk.
            } catch (IOException e) {
                failure = e;
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
        return readRecords(offset, maxBytes, Long.MAX_VALUE);
    }

    /** As {@link #readRecords(long, int)}, but only records with offset below {@code maxOffsetExclusive}. */
    public List<Record> readRecords(long offset, int maxBytes, long maxOffsetExclusive) throws IOException {
        lock.readLock().lock();
        try {
            if (offset >= nextOffset.get() || segments.isEmpty()) {
                return Collections.emptyList();
            }

            List<Record> records = new ArrayList<>();
            int bytesRead = 0;
            for (int s = findSegmentIndex(Math.max(offset, 0)); s < segments.size(); s++) {
                SegmentInfo segment = segments.get(s);
                if (!segment.local && !isCached(segment.baseOffset)) {
                    if (!records.isEmpty()) {
                        return records; // next fetch downloads it
                    }
                    lock.readLock().unlock();
                    try {
                        // Download outside the lock so appends aren't stalled by remote I/O
                        downloadToCache(segment.baseOffset);
                    } finally {
                        lock.readLock().lock();
                    }
                    // Segments may have changed while unlocked; re-resolve (records is still empty)
                    s = findSegmentIndex(Math.max(offset, 0)) - 1;
                    continue;
                }
                try (RandomAccessFile index = new RandomAccessFile(readableIndex(segment), "r");
                     RandomAccessFile log = new RandomAccessFile(readableLog(segment), "r")) {
                    long entries = index.length() / INDEX_ENTRY_BYTES;
                    for (long e = firstEntryAtOrAfter(index, entries, offset); e < entries; e++) {
                        index.seek(e * INDEX_ENTRY_BYTES);
                        long recordOffset = index.readLong();
                        long position = index.readLong();
                        if (recordOffset >= maxOffsetExclusive) {
                            return records;
                        }
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

    // ---------------------------------------------------------------------------------
    // Replication support
    // ---------------------------------------------------------------------------------

    public LeaderEpochCache leaderEpochs() {
        return leaderEpochs;
    }

    /** Records below this offset are on every in-sync replica; consumers never read past it. */
    public long getHighWatermark() {
        long hw = highWatermark;
        return hw < 0 ? nextOffset.get() : Math.min(hw, nextOffset.get());
    }

    public boolean isReplicated() {
        return highWatermark >= 0;
    }

    /** Switches to replicated mode (HW managed by the leader) and sets the high watermark. */
    public void setHighWatermark(long hw) {
        synchronized (replicationSignal) {
            highWatermark = Math.max(0, hw);
            replicationSignal.notifyAll();
        }
        long now = System.nanoTime();
        if (now - lastHwCheckpointNanos > TimeUnit.MILLISECONDS.toNanos(200)) {
            lastHwCheckpointNanos = now;
            writeHighWatermarkCheckpoint(highWatermark);
        }
        maybeUploadSealedSegments();
    }

    /** Blocks until the high watermark passes {@code offset} (acks=all). */
    public boolean awaitHighWatermark(long offset, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        synchronized (replicationSignal) {
            while (getHighWatermark() <= offset) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(replicationSignal, remaining);
            }
            return true;
        }
    }

    /** Long-poll for replica fetches: blocks until the log end passes {@code offset} or the timeout. */
    public void awaitAppendBeyond(long offset, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        synchronized (replicationSignal) {
            while (nextOffset.get() <= offset) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return;
                }
                TimeUnit.NANOSECONDS.timedWait(replicationSignal, remaining);
            }
        }
    }

    /**
     * Removes every record with offset >= {@code offset} (a follower's divergent, uncommitted
     * tail). Never touches remote segments: anything tiered is below the high watermark.
     */
    public void truncateTo(long offset) throws IOException {
        lock.writeLock().lock();
        try {
            if (offset >= nextOffset.get()) {
                return;
            }
            LOGGER.warning("Partition " + topicName + "-" + id + ": truncating log from " + nextOffset.get() + " to " + offset);
            while (segments.size() > 1 && segments.get(segments.size() - 1).baseOffset >= offset) {
                SegmentInfo removed = segments.remove(segments.size() - 1);
                if (!removed.local) {
                    throw new IOException("Refusing to truncate into remote segment " + removed.baseOffset);
                }
                activeLogChannel.close();
                activeLogFile.close();
                activeIndexChannel.close();
                activeIndexFile.close();
                removed.logFile.delete();
                removed.indexFile.delete();
                openSegmentForAppend(segments.get(segments.size() - 1));
            }
            SegmentInfo last = segments.get(segments.size() - 1);
            if (!last.local) {
                throw new IOException("Refusing to truncate into remote segment " + last.baseOffset);
            }
            activeLogChannel.close();
            activeLogFile.close();
            activeIndexChannel.close();
            activeIndexFile.close();
            try (RandomAccessFile index = new RandomAccessFile(last.indexFile, "rw");
                 RandomAccessFile log = new RandomAccessFile(last.logFile, "rw")) {
                long entries = index.length() / INDEX_ENTRY_BYTES;
                long keep = firstEntryAtOrAfter(index, entries, offset);
                if (keep < entries) {
                    index.seek(keep * INDEX_ENTRY_BYTES + 8);
                    log.setLength(index.readLong());
                    index.setLength(keep * INDEX_ENTRY_BYTES);
                }
                last.currentMessageCount = keep;
            }
            openSegmentForAppend(last);
            nextOffset.set(offset);
            if (readMetaNextOffset() > offset) {
                writeMetaNextOffset(offset); // the truncated offsets were never committed
            }
            if (highWatermark > offset) {
                highWatermark = offset;
            }
            leaderEpochs.truncateFromEnd(offset);
            synchronized (flushMonitor) {
                durableUpToOffset = Math.min(durableUpToOffset, offset - 1);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static final String HW_CHECKPOINT = ".high_watermark";

    private void writeHighWatermarkCheckpoint(long hw) {
        try {
            Path tmp = Paths.get(baseDir, HW_CHECKPOINT + ".tmp");
            Files.writeString(tmp, Long.toString(hw));
            Files.move(tmp, Paths.get(baseDir, HW_CHECKPOINT), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOGGER.log(Level.FINE, "High watermark checkpoint failed", e); // advisory: recomputed by the leader
        }
    }

    private long readHighWatermarkCheckpoint() {
        try {
            Path file = Paths.get(baseDir, HW_CHECKPOINT);
            return Files.exists(file) ? Long.parseLong(Files.readString(file).trim()) : -1;
        } catch (IOException | NumberFormatException e) {
            return -1;
        }
    }

    /** Called after every append; the replica manager sets it while this broker leads the partition. */
    public void setAppendListener(Runnable appendListener) {
        this.appendListener = appendListener;
    }

    /** Only a partition's leader tiers its segments (replicas would upload the same objects). */
    public void setTieringEnabled(boolean enabled) {
        tieringEnabled = enabled;
        if (enabled) {
            maybeUploadSealedSegments();
        }
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
        if (tiered != null) {
            throw new IOException("Compaction is not supported on tiered partitions (" + topicName + "-" + id + ")");
        }
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

    // ---------------------------------------------------------------------------------
    // Tiered storage: sealed segments are uploaded asynchronously on roll. Once uploaded,
    // all but the newest N sealed segments lose their local copy and are read through an
    // LRU cache of downloaded segments. The active segment is always local.
    // ---------------------------------------------------------------------------------

    private static final String REMOTE_MANIFEST = ".remote_segments";
    private static final String REMOTE_CACHE_DIR = ".remote_cache";

    /** Uploads sealed segments whose every record is below the high watermark (committed). */
    private void maybeUploadSealedSegments() {
        if (tiered == null || !tieringEnabled) {
            return;
        }
        lock.writeLock().lock();
        try {
            long hw = getHighWatermark();
            for (int i = 0; i < segments.size() - 1; i++) {
                SegmentInfo segment = segments.get(i);
                if (segment.local && !segment.uploaded && !segment.uploading && segments.get(i + 1).baseOffset <= hw) {
                    segment.uploading = true;
                    scheduleUpload(segment);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Called with the write lock held. */
    private void scheduleUpload(SegmentInfo segment) {
        if (tiered == null) {
            return;
        }
        RemoteSegmentId remoteId = new RemoteSegmentId(topicName, id, segment.baseOffset);
        CompletableFuture<Void> upload = tiered.remote()
                .copySegment(remoteId, segment.logFile.toPath(), segment.indexFile.toPath())
                .thenRun(() -> onUploaded(segment));
        pendingUploads.add(upload);
        upload.whenComplete((v, e) -> {
            pendingUploads.remove(upload);
            segment.uploading = false;
            if (e != null) {
                // Stays local and un-uploaded; retried on the next broker start
                LOGGER.log(Level.WARNING, "Partition " + topicName + "-" + id + ": upload of segment "
                        + segment.baseOffset + " failed", e);
            }
        });
    }

    private void onUploaded(SegmentInfo segment) {
        lock.writeLock().lock();
        try {
            segment.uploaded = true;
            writeRemoteManifest();
            enforceLocalRetention();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Deletes local copies of uploaded sealed segments beyond the newest N. Write lock held. */
    private void enforceLocalRetention() {
        int keep = tiered.localRetainedSealedSegments();
        if (keep < 0) {
            return;
        }
        int retained = 0;
        for (int i = segments.size() - 2; i >= 0; i--) { // newest sealed segment first
            SegmentInfo segment = segments.get(i);
            if (!segment.local) {
                continue;
            }
            if (!segment.uploaded || retained < keep) {
                retained++;
                continue;
            }
            segment.logFile.delete();
            segment.indexFile.delete();
            segment.local = false;
        }
    }

    /** Lines of "baseOffset recordCount" for every uploaded segment; written atomically. */
    private void writeRemoteManifest() throws IOException {
        StringBuilder manifest = new StringBuilder();
        for (SegmentInfo segment : segments) {
            if (segment.uploaded) {
                manifest.append(segment.baseOffset).append(' ').append(segment.currentMessageCount).append('\n');
            }
        }
        Path tmp = Paths.get(baseDir, REMOTE_MANIFEST + ".tmp");
        Files.writeString(tmp, manifest.toString());
        Files.move(tmp, Paths.get(baseDir, REMOTE_MANIFEST), StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    /** Merges uploaded segments into the segment list; those without local files are remote-only. */
    private void loadRemoteManifest() throws IOException {
        Path manifest = Paths.get(baseDir, REMOTE_MANIFEST);
        if (!Files.exists(manifest)) {
            return;
        }
        for (String line : Files.readAllLines(manifest)) {
            if (line.isBlank()) {
                continue;
            }
            String[] parts = line.trim().split(" ");
            long base = Long.parseLong(parts[0]);
            long count = Long.parseLong(parts[1]);
            SegmentInfo existing = null;
            for (SegmentInfo segment : segments) {
                if (segment.baseOffset == base) {
                    existing = segment;
                }
            }
            if (existing == null) {
                existing = new SegmentInfo(base, new File(baseDir, String.format("%020d.log", base)),
                        new File(baseDir, String.format("%020d.index", base)), count);
                existing.local = false;
                segments.add(existing);
            }
            existing.uploaded = true;
        }
    }

    private File cacheDir() {
        return new File(baseDir, REMOTE_CACHE_DIR);
    }

    private File readableLog(SegmentInfo segment) {
        return segment.local ? segment.logFile : new File(cacheDir(), String.format("%020d.log", segment.baseOffset));
    }

    private File readableIndex(SegmentInfo segment) {
        return segment.local ? segment.indexFile : new File(cacheDir(), String.format("%020d.index", segment.baseOffset));
    }

    private boolean isCached(long baseOffset) {
        synchronized (remoteCache) {
            return remoteCache.get(baseOffset) != null; // get() also refreshes LRU order
        }
    }

    /** Downloads a remote segment into the cache; concurrent readers share one download. */
    private void downloadToCache(long baseOffset) throws IOException {
        CompletableFuture<Void> download = inflightDownloads.computeIfAbsent(baseOffset, base -> {
            File dir = cacheDir();
            dir.mkdirs();
            File log = new File(dir, String.format("%020d.log", base));
            File index = new File(dir, String.format("%020d.index", base));
            return tiered.remote()
                    .fetchSegment(new RemoteSegmentId(topicName, id, base), log.toPath(), index.toPath())
                    .thenRun(() -> {
                        synchronized (remoteCache) {
                            remoteCache.put(base, Boolean.TRUE);
                            evictCacheOverflow();
                        }
                    })
                    .whenComplete((v, e) -> inflightDownloads.remove(base));
        });
        try {
            download.get(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted downloading remote segment " + baseOffset, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("Failed to download remote segment " + topicName + "-" + id + "@" + baseOffset,
                    e instanceof ExecutionException ? e.getCause() : e);
        }
    }

    private void evictCacheOverflow() {
        var it = remoteCache.keySet().iterator();
        while (remoteCache.size() > tiered.cachedRemoteSegments() && it.hasNext()) {
            long evicted = it.next();
            it.remove();
            new File(cacheDir(), String.format("%020d.log", evicted)).delete();
            new File(cacheDir(), String.format("%020d.index", evicted)).delete();
        }
    }

    /** Completes when every upload scheduled so far has finished (for tests and shutdown). */
    public CompletableFuture<Void> awaitTieringIdle() {
        return CompletableFuture.allOf(pendingUploads.toArray(new CompletableFuture[0]))
                .exceptionally(e -> null);
    }

    /** Base offsets of segments whose only copy is remote. */
    public List<Long> remoteOnlySegments() {
        lock.readLock().lock();
        try {
            List<Long> result = new ArrayList<>();
            for (SegmentInfo segment : segments) {
                if (!segment.local) {
                    result.add(segment.baseOffset);
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public String getTopicName() { return topicName; }

    public int getId() { return id; }
    public String getBaseDir() { return baseDir; }
    public int getLeader() { return leader; }
    public void setLeader(int leader) { this.leader = leader; }
    public List<Integer> getFollowers() { return followers; }
    public void setFollowers(List<Integer> followers) { this.followers = followers; }
}
