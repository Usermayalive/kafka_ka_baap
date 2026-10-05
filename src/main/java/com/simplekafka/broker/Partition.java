package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Partition {
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final long MAX_SEGMENT_SIZE = 1024 * 1024; // 1MB

    private final int id;
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

    public Partition(int id, String topicBaseDir) {
        this.id = id;
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

            // Crash recovery: check for incomplete previous compaction
            File compactLock = new File(baseDir, ".compact.lock");
            File compactStaging = new File(baseDir, ".compact_staging");
            if (compactLock.exists() || compactStaging.exists()) {
                LOGGER.warning("Detected incomplete previous compaction for partition " + id + ". Cleaning up staging files...");
                if (compactStaging.exists()) {
                    File[] stagingFiles = compactStaging.listFiles();
                    if (stagingFiles != null) {
                        for (File f : stagingFiles) f.delete();
                    }
                    compactStaging.delete();
                }
                if (compactLock.exists()) {
                    compactLock.delete();
                }
            }

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
                nextOffset.set(lastSegment.baseOffset + lastSegment.currentMessageCount);
                openSegmentForAppend(lastSegment);
            } else {
                createNewSegment(0);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);
            throw new RuntimeException("Failed to initialize partition", e);
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
        if (activeLogChannel != null) {
            activeLogChannel.close();
            activeLogFile.close();
        }
        if (activeIndexChannel != null) {
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
            activeLogChannel.force(true);

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
        activeIndexChannel.force(true);
    }

    public List<byte[]> readMessages(long offset, int maxBytes) throws IOException {
        lock.readLock().lock();
        try {
            if (offset >= nextOffset.get() || offset < 0) {
                return Collections.emptyList();
            }

            SegmentInfo segment = findSegmentForOffset(offset);
            if (segment == null) {
                return Collections.emptyList();
            }

            long position = findPositionForOffset(segment, offset);
            if (position < 0) {
                return Collections.emptyList();
            }

            List<byte[]> messages = new ArrayList<>();
            int bytesRead = 0;

            try (RandomAccessFile readLog = new RandomAccessFile(segment.logFile, "r")) {
                readLog.seek(position);

                while (bytesRead < maxBytes && readLog.getFilePointer() < readLog.length()) {
                    int messageSize = readLog.readInt();
                    
                    // Always return at least one message to prevent blocking
                    if (bytesRead > 0 && bytesRead + 4 + messageSize > maxBytes) {
                        break; 
                    }

                    byte[] message = new byte[messageSize];
                    readLog.readFully(message);
                    messages.add(message);
                    bytesRead += (4 + messageSize);
                }
            }
            
            return messages;

        } finally {
            lock.readLock().unlock();
        }
    }

    private SegmentInfo findSegmentForOffset(long offset) {
        int left = 0;
        int right = segments.size() - 1;
        SegmentInfo result = null;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            SegmentInfo midSegment = segments.get(mid);

            if (midSegment.baseOffset <= offset) {
                result = midSegment;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return result;
    }

    private long findPositionForOffset(SegmentInfo segment, long offset) throws IOException {
        try (RandomAccessFile readIndex = new RandomAccessFile(segment.indexFile, "r")) {
            long entryCount = readIndex.length() / 16;
            if (entryCount == 0) return -1;

            long low = 0;
            long high = entryCount - 1;
            long foundPosition = -1;

            while (low <= high) {
                long mid = low + (high - low) / 2;
                readIndex.seek(mid * 16);
                long storedOffset = readIndex.readLong();
                long dataPos = readIndex.readLong();

                if (storedOffset == offset) {
                    return dataPos;
                } else if (storedOffset < offset) {
                    low = mid + 1;
                } else {
                    foundPosition = dataPos;
                    high = mid - 1;
                }
            }
            return foundPosition;
        }
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

            File compactLock = new File(baseDir, ".compact.lock");
            File compactStaging = new File(baseDir, ".compact_staging");
            if (compactStaging.exists()) {
                File[] stagingFiles = compactStaging.listFiles();
                if (stagingFiles != null) {
                    for (File f : stagingFiles) f.delete();
                }
                compactStaging.delete();
            }
            compactStaging.mkdirs();
            compactLock.createNewFile();

            String stagedLogName = String.format("%020d.log", 0L);
            String stagedIndexName = String.format("%020d.index", 0L);
            File stagedLog = new File(compactStaging, stagedLogName);
            File stagedIndex = new File(compactStaging, stagedIndexName);

            long maxOffsetSeen = -1;
            try {
                try (RandomAccessFile sLogRaf = new RandomAccessFile(stagedLog, "rw");
                     FileChannel sLogCh = sLogRaf.getChannel();
                     RandomAccessFile sIdxRaf = new RandomAccessFile(stagedIndex, "rw");
                     FileChannel sIdxCh = sIdxRaf.getChannel()) {

                    long curPos = 0;
                    ByteBuffer idxBuf = ByteBuffer.allocate(16);
                    for (CompactedRecord record : latestByKey.values()) {
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

                        if (record.offset() > maxOffsetSeen) {
                            maxOffsetSeen = record.offset();
                        }
                    }
                    sLogCh.force(true);
                    sIdxCh.force(true);
                }

                if (activeLogChannel != null) {
                    activeLogChannel.close();
                    activeLogFile.close();
                }
                if (activeIndexChannel != null) {
                    activeIndexChannel.close();
                    activeIndexFile.close();
                }

                File dir = new File(baseDir);
                File[] oldFiles = dir.listFiles((d, name) -> name.endsWith(".log") || name.endsWith(".index"));
                if (oldFiles != null) {
                    for (File f : oldFiles) {
                        f.delete();
                    }
                }

                File destLog = new File(baseDir, stagedLogName);
                File destIndex = new File(baseDir, stagedIndexName);
                Files.move(stagedLog.toPath(), destLog.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                Files.move(stagedIndex.toPath(), destIndex.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

                compactStaging.delete();
                compactLock.delete();

                segments.clear();
                long count = destIndex.length() / 16;
                SegmentInfo newSeg = new SegmentInfo(0L, destLog, destIndex, count);
                segments.add(newSeg);
                openSegmentForAppend(newSeg);

                if (maxOffsetSeen >= 0) {
                    nextOffset.set(maxOffsetSeen + 1);
                }
            } finally {
                if (compactLock.exists()) {
                    compactLock.delete();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
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

    private void appendRecordWithOffsetInternal(long offset, byte[] message) throws IOException {
        if (activeLogChannel.size() + message.length + 4 > MAX_SEGMENT_SIZE) {
            createNewSegment(offset);
        }

        long currentPosition = activeLogChannel.position();

        ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();

        while (buffer.hasRemaining()) {
            activeLogChannel.write(buffer);
        }
        activeLogChannel.force(true);

        updateIndex(offset, currentPosition);

        SegmentInfo activeSegment = segments.get(segments.size() - 1);
        activeSegment.currentMessageCount++;
    }

    private void appendRecordInternal(byte[] message) throws IOException {
        long currentOffset = nextOffset.get();
        appendRecordWithOffsetInternal(currentOffset, message);
        nextOffset.incrementAndGet();
    }

    public int getId() { return id; }
    public String getBaseDir() { return baseDir; }
    public int getLeader() { return leader; }
    public void setLeader(int leader) { this.leader = leader; }
    public List<Integer> getFollowers() { return followers; }
    public void setFollowers(List<Integer> followers) { this.followers = followers; }
}
