package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        long relativeOffset = offset - segment.baseOffset;
        if (relativeOffset < 0 || relativeOffset >= segment.currentMessageCount) {
            return -1;
        }

        try (RandomAccessFile readIndex = new RandomAccessFile(segment.indexFile, "r")) {
            long indexPosition = relativeOffset * 16;
            if (indexPosition >= readIndex.length()) {
                return -1;
            }
            
            readIndex.seek(indexPosition);
            long storedOffset = readIndex.readLong(); // Read offset
            long dataPosition = readIndex.readLong(); // Read position
            
            if (storedOffset == offset) {
                return dataPosition;
            } else {
                LOGGER.warning("Index offset mismatch. Expected: " + offset + " Found: " + storedOffset);
                return -1;
            }
        }
    }

    public int getId() { return id; }
    public int getLeader() { return leader; }
    public void setLeader(int leader) { this.leader = leader; }
    public List<Integer> getFollowers() { return followers; }
    public void setFollowers(List<Integer> followers) { this.followers = followers; }
}
