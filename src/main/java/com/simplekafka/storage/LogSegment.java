package com.simplekafka.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Represents a single physical segment of a partition log (.log) and its corresponding index (.sindex).
 *
 * THREAD SAFETY:
 * - Appends are strictly synchronized per LogSegment instance, guaranteeing monotonic offsets,
 *   consistent currentSize progression, and atomic sparse index updates without race conditions.
 * - Reads operate via positional FileChannel.read(ByteBuffer, position), which is thread-safe
 *   and lock-free for concurrent reader threads.
 */
public class LogSegment implements AutoCloseable {
    private final long baseOffset;
    private final FileChannel logChannel;
    private final SparseOffsetIndex sparseIndex;
    private final File logFile;
    private final File indexFile;

    private long currentSize;
    private long nextOffset;

    public LogSegment(File logFile, File indexFile, long baseOffset) throws IOException {
        this.logFile = logFile;
        this.indexFile = indexFile;
        this.baseOffset = baseOffset;
        this.logChannel = FileChannel.open(
                logFile.toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );
        this.currentSize = logChannel.size();
        this.sparseIndex = SparseOffsetIndex.loadOrRebuild(indexFile, logFile, baseOffset);
        this.nextOffset = recoverNextOffset();
    }

    /**
     * Appends a message to this segment in a thread-safe manner.
     *
     * @param message Raw payload bytes
     * @return Assigned 0-based logical offset
     * @throws IOException If disk write fails
     */
    public synchronized long append(byte[] message) throws IOException {
        long writePosition = currentSize;
        int recordLength = 4 + message.length;

        ByteBuffer buf = ByteBuffer.allocate(recordLength);
        buf.putInt(message.length);
        buf.put(message);
        buf.flip();

        while (buf.hasRemaining()) {
            writePosition += logChannel.write(buf, writePosition);
        }

        long assignedOffset = nextOffset;
        sparseIndex.maybeAddEntry(assignedOffset, currentSize);

        currentSize += recordLength;
        nextOffset++;
        return assignedOffset;
    }

    /**
     * Appends a batch of messages in a single consolidated I/O write syscall.
     * Drastically minimizes system call overhead for group-commit batching.
     */
    public synchronized long[] appendBatch(List<byte[]> messages) throws IOException {
        if (messages == null || messages.isEmpty()) {
            return new long[0];
        }

        int totalBytes = 0;
        for (byte[] msg : messages) {
            totalBytes += 4 + msg.length;
        }

        ByteBuffer consolidated = ByteBuffer.allocate(totalBytes);
        long[] assignedOffsets = new long[messages.size()];
        long recordPos = currentSize;

        for (int i = 0; i < messages.size(); i++) {
            byte[] msg = messages.get(i);
            consolidated.putInt(msg.length);
            consolidated.put(msg);

            long assigned = nextOffset + i;
            assignedOffsets[i] = assigned;
            sparseIndex.maybeAddEntry(assigned, recordPos);
            recordPos += 4 + msg.length;
        }

        consolidated.flip();
        long destPos = currentSize;
        while (consolidated.hasRemaining()) {
            destPos += logChannel.write(consolidated, destPos);
        }

        currentSize += totalBytes;
        nextOffset += messages.size();
        return assignedOffsets;
    }

    /**
     * Reads a single record at the target logical offset.
     * Lock-free positional read from FileChannel; OS page cache serves hot reads.
     */
    public byte[] read(long offset) throws IOException {
        long position = sparseIndex.lookupPosition(offset);
        return scanForOffset(position, offset);
    }

    /**
     * Looks up physical byte position for an offset (used by zero-copy Netty sendfile).
     */
    public synchronized long lookupPhysicalPosition(long offset) {
        return sparseIndex.lookupPosition(offset);
    }

    private byte[] scanForOffset(long startPosition, long targetOffset) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(4);
        long curPos = startPosition;
        long curOffset = sparseIndex.lookupOffsetForPosition(startPosition);

        while (curPos < currentSize) {
            header.clear();
            int bytesRead = logChannel.read(header, curPos);
            if (bytesRead < 4) {
                break;
            }
            header.flip();
            int payloadLen = header.getInt();

            if (curOffset == targetOffset) {
                ByteBuffer payload = ByteBuffer.allocate(payloadLen);
                int read = 0;
                while (read < payloadLen) {
                    int r = logChannel.read(payload, curPos + 4 + read);
                    if (r <= 0) break;
                    read += r;
                }
                return payload.array();
            }

            curPos += 4 + payloadLen;
            curOffset++;
            if (curOffset > targetOffset) {
                break;
            }
        }
        return null;
    }

    public FileChannel getChannel() {
        return logChannel;
    }

    public synchronized long getCurrentSize() {
        return currentSize;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public synchronized long getNextOffset() {
        return nextOffset;
    }

    public SparseOffsetIndex getSparseIndex() {
        return sparseIndex;
    }

    public synchronized void flush(boolean forceFsync) throws IOException {
        sparseIndex.flush();
        if (forceFsync) {
            logChannel.force(false);
        }
    }

    public synchronized void flush() throws IOException {
        flush(false);
    }

    @Override
    public synchronized void close() throws IOException {
        flush(true);
        logChannel.close();
    }

    private long recoverNextOffset() throws IOException {
        if (currentSize == 0) {
            return baseOffset;
        }
        long startPos = sparseIndex.lookupPosition(Long.MAX_VALUE);
        long curOffset = sparseIndex.lookupOffsetForPosition(startPos);
        long curPos = startPos;
        ByteBuffer header = ByteBuffer.allocate(4);

        while (curPos < currentSize) {
            header.clear();
            int r = logChannel.read(header, curPos);
            if (r < 4) {
                // Incomplete length header at EOF: truncate partial record
                logChannel.truncate(curPos);
                this.currentSize = curPos;
                break;
            }
            header.flip();
            int len = header.getInt();
            if (len < 0 || curPos + 4 + len > currentSize) {
                // Truncated or corrupt payload at EOF: roll back to last clean boundary
                logChannel.truncate(curPos);
                this.currentSize = curPos;
                break;
            }
            curPos += 4 + len;
            curOffset++;
        }
        return Math.max(baseOffset, curOffset);
    }
}
