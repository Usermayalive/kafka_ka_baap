package com.simplekafka.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WriteAheadLog orchestrating active and sealed LogSegments with Group Commit batching.
 *
 * High-throughput append path: Multiple concurrent producers enqueue write requests.
 * The group committer batches writes, writes them sequentially to the active segment,
 * flushes periodically, and completes caller CompletableFutures.
 */
public class WriteAheadLog implements AutoCloseable {
    public static final long DEFAULT_MAX_SEGMENT_BYTES = 100 * 1024 * 1024; // 100 MB

    public enum DurabilityMode {
        OS_PAGE_CACHE,   // High-throughput OS buffer write (Kafka default)
        PHYSICAL_FSYNC   // Force physical fsync on every commit batch
    }

    private final File partitionDir;
    private final long maxSegmentBytes;
    private final DurabilityMode durabilityMode;
    private final List<LogSegment> segments = new CopyOnWriteArrayList<>();
    private volatile LogSegment activeSegment;

    private final BlockingQueue<WriteRequest> writeQueue = new LinkedBlockingQueue<>(65_536);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread committerThread;

    public record WriteRequest(byte[] payload, CompletableFuture<Long> future) {}

    public WriteAheadLog(File partitionDir) throws IOException {
        this(partitionDir, DEFAULT_MAX_SEGMENT_BYTES, DurabilityMode.OS_PAGE_CACHE);
    }

    public WriteAheadLog(File partitionDir, long maxSegmentBytes) throws IOException {
        this(partitionDir, maxSegmentBytes, DurabilityMode.OS_PAGE_CACHE);
    }

    public WriteAheadLog(File partitionDir, long maxSegmentBytes, DurabilityMode durabilityMode) throws IOException {
        this.partitionDir = partitionDir;
        this.maxSegmentBytes = maxSegmentBytes;
        this.durabilityMode = durabilityMode;

        if (!partitionDir.exists()) {
            partitionDir.mkdirs();
        }

        recoverSegments();

        this.committerThread = Thread.ofVirtual().name("wal-group-committer-" + partitionDir.getName()).start(this::runGroupCommitLoop);
    }

    private synchronized void recoverSegments() throws IOException {
        File[] logFiles = partitionDir.listFiles(f -> f.getName().endsWith(".log"));
        if (logFiles == null || logFiles.length == 0) {
            roll(0);
            return;
        }

        // Sort log files by base offset in filename (e.g., 00000000000000000000.log)
        java.util.Arrays.sort(logFiles, (a, b) -> {
            long offA = parseOffset(a.getName());
            long offB = parseOffset(b.getName());
            return Long.compare(offA, offB);
        });

        for (File logFile : logFiles) {
            long baseOffset = parseOffset(logFile.getName());
            String indexName = String.format("%020d.sindex", baseOffset);
            File indexFile = new File(partitionDir, indexName);
            LogSegment seg = new LogSegment(logFile, indexFile, baseOffset);
            segments.add(seg);
        }

        activeSegment = segments.get(segments.size() - 1);
    }

    private synchronized LogSegment roll(long newBaseOffset) throws IOException {
        if (activeSegment != null) {
            activeSegment.flush();
        }
        String prefix = String.format("%020d", newBaseOffset);
        File logFile = new File(partitionDir, prefix + ".log");
        File indexFile = new File(partitionDir, prefix + ".sindex");

        LogSegment newSegment = new LogSegment(logFile, indexFile, newBaseOffset);
        segments.add(newSegment);
        activeSegment = newSegment;
        return newSegment;
    }

    /**
     * Enqueues an append request with Group Commit batching.
     */
    public CompletableFuture<Long> appendAsync(byte[] message) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        if (!running.get()) {
            future.completeExceptionally(new IllegalStateException("WAL is closed"));
            return future;
        }

        try {
            if (!writeQueue.offer(new WriteRequest(message, future), 3, TimeUnit.SECONDS)) {
                future.completeExceptionally(new RejectedExecutionException("WAL write queue full"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * Synchronous append convenience method.
     */
    public long append(byte[] message) throws IOException {
        try {
            return appendAsync(message).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while appending", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw new IOException("Failed to append", e.getCause());
        }
    }

    public byte[] read(long offset) throws IOException {
        LogSegment seg = findSegmentForOffset(offset);
        if (seg == null) {
            return null;
        }
        return seg.read(offset);
    }

    public LogSegment findSegmentForOffset(long offset) {
        // Binary search or reverse iteration over segments
        for (int i = segments.size() - 1; i >= 0; i--) {
            LogSegment seg = segments.get(i);
            if (seg.getBaseOffset() <= offset) {
                return seg;
            }
        }
        return null;
    }

    private void runGroupCommitLoop() {
        List<WriteRequest> batch = new ArrayList<>(1024);
        while (running.get() || !writeQueue.isEmpty()) {
            try {
                WriteRequest first = writeQueue.poll(1, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                writeQueue.drainTo(batch, 1023);

                synchronized (this) {
                    List<byte[]> payloads = new ArrayList<>(batch.size());
                    List<WriteRequest> currentBatch = new ArrayList<>(batch.size());
                    long projectedSize = activeSegment.getCurrentSize();

                    for (WriteRequest req : batch) {
                        int recordBytes = 4 + req.payload().length;
                        if (projectedSize + recordBytes >= maxSegmentBytes && !payloads.isEmpty()) {
                            // Roll segment before writing next chunk
                            try {
                                long[] offsets = activeSegment.appendBatch(payloads);
                                for (int i = 0; i < currentBatch.size(); i++) {
                                    currentBatch.get(i).future().complete(offsets[i]);
                                }
                            } catch (Throwable t) {
                                for (WriteRequest r : currentBatch) {
                                    r.future().completeExceptionally(t);
                                }
                            }
                            payloads.clear();
                            currentBatch.clear();
                            try {
                                roll(activeSegment.getNextOffset());
                                projectedSize = activeSegment.getCurrentSize();
                            } catch (IOException e) {
                                req.future().completeExceptionally(e);
                                continue;
                            }
                        }
                        payloads.add(req.payload());
                        currentBatch.add(req);
                        projectedSize += recordBytes;
                    }

                    if (!payloads.isEmpty()) {
                        try {
                            long[] offsets = activeSegment.appendBatch(payloads);
                            for (int i = 0; i < currentBatch.size(); i++) {
                                currentBatch.get(i).future().complete(offsets[i]);
                            }
                        } catch (Throwable t) {
                            for (WriteRequest r : currentBatch) {
                                r.future().completeExceptionally(t);
                            }
                        }
                    }

                    try {
                        activeSegment.flush(durabilityMode == DurabilityMode.PHYSICAL_FSYNC);
                    } catch (IOException e) {
                        // Log flush error
                    }
                }
                batch.clear();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private long parseOffset(String filename) {
        String base = filename.substring(0, filename.lastIndexOf('.'));
        return Long.parseLong(base);
    }

    public LogSegment getActiveSegment() {
        return activeSegment;
    }

    public List<LogSegment> getSegments() {
        return segments;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        committerThread.interrupt();
        try {
            committerThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        for (LogSegment seg : segments) {
            seg.close();
        }
    }
}
