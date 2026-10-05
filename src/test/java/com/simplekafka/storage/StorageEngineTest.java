package com.simplekafka.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class StorageEngineTest {

    @TempDir
    Path tempDir;

    private File partitionDir;

    @BeforeEach
    void setUp() {
        partitionDir = tempDir.toFile();
    }

    @Test
    void testSparseOffsetIndexCreationAndLookup() throws IOException {
        File indexFile = new File(partitionDir, "00000000000000000000.sindex");
        SparseOffsetIndex index = new SparseOffsetIndex(new ArrayList<>(), indexFile);

        // Add entries every ~4KB
        index.maybeAddEntry(0, 0);
        index.maybeAddEntry(10, 2048); // Should NOT add (under 4KB threshold)
        assertEquals(1, index.getEntryCount());

        index.maybeAddEntry(25, 4096); // Should add (reaches 4KB)
        assertEquals(2, index.getEntryCount());

        index.maybeAddEntry(50, 8192); // Should add (reaches 8KB)
        assertEquals(3, index.getEntryCount());

        // Test binary search lookup
        assertEquals(0, index.lookupPosition(0));
        assertEquals(0, index.lookupPosition(15)); // maps to floor <= 15 -> offset 0 (pos 0)
        assertEquals(4096, index.lookupPosition(25)); // exact match
        assertEquals(4096, index.lookupPosition(35)); // maps to floor <= 35 -> offset 25 (pos 4096)
        assertEquals(8192, index.lookupPosition(100)); // maps to floor <= 100 -> offset 50 (pos 8192)

        // Test persistence and reload via FFM MemorySegment
        index.flush();
        assertTrue(indexFile.exists());
        assertEquals(3 * SparseOffsetIndex.ENTRY_SIZE, indexFile.length());

        SparseOffsetIndex loaded = SparseOffsetIndex.loadFromFile(indexFile);
        assertEquals(3, loaded.getEntryCount());
        assertEquals(4096, loaded.lookupPosition(30));
    }

    @Test
    void testLogSegmentAppendAndRead() throws IOException {
        File logFile = new File(partitionDir, "00000000000000000000.log");
        File indexFile = new File(partitionDir, "00000000000000000000.sindex");

        try (LogSegment segment = new LogSegment(logFile, indexFile, 0)) {
            for (int i = 0; i < 100; i++) {
                byte[] msg = ("test-message-" + i).getBytes(StandardCharsets.UTF_8);
                long offset = segment.append(msg);
                assertEquals(i, offset);
            }

            for (int i = 0; i < 100; i++) {
                byte[] read = segment.read(i);
                assertNotNull(read, "Record at offset " + i + " should not be null");
                assertEquals("test-message-" + i, new String(read, StandardCharsets.UTF_8));
            }

            assertNull(segment.read(100));
        }

        // Verify recovery after close
        try (LogSegment recovered = new LogSegment(logFile, indexFile, 0)) {
            assertEquals(100, recovered.getNextOffset());
            byte[] read = recovered.read(42);
            assertEquals("test-message-42", new String(read, StandardCharsets.UTF_8));
        }
    }

    @Test
    void testLogSegmentConcurrentAppends() throws Exception {
        File logFile = new File(partitionDir, "concurrent.log");
        File indexFile = new File(partitionDir, "concurrent.sindex");

        int threadCount = 10;
        int messagesPerThread = 200;
        int totalMessages = threadCount * messagesPerThread;

        try (LogSegment segment = new LogSegment(logFile, indexFile, 0)) {
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(1);
            List<Future<List<Long>>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                futures.add(executor.submit(() -> {
                    latch.await();
                    List<Long> assignedOffsets = new ArrayList<>(messagesPerThread);
                    for (int m = 0; m < messagesPerThread; m++) {
                        byte[] payload = ("t" + threadId + "-m" + m).getBytes(StandardCharsets.UTF_8);
                        assignedOffsets.add(segment.append(payload));
                    }
                    return assignedOffsets;
                }));
            }

            latch.countDown(); // Start all threads simultaneously

            List<Long> allOffsets = new ArrayList<>();
            for (Future<List<Long>> f : futures) {
                allOffsets.addAll(f.get());
            }
            executor.shutdown();

            assertEquals(totalMessages, allOffsets.size());
            // Verify all offsets are unique and within [0, totalMessages - 1]
            allOffsets.sort(Long::compare);
            for (int i = 0; i < totalMessages; i++) {
                assertEquals((long) i, allOffsets.get(i), "Offset sequence should have zero gaps or duplicates");
            }

            // Verify each message is accurately readable
            for (int i = 0; i < totalMessages; i++) {
                byte[] read = segment.read(i);
                assertNotNull(read, "Offset " + i + " must be readable");
                assertTrue(new String(read, StandardCharsets.UTF_8).startsWith("t"));
            }
        }
    }

    @Test
    void testWriteAheadLogGroupCommitAndRolling() throws Exception {
        // Use a small max segment size (8 KB) to trigger segment rolling
        long smallSegmentBytes = 8 * 1024;
        try (WriteAheadLog wal = new WriteAheadLog(partitionDir, smallSegmentBytes)) {
            int count = 500;
            List<CompletableFuture<Long>> futures = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                byte[] payload = new byte[100]; // 100 bytes + 4 byte len = 104 bytes per record
                payload[0] = (byte) (i & 0xFF);
                futures.add(wal.appendAsync(payload));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

            for (int i = 0; i < count; i++) {
                long offset = futures.get(i).get();
                assertEquals((long) i, offset);
            }

            // Verify segment rolling occurred (500 records * ~104 bytes = ~52 KB -> ~7 segments)
            assertTrue(wal.getSegments().size() > 1, "WAL should have rolled into multiple segments");

            // Verify reads across all segments
            for (int i = 0; i < count; i++) {
                byte[] read = wal.read(i);
                assertNotNull(read, "Offset " + i + " should exist across rolled segments");
                assertEquals((byte) (i & 0xFF), read[0]);
            }
        }
    }

    @Test
    void testLegacyPartitionMigration() throws IOException {
        File legacyDir = new File(partitionDir, "partition-0");
        legacyDir.mkdirs();

        File oldLog = new File(legacyDir, "00000000000000000000.log");
        File oldIndex = new File(legacyDir, "00000000000000000000.index");

        // Write 1,000 records to old log and old dense index
        long curPos = 0;
        try (FileOutputStream logOut = new FileOutputStream(oldLog);
             FileOutputStream idxOut = new FileOutputStream(oldIndex)) {
            ByteBuffer idxBuf = ByteBuffer.allocate(16);
            ByteBuffer lenBuf = ByteBuffer.allocate(4);

            for (int i = 0; i < 1000; i++) {
                byte[] msg = ("legacy-msg-" + i).getBytes(StandardCharsets.UTF_8);
                
                // Write log: [len:4][payload]
                lenBuf.clear();
                lenBuf.putInt(msg.length);
                logOut.write(lenBuf.array());
                logOut.write(msg);

                // Write dense index: [offset:8][pos:8]
                idxBuf.clear();
                idxBuf.putLong(i);
                idxBuf.putLong(curPos);
                idxOut.write(idxBuf.array());

                curPos += 4 + msg.length;
            }
        }

        long denseIndexLength = oldIndex.length();
        assertEquals(1000 * 16, denseIndexLength, "Old dense index must have 1,000 entries (16,000 bytes)");

        // Perform migration
        LegacyPartitionMigrator migrator = new LegacyPartitionMigrator();
        migrator.migratePartition(legacyDir);

        // Verify old .index is gone and replaced by .sindex
        File newIndex = new File(legacyDir, "00000000000000000000.sindex");
        assertTrue(newIndex.exists(), "New .sindex file should exist");
        assertFalse(new File(legacyDir, "00000000000000000000.index").exists(), "Old dense .index should not exist in active dir");

        // Sparse index should be significantly smaller than dense index (4KB step vs every record)
        assertTrue(newIndex.length() < denseIndexLength / 4, 
                "Sparse index (" + newIndex.length() + " bytes) should be significantly smaller than dense index (" + denseIndexLength + " bytes)");

        // Read migrated data using LogSegment
        try (LogSegment segment = new LogSegment(new File(legacyDir, "00000000000000000000.log"), newIndex, 0)) {
            for (int i = 0; i < 1000; i++) {
                byte[] read = segment.read(i);
                assertNotNull(read, "Migrated record at offset " + i + " must be readable");
                assertEquals("legacy-msg-" + i, new String(read, StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void testCrashDuringMigrationSelfHealing() throws IOException {
        File legacyDir = new File(partitionDir, "partition-crash-test");
        legacyDir.mkdirs();

        File oldLog = new File(legacyDir, "00000000000000000000.log");
        File oldIndex = new File(legacyDir, "00000000000000000000.index");

        long curPos = 0;
        try (FileOutputStream logOut = new FileOutputStream(oldLog);
             FileOutputStream idxOut = new FileOutputStream(oldIndex)) {
            ByteBuffer idxBuf = ByteBuffer.allocate(16);
            ByteBuffer lenBuf = ByteBuffer.allocate(4);

            for (int i = 0; i < 200; i++) {
                byte[] msg = ("crash-test-msg-" + i).getBytes(StandardCharsets.UTF_8);
                lenBuf.clear();
                lenBuf.putInt(msg.length);
                logOut.write(lenBuf.array());
                logOut.write(msg);

                idxBuf.clear();
                idxBuf.putLong(i);
                idxBuf.putLong(curPos);
                idxOut.write(idxBuf.array());
                curPos += 4 + msg.length;
            }
        }

        // Simulate crash mid-migration:
        // 1. Create .migration.lock
        File lockFile = new File(legacyDir, ".migration.lock");
        assertTrue(lockFile.createNewFile(), "Lock file should be created");

        // 2. Create partially migrated staging directory with corrupt/incomplete content
        File stagingDir = new File(legacyDir.getParentFile(), legacyDir.getName() + ".migrating");
        stagingDir.mkdirs();
        File junkFile = new File(stagingDir, "corrupted_half_written.tmp");
        try (FileOutputStream fos = new FileOutputStream(junkFile)) {
            fos.write(new byte[]{0x01, 0x02, 0x03});
        }

        // Re-run migration. It must detect the lock, clean up corrupted staging dir, and complete successfully
        LegacyPartitionMigrator migrator = new LegacyPartitionMigrator();
        migrator.migratePartition(legacyDir);

        assertFalse(lockFile.exists(), "Lock file should be cleaned up after migration");
        assertFalse(stagingDir.exists(), "Staging dir should be cleanly promoted or removed");

        File newIndex = new File(legacyDir, "00000000000000000000.sindex");
        assertTrue(newIndex.exists(), "Migrated .sindex should exist");

        // Verify all 200 records are accurately readable
        try (LogSegment segment = new LogSegment(new File(legacyDir, "00000000000000000000.log"), newIndex, 0)) {
            for (int i = 0; i < 200; i++) {
                byte[] read = segment.read(i);
                assertNotNull(read, "Record at offset " + i + " must be readable after crash self-healing");
                assertEquals("crash-test-msg-" + i, new String(read, StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void testTwoThousandPartitionsLifecycle() throws Exception {
        int partitionCount = 2000;
        List<LogSegment> activeSegments = new ArrayList<>(partitionCount);
        File partitionsRoot = new File(partitionDir, "partitions-scale-test");
        partitionsRoot.mkdirs();

        try {
            // Open 2,000 partition segments concurrently
            for (int p = 0; p < partitionCount; p++) {
                File pDir = new File(partitionsRoot, "p-" + p);
                pDir.mkdirs();
                File logFile = new File(pDir, "00000000000000000000.log");
                File indexFile = new File(pDir, "00000000000000000000.sindex");
                LogSegment seg = new LogSegment(logFile, indexFile, 0);
                activeSegments.add(seg);
            }

            assertEquals(partitionCount, activeSegments.size());

            // Write 5 messages to each partition
            byte[] payload = "scale-test-payload".getBytes(StandardCharsets.UTF_8);
            for (LogSegment seg : activeSegments) {
                for (int m = 0; m < 5; m++) {
                    seg.append(payload);
                }
            }

            // Read back from partitions
            for (int p = 0; p < 100; p++) { // Sample read check
                LogSegment seg = activeSegments.get(p);
                byte[] read = seg.read(4);
                assertNotNull(read);
                assertEquals("scale-test-payload", new String(read, StandardCharsets.UTF_8));
            }

        } finally {
            // Close all 2,000 segments cleanly
            for (LogSegment seg : activeSegments) {
                try {
                    seg.close();
                } catch (Exception ignored) {}
            }
        }
    }

    @Test
    void testTenThousandIndexLoadFdsDeterministic() throws Exception {
        File indexFile = new File(partitionDir, "ten_thousand_loop.sindex");
        SparseOffsetIndex index = new SparseOffsetIndex(new ArrayList<>(), indexFile);
        for (int i = 0; i < 500; i++) {
            index.maybeAddEntry(i * 10, i * 4096);
        }
        index.flush();

        // Load 10,000 times in a loop. With MappedByteBuffer this exhausts FDs / VMA.
        // With FFM Arena.ofConfined() it deterministic unmaps immediately on exit of try-with-resources.
        for (int i = 0; i < 10_000; i++) {
            SparseOffsetIndex loaded = SparseOffsetIndex.loadFromFile(indexFile);
            assertNotNull(loaded);
            if (i % 2500 == 0) {
                assertEquals(0, loaded.lookupPosition(0));
            }
        }
    }

    @Test
    void testOneHundredThousandRecordsSparseIndexReduction() throws IOException {
        File denseFile = new File(partitionDir, "100k_dense.index");
        File sparseFile = new File(partitionDir, "100k_sparse.sindex");

        int recordCount = 100_000;
        int messageSize = 100; // 100-byte message

        // 100K dense entries = 100,000 * 16 bytes = 1,600,000 bytes
        try (FileOutputStream fos = new FileOutputStream(denseFile)) {
            ByteBuffer buf = ByteBuffer.allocate(16);
            long pos = 0;
            for (int i = 0; i < recordCount; i++) {
                buf.clear();
                buf.putLong(i);
                buf.putLong(pos);
                fos.write(buf.array());
                pos += 4 + messageSize;
            }
        }

        SparseOffsetIndex sparseIndex = new SparseOffsetIndex(new ArrayList<>(), sparseFile);
        long curPos = 0;
        for (int i = 0; i < recordCount; i++) {
            sparseIndex.maybeAddEntry(i, curPos);
            curPos += 4 + messageSize;
        }
        sparseIndex.flush();

        long denseBytes = denseFile.length();
        long sparseBytes = sparseFile.length();

        assertEquals(100_000 * 16, denseBytes);
        // 100,000 msgs * 104 bytes = 10,400,000 bytes. At 4096 bytes per index entry -> ~2,539 entries -> ~40,624 bytes
        assertTrue(sparseBytes < denseBytes / 25, 
                "Sparse index (" + sparseBytes + "B) must be at least 25x smaller than dense index (" + denseBytes + "B)");
    }

    @Test
    void testSimultaneousTwoThousandIndexMappingsOpen() throws Exception {
        int count = 2000;
        File root = new File(partitionDir, "simultaneous-indexes");
        root.mkdirs();

        List<SparseOffsetIndex> heldIndexes = new ArrayList<>(count);

        // Create and hold 2,000 index instances simultaneously in memory
        for (int i = 0; i < count; i++) {
            File idxFile = new File(root, String.format("index_%05d.sindex", i));
            SparseOffsetIndex idx = new SparseOffsetIndex(new ArrayList<>(), idxFile);
            for (int k = 0; k < 10; k++) {
                idx.maybeAddEntry(k * 10, k * 4096);
            }
            idx.flush();

            // Load via FFM and hold in memory
            SparseOffsetIndex loaded = SparseOffsetIndex.loadFromFile(idxFile);
            heldIndexes.add(loaded);
        }

        assertEquals(count, heldIndexes.size());

        // Verify each held index can be queried concurrently
        for (int i = 0; i < 50; i++) {
            assertEquals(4096, heldIndexes.get(i).lookupPosition(15));
        }
    }

    @Test
    void testWalCrashRecoveryWithTruncatedTailRecord() throws Exception {
        File walDir = new File(partitionDir, "wal-corrupt-tail-recovery");
        walDir.mkdirs();

        // 1. Write 50 clean messages
        try (WriteAheadLog wal = new WriteAheadLog(walDir, 50 * 1024 * 1024)) {
            for (int i = 0; i < 50; i++) {
                wal.append(("clean-record-" + i).getBytes(StandardCharsets.UTF_8));
            }
        }

        // 2. Locate the active log file
        File[] logFiles = walDir.listFiles(f -> f.getName().endsWith(".log"));
        assertNotNull(logFiles);
        assertEquals(1, logFiles.length);
        File logFile = logFiles[0];
        long cleanFileSize = logFile.length();
        assertTrue(cleanFileSize > 0);

        // 3. Simulate an ungraceful hard crash mid-syscall:
        // Append 2 bytes of an incomplete 4-byte length prefix + 10 junk bytes
        try (FileOutputStream fos = new FileOutputStream(logFile, true)) {
            // Write corrupted length header (says 500 bytes payload), but only write 12 bytes
            ByteBuffer corruptBuf = ByteBuffer.allocate(16);
            corruptBuf.putInt(500); // Claims 500 bytes follow
            corruptBuf.put("partial-junk".getBytes(StandardCharsets.UTF_8)); // only 12 bytes
            fos.write(corruptBuf.array());
        }

        assertTrue(logFile.length() > cleanFileSize, "Corrupt tail bytes must be appended");

        // 4. Reopen WAL. It must detect the truncated tail, truncate it back to cleanFileSize,
        // and resume normally.
        try (WriteAheadLog recoveredWal = new WriteAheadLog(walDir, 50 * 1024 * 1024)) {
            // Verify file was truncated back to clean size
            assertEquals(cleanFileSize, logFile.length(), "Log file should be truncated to last clean record");

            // Verify all 50 original clean messages are intact
            for (int i = 0; i < 50; i++) {
                byte[] read = recoveredWal.read(i);
                assertNotNull(read, "Record at offset " + i + " must be readable");
                assertEquals("clean-record-" + i, new String(read, StandardCharsets.UTF_8));
            }

            // Verify WAL can resume appending new records without error starting at offset 50
            long nextOffset = recoveredWal.append("resumed-record-50".getBytes(StandardCharsets.UTF_8));
            assertEquals(50, nextOffset);

            byte[] resumedRead = recoveredWal.read(50);
            assertEquals("resumed-record-50", new String(resumedRead, StandardCharsets.UTF_8));
        }
    }
}
