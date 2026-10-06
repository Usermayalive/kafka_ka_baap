package com.simplekafka.tiered;

import com.simplekafka.broker.Partition;
import com.simplekafka.storage.WriteAheadLog.DurabilityMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TieredStorageTest {

    @TempDir
    File tempDir;

    private static byte[] record(int i) {
        byte[] payload = new byte[100 * 1024]; // ~10 records per 1MB segment
        ByteBuffer.wrap(payload).putInt(i);
        return payload;
    }

    private static int idOf(byte[] payload) {
        return ByteBuffer.wrap(payload).getInt();
    }

    private Partition tieredPartition(LocalDirectoryRemoteStorageManager remote, int retained) {
        return new Partition(0, new File(tempDir, "orders").getAbsolutePath(), DurabilityMode.OS_PAGE_CACHE,
                new TieredStorageConfig(remote, retained, 2));
    }

    @Test
    void testRolledSegmentsAreUploadedAndOldLocalCopiesDeleted() throws Exception {
        LocalDirectoryRemoteStorageManager remote = new LocalDirectoryRemoteStorageManager(tempDir.toPath().resolve("remote"));
        Partition partition = tieredPartition(remote, 1);
        for (int i = 0; i < 45; i++) { // ~5 segments: 4 sealed + 1 active
            partition.append(record(i));
        }
        partition.awaitTieringIdle().get(10, TimeUnit.SECONDS);

        List<Long> remoteOnly = partition.remoteOnlySegments();
        assertEquals(3, remoteOnly.size(), "4 sealed segments, newest 1 kept locally");
        for (long base : remoteOnly) {
            assertTrue(remote.exists(new RemoteSegmentId("orders", 0, base)), "Segment " + base + " must be in remote storage");
            assertFalse(new File(partition.getBaseDir(), String.format("%020d.log", base)).exists(),
                    "Local copy of uploaded segment " + base + " must be deleted");
        }
    }

    @Test
    void testReadsSpanRemoteAndLocalSegmentsTransparently() throws Exception {
        LocalDirectoryRemoteStorageManager remote = new LocalDirectoryRemoteStorageManager(tempDir.toPath().resolve("remote"));
        Partition partition = tieredPartition(remote, 0);
        for (int i = 0; i < 45; i++) {
            partition.append(record(i));
        }
        partition.awaitTieringIdle().get(10, TimeUnit.SECONDS);
        assertFalse(partition.remoteOnlySegments().isEmpty());

        // Read everything back, one fetch at a time like a consumer
        long next = 0;
        int expected = 0;
        while (next < partition.getNextOffset()) {
            List<Partition.Record> batch = partition.readRecords(next, 1024 * 1024);
            assertFalse(batch.isEmpty(), "Fetch at " + next + " must make progress");
            for (Partition.Record r : batch) {
                assertEquals(expected, r.offset());
                assertEquals(expected, idOf(r.payload()));
                expected++;
            }
            next = batch.get(batch.size() - 1).offset() + 1;
        }
        assertEquals(45, expected);
    }

    @Test
    void testRemoteSegmentsSurviveRestart() throws Exception {
        LocalDirectoryRemoteStorageManager remote = new LocalDirectoryRemoteStorageManager(tempDir.toPath().resolve("remote"));
        Partition partition = tieredPartition(remote, 0);
        for (int i = 0; i < 25; i++) {
            partition.append(record(i));
        }
        partition.awaitTieringIdle().get(10, TimeUnit.SECONDS);
        int remoteCount = partition.remoteOnlySegments().size();
        assertTrue(remoteCount > 0);

        Partition restarted = tieredPartition(remote, 0);
        assertEquals(remoteCount, restarted.remoteOnlySegments().size());
        assertEquals(25, restarted.getNextOffset());
        assertEquals(3, idOf(restarted.readRecords(3, 1).get(0).payload()), "Cold read after restart");
        assertEquals(25, restarted.append(record(25)));
    }

    @Test
    void testSealedSegmentNotUploadedBeforeCrashIsUploadedOnRestart() throws Exception {
        Path remoteRoot = tempDir.toPath().resolve("remote");
        // First run with an always-failing remote: segments seal but never upload
        RemoteStorageManager broken = new RemoteStorageManager() {
            public java.util.concurrent.CompletableFuture<Void> copySegment(RemoteSegmentId id, Path log, Path idx) {
                return java.util.concurrent.CompletableFuture.failedFuture(new IOException("S3 down"));
            }
            public java.util.concurrent.CompletableFuture<Void> fetchSegment(RemoteSegmentId id, Path log, Path idx) {
                return java.util.concurrent.CompletableFuture.failedFuture(new IOException("S3 down"));
            }
            public java.util.concurrent.CompletableFuture<Void> deleteSegment(RemoteSegmentId id) {
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }
        };
        Partition partition = new Partition(0, new File(tempDir, "orders").getAbsolutePath(),
                DurabilityMode.OS_PAGE_CACHE, new TieredStorageConfig(broken, 0, 2));
        for (int i = 0; i < 25; i++) {
            partition.append(record(i));
        }
        partition.awaitTieringIdle().get(10, TimeUnit.SECONDS);
        assertTrue(partition.remoteOnlySegments().isEmpty(), "Failed uploads must keep data local");

        LocalDirectoryRemoteStorageManager remote = new LocalDirectoryRemoteStorageManager(remoteRoot);
        Partition restarted = tieredPartition(remote, 0);
        restarted.awaitTieringIdle().get(10, TimeUnit.SECONDS);
        assertEquals(2, restarted.remoteOnlySegments().size(), "Both sealed segments uploaded on restart");
        assertEquals(0, idOf(restarted.readRecords(0, 1).get(0).payload()));
    }

    @Test
    void testCompactionRejectedOnTieredPartition() {
        Partition partition = tieredPartition(new LocalDirectoryRemoteStorageManager(tempDir.toPath().resolve("r")), 1);
        assertThrows(IOException.class, () -> partition.compact(msg -> "k"));
    }

    @Test
    void testCodecRoundTripCompresses() throws IOException {
        byte[] raw = new byte[256 * 1024]; // zeros compress well
        byte[] compressed = SegmentCodec.compress(raw);
        assertTrue(compressed.length < raw.length / 10);
        assertArrayEquals(raw, SegmentCodec.decompress(compressed));
    }
}
