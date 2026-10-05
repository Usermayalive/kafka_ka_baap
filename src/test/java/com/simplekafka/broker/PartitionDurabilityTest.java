package com.simplekafka.broker;

import com.simplekafka.storage.WriteAheadLog.DurabilityMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionDurabilityTest {

    @TempDir
    File tempDir;

    @Test
    void testGroupCommitAssignsUniqueOffsetsUnderConcurrency() throws Exception {
        Partition part = new Partition(0, tempDir.getAbsolutePath(), DurabilityMode.PHYSICAL_FSYNC);
        int threads = 8;
        int perThread = 50;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<List<Long>>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int thread = t;
            futures.add(pool.submit(() -> {
                List<Long> offsets = new ArrayList<>();
                for (int i = 0; i < perThread; i++) {
                    offsets.add(part.append(("t" + thread + "-" + i).getBytes(StandardCharsets.UTF_8)));
                }
                return offsets;
            }));
        }

        Set<Long> allOffsets = new HashSet<>();
        for (Future<List<Long>> f : futures) {
            allOffsets.addAll(f.get(60, TimeUnit.SECONDS));
        }
        pool.shutdown();

        assertEquals(threads * perThread, allOffsets.size(), "Every append must get a distinct offset");
        assertEquals(threads * perThread, part.readMessages(0, Integer.MAX_VALUE).size());
    }

    @Test
    void testGroupCommitSurvivesSegmentRoll() throws Exception {
        String base = tempDir.getAbsolutePath();
        Partition part = new Partition(0, base, DurabilityMode.PHYSICAL_FSYNC);
        byte[] payload = new byte[100 * 1024];
        // 15 x ~100KB crosses the 1MB segment boundary
        for (int i = 0; i < 15; i++) {
            assertEquals(i, part.append(payload));
        }

        Partition reopened = new Partition(0, base, DurabilityMode.PHYSICAL_FSYNC);
        assertEquals(15, reopened.append(payload), "Next offset must continue after the rolled segment");
    }

    @Test
    void testRecoveryTruncatesOrphanedLogBytes() throws Exception {
        String base = tempDir.getAbsolutePath();
        Partition part = new Partition(0, base);
        for (int i = 0; i < 5; i++) {
            part.append(("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Simulate a crash after the log write but before the index write:
        // a complete-looking record exists in the log with no index entry.
        File log = new File(part.getBaseDir(), String.format("%020d.log", 0L));
        try (RandomAccessFile raf = new RandomAccessFile(log, "rw")) {
            raf.seek(raf.length());
            raf.writeInt(3);
            raf.write("bad".getBytes(StandardCharsets.UTF_8));
        }

        Partition recovered = new Partition(0, base);
        List<byte[]> msgs = recovered.readMessages(0, Integer.MAX_VALUE);
        assertEquals(5, msgs.size(), "Orphaned record without an index entry must be truncated");

        assertEquals(5, recovered.append("msg-5".getBytes(StandardCharsets.UTF_8)));
        List<byte[]> after = recovered.readMessages(0, Integer.MAX_VALUE);
        assertEquals(6, after.size());
        assertEquals("msg-5", new String(after.get(5), StandardCharsets.UTF_8));
    }

    @Test
    void testRecoveryDropsIndexEntriesPointingPastLogEnd() throws Exception {
        String base = tempDir.getAbsolutePath();
        Partition part = new Partition(0, base);
        for (int i = 0; i < 5; i++) {
            part.append(("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Simulate the index tail reaching disk but the log tail not: cut the last record short.
        File log = new File(part.getBaseDir(), String.format("%020d.log", 0L));
        try (RandomAccessFile raf = new RandomAccessFile(log, "rw")) {
            raf.setLength(raf.length() - 2);
        }

        Partition recovered = new Partition(0, base);
        List<byte[]> msgs = recovered.readMessages(0, Integer.MAX_VALUE);
        assertEquals(4, msgs.size());
        assertEquals("msg-3", new String(msgs.get(3), StandardCharsets.UTF_8));
        assertEquals(4, recovered.append("msg-4b".getBytes(StandardCharsets.UTF_8)),
                "The torn record's offset is reassigned to the next append");
    }

    @Test
    void testNextOffsetAfterCompactionAndRestartSkipsGaps() throws Exception {
        String base = tempDir.getAbsolutePath();
        Partition part = new Partition(0, base);
        // keys a,b,a,b,a,c -> survivors: b@3, a@4, c@5
        String[] keys = {"a", "b", "a", "b", "a", "c"};
        for (String k : keys) {
            part.append((k + ":v").getBytes(StandardCharsets.UTF_8));
        }
        part.compact(msg -> new String(msg, StandardCharsets.UTF_8).split(":")[0]);

        Partition recovered = new Partition(0, base);
        assertEquals(6, recovered.append("d:v".getBytes(StandardCharsets.UTF_8)),
                "Restart must resume after the highest surviving offset, not after the record count");
    }
}
