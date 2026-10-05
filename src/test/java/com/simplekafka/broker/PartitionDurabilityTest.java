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

    private static final CompactionKeyExtractor TOMBSTONE_A_EXTRACTOR = msg -> {
        if (msg.length == 0) return "A"; // tombstone targeting key A
        return new String(msg, StandardCharsets.UTF_8).split(":")[0];
    };

    @Test
    void testTombstoneCompactionNeverReissuesOffsets() throws Exception {
        String base = tempDir.getAbsolutePath();
        Partition part = new Partition(0, base);
        part.append("B:v".getBytes(StandardCharsets.UTF_8)); // 0
        part.append("A:v".getBytes(StandardCharsets.UTF_8)); // 1
        part.append(new byte[0]);                             // 2: tombstone for A, highest offset issued
        part.compact(TOMBSTONE_A_EXTRACTOR);                  // survivors: B@0 only

        assertEquals(3, part.append("C:v".getBytes(StandardCharsets.UTF_8)),
                "Offsets dropped by compaction must not be reissued");
    }

    @Test
    void testTombstoneCompactionNeverReissuesOffsetsAfterRestart() throws Exception {
        String base = tempDir.getAbsolutePath();
        Partition part = new Partition(0, base);
        part.append("B:v".getBytes(StandardCharsets.UTF_8));
        part.append("A:v".getBytes(StandardCharsets.UTF_8));
        part.append(new byte[0]);
        part.compact(TOMBSTONE_A_EXTRACTOR);

        // Last surviving index entry is offset 0, but offsets 1-2 were already issued
        Partition recovered = new Partition(0, base);
        assertEquals(3, recovered.append("C:v".getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Crash after the commit point, after the original files were deleted but before the
     * compacted files were moved in. Previously recovery deleted the staging directory here,
     * losing every record.
     */
    @Test
    void testCompactionCrashAfterCommitPointRollsForward() throws Exception {
        String[] records = {"a:1", "b:1", "a:2", "c:1"};
        CompactionKeyExtractor byKey = msg -> new String(msg, StandardCharsets.UTF_8).split(":")[0];

        // Produce the compacted output in a twin partition
        Partition twin = new Partition(0, new File(tempDir, "twin").getAbsolutePath());
        for (String r : records) twin.append(r.getBytes(StandardCharsets.UTF_8));
        twin.compact(byKey);

        String base = new File(tempDir, "crashed").getAbsolutePath();
        Partition part = new Partition(0, base);
        for (String r : records) part.append(r.getBytes(StandardCharsets.UTF_8));

        File partDir = new File(part.getBaseDir());
        File ready = new File(partDir, ".compact_ready");
        ready.mkdirs();
        for (String name : new String[]{"00000000000000000000.log", "00000000000000000000.index"}) {
            java.nio.file.Files.copy(new File(twin.getBaseDir(), name).toPath(), new File(ready, name).toPath());
            new File(partDir, name).delete();
        }
        new File(partDir, ".compact.lock").createNewFile();

        Partition recovered = new Partition(0, base);
        List<String> msgs = new ArrayList<>();
        for (byte[] m : recovered.readMessages(0, Integer.MAX_VALUE)) msgs.add(new String(m, StandardCharsets.UTF_8));
        assertEquals(List.of("b:1", "a:2", "c:1"), msgs, "Committed compaction must be completed, not discarded");
        assertFalse(ready.exists());
        assertFalse(new File(partDir, ".compact.lock").exists());
        assertEquals(4, recovered.append("d:1".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void testCompactedLogStaysInOffsetOrder() throws Exception {
        Partition part = new Partition(0, tempDir.getAbsolutePath());
        for (String r : new String[]{"a:1", "b:1", "a:2", "c:1"}) {
            part.append(r.getBytes(StandardCharsets.UTF_8));
        }
        part.compact(msg -> new String(msg, StandardCharsets.UTF_8).split(":")[0]);

        // Survivors b@1, a@2, c@3: a read starting at each offset must begin with that record
        assertEquals("b:1", new String(part.readMessages(1, 1).get(0), StandardCharsets.UTF_8));
        assertEquals("a:2", new String(part.readMessages(2, 1).get(0), StandardCharsets.UTF_8));
        assertEquals("c:1", new String(part.readMessages(3, 1).get(0), StandardCharsets.UTF_8));
    }

    @Test
    void testCompactionCrashBeforeCommitPointRollsBack() throws Exception {
        String base = tempDir.getAbsolutePath();
        Partition part = new Partition(0, base);
        for (int i = 0; i < 4; i++) part.append(("k" + i + ":v").getBytes(StandardCharsets.UTF_8));

        File partDir = new File(part.getBaseDir());
        File staging = new File(partDir, ".compact_staging");
        staging.mkdirs();
        new File(staging, "00000000000000000000.log").createNewFile(); // partial, uncommitted output
        new File(partDir, ".compact.lock").createNewFile();

        Partition recovered = new Partition(0, base);
        assertEquals(4, recovered.readMessages(0, Integer.MAX_VALUE).size(), "Original records must be untouched");
        assertFalse(staging.exists());
    }
}
