package com.simplekafka.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ReplicationPrimitivesTest {

    @TempDir
    File tempDir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static List<Long> offsets(List<Partition.Record> records) {
        return records.stream().map(Partition.Record::offset).toList();
    }

    @Test
    void testAppendReplicatedKeepsLeaderOffsetsIncludingGaps() throws Exception {
        Partition follower = new Partition(0, tempDir.getAbsolutePath());
        follower.appendReplicated(0, bytes("a"));
        follower.appendReplicated(3, bytes("d")); // leader compacted 1-2 away
        follower.appendReplicated(4, bytes("e"));
        assertEquals(List.of(0L, 3L, 4L), offsets(follower.readRecords(0, 1 << 20)));
        assertEquals(5, follower.getNextOffset());
        assertThrows(IOException.class, () -> follower.appendReplicated(2, bytes("x")), "Offsets never go backwards");
    }

    @Test
    void testTruncateWithinSegmentThenAppend() throws Exception {
        Partition p = new Partition(0, tempDir.getAbsolutePath());
        for (int i = 0; i < 10; i++) {
            p.append(bytes("m" + i));
        }
        p.truncateTo(6);
        assertEquals(6, p.getNextOffset());
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L), offsets(p.readRecords(0, 1 << 20)));
        assertEquals(6, p.appendReplicated(6, bytes("new-6")));
        assertEquals("new-6", new String(p.readRecords(6, 1 << 20).get(0).payload(), StandardCharsets.UTF_8));

        Partition reopened = new Partition(0, tempDir.getAbsolutePath());
        assertEquals(7, reopened.getNextOffset(), "Truncation survives restart");
    }

    @Test
    void testTruncateAcrossSegmentBoundary() throws Exception {
        Partition p = new Partition(0, tempDir.getAbsolutePath());
        byte[] big = new byte[200 * 1024]; // ~5 per 1MB segment
        for (int i = 0; i < 14; i++) {
            ByteBuffer.wrap(big).putInt(i);
            p.append(big);
        }
        p.truncateTo(3); // drops two whole segments and part of the first
        assertEquals(3, p.getNextOffset());
        assertEquals(List.of(0L, 1L, 2L), offsets(p.readRecords(0, 16 << 20)));
        for (int i = 3; i < 12; i++) {
            ByteBuffer.wrap(big).putInt(100 + i);
            assertEquals(i, p.appendReplicated(i, big));
        }
        assertEquals(103, ByteBuffer.wrap(p.readRecords(3, 1).get(0).payload()).getInt());
        assertEquals(12, new Partition(0, tempDir.getAbsolutePath()).getNextOffset());
    }

    @Test
    void testHighWatermarkBoundsReadsAndDefaultsToLogEnd() throws Exception {
        Partition p = new Partition(0, tempDir.getAbsolutePath());
        for (int i = 0; i < 5; i++) {
            p.append(bytes("m" + i));
        }
        assertFalse(p.isReplicated());
        assertEquals(5, p.getHighWatermark(), "Unreplicated: HW is the log end");
        p.setHighWatermark(3);
        assertEquals(List.of(0L, 1L, 2L), offsets(p.readRecords(0, 1 << 20, p.getHighWatermark())));
        assertTrue(p.awaitHighWatermark(2, 10));
        assertFalse(p.awaitHighWatermark(3, 50), "Offset 3 is not committed yet");
        p.truncateTo(2);
        assertEquals(2, p.getHighWatermark(), "HW never exceeds the log end");
    }

    @Test
    void testProducerStateFollowsTheLogItDescribes() throws Exception {
        com.simplekafka.network.IdempotenceManager idempotence = new com.simplekafka.network.IdempotenceManager();
        com.simplekafka.network.ProducerState state = new com.simplekafka.network.ProducerState(7L, (short) 0, 9, 14, 0);
        idempotence.applyReplicated("t", 0, List.of(state), 10);
        assertNull(idempotence.getProducerState("t", 0, 7L), "Not adopted until the replica holds offset 14");
        idempotence.applyReplicated("t", 0, List.of(state), 15);
        assertEquals(9, idempotence.getProducerState("t", 0, 7L).lastSequenceNumber());
        idempotence.truncate("t", 0, 12);
        assertNull(idempotence.getProducerState("t", 0, 7L), "Truncating offset 14 forgets the state that claimed it");
    }

    @Test
    void testLeaderEpochCacheFindsDivergencePoint() throws Exception {
        Partition p = new Partition(0, tempDir.getAbsolutePath());
        LeaderEpochCache cache = p.leaderEpochs();
        cache.assign(1, 0);
        cache.assign(2, 50);
        cache.assign(4, 80);
        cache.assign(3, 90); // stale epoch: ignored
        assertEquals(4, cache.latestEpoch());
        assertEquals(50, cache.endOffsetFor(1, 120), "Epoch 1 ended where epoch 2 began");
        assertEquals(80, cache.endOffsetFor(3, 120), "Unknown epoch 3 ends where the next known epoch begins");
        assertEquals(120, cache.endOffsetFor(4, 120), "Latest epoch extends to the log end");
        assertEquals(-1, cache.endOffsetFor(0, 120), "Older than our history: no answer");

        cache.truncateFromEnd(60);
        assertEquals(List.of(new LeaderEpochCache.Entry(1, 0), new LeaderEpochCache.Entry(2, 50)), cache.entries());
        assertEquals(cache.entries(), new Partition(0, tempDir.getAbsolutePath()).leaderEpochs().entries(),
                "Epoch history is persisted");
    }
}
