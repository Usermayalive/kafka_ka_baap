package com.simplekafka.metadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

public class MetadataQuorumTest {

    @TempDir
    File tempDir;

    private final List<MetadataQuorum> running = new ArrayList<>();

    @AfterEach
    void teardown() {
        for (MetadataQuorum q : running) {
            try {
                q.close();
            } catch (IOException ignored) {
            }
        }
    }

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static List<MetadataQuorum.Peer> peers(int n) throws IOException {
        List<MetadataQuorum.Peer> peers = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            peers.add(new MetadataQuorum.Peer(i, "127.0.0.1", freePort()));
        }
        return peers;
    }

    private MetadataQuorum start(MetadataQuorum.Peer self, List<MetadataQuorum.Peer> voters, boolean listener,
                                 long snapshotThreshold) throws IOException {
        MetadataQuorum q = new MetadataQuorum(self, voters, listener, new File(tempDir, "n" + self.id()), 3_000);
        q.setSnapshotThreshold(snapshotThreshold);
        q.start();
        running.add(q);
        return q;
    }

    private List<MetadataQuorum> startAll(List<MetadataQuorum.Peer> voters, long snapshotThreshold) throws IOException {
        List<MetadataQuorum> nodes = new ArrayList<>();
        for (MetadataQuorum.Peer p : voters) {
            nodes.add(start(p, voters, false, snapshotThreshold));
        }
        return nodes;
    }

    private static void await(String what, long timeoutMs, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("Timed out after " + timeoutMs + " ms waiting for: " + what);
            }
            Thread.sleep(20);
        }
    }

    private static MetadataQuorum leaderOf(List<MetadataQuorum> nodes) {
        return nodes.stream().filter(MetadataQuorum::isLeader).findFirst().orElse(null);
    }

    private static MetadataImage.PartitionState partition(int leader) {
        return new MetadataImage.PartitionState(leader, List.of(leader), List.of(leader), 0);
    }

    @Test
    void testThreeNodesElectLeaderAndReElectWhenLeaderDies() throws Exception {
        List<MetadataQuorum> nodes = startAll(peers(3), 1_000);
        await("initial leader", 10_000, () -> leaderOf(nodes) != null);
        MetadataQuorum leader = leaderOf(nodes);
        leader.submit(MetadataCommand.createTopic("orders", Map.of("partitions", "1"), List.of(partition(1))));

        long killedAt = System.nanoTime();
        leader.close();
        List<MetadataQuorum> survivors = new ArrayList<>(nodes);
        survivors.remove(leader);
        await("re-election", 5_000, () -> leaderOf(survivors) != null);
        long electionMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - killedAt);
        System.out.println("Re-election after leader death: " + electionMs + " ms");

        MetadataQuorum newLeader = leaderOf(survivors);
        assertNotEquals(leader.self().id(), newLeader.self().id());
        assertNotNull(newLeader.image().topicImage("orders"), "Committed state survives leader loss");
        newLeader.submit(MetadataCommand.createTopic("payments", Map.of("partitions", "1"), List.of(partition(2))));
        for (MetadataQuorum q : survivors) {
            await("payments replicated to n" + q.self().id(), 5_000, () -> q.image().topicImage("payments") != null);
        }
    }

    @Test
    void testStateSurvivesRestartOfEveryNodeViaSnapshotAndLog() throws Exception {
        List<MetadataQuorum.Peer> voters = peers(3);
        List<MetadataQuorum> nodes = startAll(voters, 10);
        await("leader", 10_000, () -> leaderOf(nodes) != null);
        MetadataQuorum leader = leaderOf(nodes);
        for (int b = 1; b <= 3; b++) {
            leader.submit(MetadataCommand.registerBroker(b, "127.0.0.1", 9000 + b, 1));
        }
        for (int t = 0; t < 25; t++) { // > snapshot threshold: restore needs snapshot + log tail
            leader.submit(MetadataCommand.createTopic("topic-" + t, Map.of("partitions", "1"), List.of(partition(1 + t % 3))));
        }
        leader.submit(MetadataCommand.electLeader("topic-0", 0, 2));
        for (MetadataQuorum q : nodes) {
            q.close();
        }
        running.clear();

        List<MetadataQuorum> restarted = startAll(voters, 10);
        await("leader after full restart", 10_000, () -> leaderOf(restarted) != null);
        for (MetadataQuorum q : restarted) {
            await("n" + q.self().id() + " restored", 5_000, () -> q.image().topics().size() == 25);
            assertEquals(3, q.image().brokers().size());
            assertEquals(2, q.image().topicImage("topic-0").partitions().get(0).leader());
            assertEquals(1, q.image().topicImage("topic-0").partitions().get(0).leaderEpoch());
        }
        File snapshots = new File(tempDir, "n1/" + MetadataQuorum.GROUP_ID.getUuid() + "/sm");
        assertTrue(snapshots.exists() && Objects.requireNonNull(snapshots.list()).length > 0,
                "Auto-snapshots must have been taken");
    }

    @Test
    void testSnapshotPreservesForgottenEntities() {
        MetadataImage image = new MetadataImage();
        image.apply(MetadataCommand.forgetEntity("abc123"));
        MetadataImage restored = new MetadataImage();
        restored.loadJson(image.toJson());
        assertEquals(1, restored.erasureGeneration("abc123"));
    }

    @Test
    void testLearnerCatchesUpAndEvenQuorumPromotionIsRefused() throws Exception {
        List<MetadataQuorum.Peer> voters = peers(3);
        List<MetadataQuorum> nodes = startAll(voters, 1_000);
        await("leader", 10_000, () -> leaderOf(nodes) != null);
        MetadataQuorum leader = leaderOf(nodes);
        leader.submit(MetadataCommand.createTopic("orders", Map.of("partitions", "1"), List.of(partition(1))));

        MetadataQuorum.Peer p4 = new MetadataQuorum.Peer(4, "127.0.0.1", freePort());
        MetadataQuorum n4 = start(p4, voters, true, 1_000);
        leader.addListener(p4);
        await("learner 4 catches up", 10_000, () -> n4.image().topicImage("orders") != null);
        assertEquals(Set.of(4), leader.listenerIds());

        IllegalArgumentException even = assertThrows(IllegalArgumentException.class, () -> leader.promote(List.of(4)));
        assertTrue(even.getMessage().contains("4 voters"));
        assertEquals(3, leader.voterIds().size(), "Refused promotion must not change the voter set");

        MetadataQuorum.Peer p5 = new MetadataQuorum.Peer(5, "127.0.0.1", freePort());
        MetadataQuorum n5 = start(p5, voters, true, 1_000);
        leader.addListener(p5);
        await("learner 5 catches up", 10_000, () -> n5.image().topicImage("orders") != null);
        leader.promote(List.of(4, 5));
        assertEquals(Set.of(1, 2, 3, 4, 5), leader.voterIds());

        // A promoted node takes part in elections: hand it leadership
        leader.transferLeadership(4, 5_000);
        await("n4 becomes leader", 5_000, n4::isLeader);
        n4.submit(MetadataCommand.createTopic("after-promotion", Map.of("partitions", "1"), List.of(partition(1))));
        await("n5 replicates", 5_000, () -> n5.image().topicImage("after-promotion") != null);
    }
}
