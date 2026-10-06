package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.coordinator.ConsumerOffsetStore;
import com.simplekafka.coordinator.GroupCoordinator.TopicPartition;
import com.simplekafka.network.NettyBrokerServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

public class GroupConsumerTest {
    private static final int SESSION_MS = 1_000;
    private static final long HEARTBEAT_MS = 50;

    @TempDir
    File tempDir;

    private NettyBrokerServer server;
    private SimpleKafkaClient client;
    private SimpleKafkaBroker broker;
    private final List<GroupConsumer> members = new ArrayList<>();
    private final Map<String, Integer> deliveries = new ConcurrentHashMap<>();

    @BeforeEach
    void setup() throws Exception {
        int[] port = new int[1];
        broker = new SimpleKafkaBroker(1, "127.0.0.1", 0, 2181) {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public List<BrokerInfo> liveBrokers() {
                return List.of(new BrokerInfo(1, "127.0.0.1", port[0]));
            }
            @Override protected String dataDir() {
                return tempDir.getAbsolutePath();
            }
        };
        List<Partition> orders = new ArrayList<>();
        List<Partition> offsets = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            orders.add(new Partition(i, new File(tempDir, "orders").getAbsolutePath()));
            offsets.add(new Partition(i, new File(tempDir, ConsumerOffsetStore.TOPIC).getAbsolutePath()));
        }
        broker.getTopics().put("orders", orders);
        broker.getTopics().put(ConsumerOffsetStore.TOPIC, offsets);
        server = new NettyBrokerServer("127.0.0.1", 0, broker);
        server.start();
        port[0] = server.getPort();
        client = new SimpleKafkaClient("127.0.0.1", server.getPort());
        client.initialize();
    }

    @AfterEach
    void teardown() {
        members.forEach(GroupConsumer::close);
        client.close();
        server.stop();
        broker.getGroupCoordinator().close();
    }

    private GroupConsumer member(String instanceId) throws Exception {
        GroupConsumer m = new GroupConsumer(client, "billing", instanceId, Set.of("orders"),
                (msg, offset) -> deliveries.merge(new String(msg, StandardCharsets.UTF_8), 1, Integer::sum),
                SESSION_MS, HEARTBEAT_MS);
        m.start();
        members.add(m);
        return m;
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("Timed out waiting for: " + what);
            }
            Thread.sleep(20);
        }
    }

    /** Every partition owned by exactly one of the given members. */
    private static boolean covers(int partitions, GroupConsumer... group) {
        Set<TopicPartition> union = new HashSet<>();
        int total = 0;
        for (GroupConsumer m : group) {
            union.addAll(m.assignment());
            total += m.assignment().size();
        }
        return union.size() == partitions && total == partitions;
    }

    private static List<Integer> sizes(GroupConsumer... group) {
        return Arrays.stream(group).map(m -> m.assignment().size()).sorted().toList();
    }

    @Test
    void testTwoMembersSplitPartitionsAndConsumeEachRecordOnce() throws Exception {
        GroupConsumer a = member("a");
        GroupConsumer b = member("b");
        await("a and b split 4 partitions", () -> covers(4, a, b) && sizes(a, b).equals(List.of(2, 2)));

        for (int i = 0; i < 40; i++) {
            client.send("orders", i % 4, ("r" + i).getBytes(StandardCharsets.UTF_8));
        }
        await("all 40 records delivered", () -> deliveries.size() == 40);
        Thread.sleep(300);
        assertTrue(deliveries.values().stream().allMatch(c -> c == 1), "Each record handled exactly once");
    }

    @Test
    void testThirdMemberTriggersStickyCooperativeRebalance() throws Exception {
        GroupConsumer a = member("a");
        GroupConsumer b = member("b");
        await("a and b balanced", () -> covers(4, a, b) && sizes(a, b).equals(List.of(2, 2)));
        Set<TopicPartition> aBefore = a.assignment();
        Set<TopicPartition> bBefore = b.assignment();

        GroupConsumer c = member("c");
        await("three-way balance", () -> covers(4, a, b, c) && sizes(a, b, c).equals(List.of(1, 1, 2)));
        assertTrue(aBefore.containsAll(a.assignment()), "Sticky: a only gives partitions up, never swaps");
        assertTrue(bBefore.containsAll(b.assignment()), "Sticky: b only gives partitions up, never swaps");
    }

    @Test
    void testCrashedMemberPartitionsAreReassignedAfterSessionTimeout() throws Exception {
        GroupConsumer a = member("a");
        GroupConsumer b = member("b");
        await("a and b balanced", () -> covers(4, a, b) && sizes(a, b).equals(List.of(2, 2)));

        b.abandonForTest(); // no leave, no more heartbeats
        long crashedAt = System.nanoTime();
        await("a takes over everything", () -> a.assignment().size() == 4);
        long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - crashedAt);
        assertTrue(tookMs >= SESSION_MS - 100, "Reassignment waits for the session timeout (took " + tookMs + " ms)");
    }

    @Test
    void testStaticMemberRestartKeepsAssignmentWithoutRebalance() throws Exception {
        GroupConsumer a = member("svc-a");
        GroupConsumer b = member("svc-b");
        await("balanced", () -> covers(4, a, b) && sizes(a, b).equals(List.of(2, 2)));
        Set<TopicPartition> bAssignment = b.assignment();
        int generation = broker.getGroupCoordinator().generation("billing");

        b.close(); // static: stays in the group
        GroupConsumer bRestarted = member("svc-b");
        await("restarted b owns its old partitions", () -> bRestarted.assignment().equals(bAssignment));
        assertEquals(generation, broker.getGroupCoordinator().generation("billing"), "No rebalance on static rejoin");
        assertEquals(2, a.assignment().size());
    }

    @Test
    void testLeaveGroupReassignsImmediately() throws Exception {
        GroupConsumer a = member("a");
        GroupConsumer b = member("b");
        await("balanced", () -> covers(4, a, b) && sizes(a, b).equals(List.of(2, 2)));
        long leftAt = System.nanoTime();
        b.leaveGroup();
        await("a owns all", () -> a.assignment().size() == 4);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leftAt) < SESSION_MS,
                "Explicit leave must not wait for the session timeout");
    }
}
