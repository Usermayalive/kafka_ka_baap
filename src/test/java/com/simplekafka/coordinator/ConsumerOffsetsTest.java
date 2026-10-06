package com.simplekafka.coordinator;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.BrokerErrorException;
import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.client.SimpleKafkaConsumer;
import com.simplekafka.network.KafkaFrame;
import com.simplekafka.network.NettyBrokerServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ConsumerOffsetsTest {

    @TempDir
    File tempDir;

    private NettyBrokerServer server;
    private SimpleKafkaClient client;

    /** A standalone broker whose partitions (including __consumer_offsets) live under tempDir. */
    private void startBroker(boolean hostOffsetsTopic) throws Exception {
        int[] port = new int[1];
        SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "127.0.0.1", 0, 2181) {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public List<BrokerInfo> liveBrokers() {
                return List.of(new BrokerInfo(1, "127.0.0.1", port[0]));
            }
        };
        broker.getTopics().put("events", new ArrayList<>(List.of(new Partition(0, new File(tempDir, "events").getAbsolutePath()))));
        if (hostOffsetsTopic) {
            List<Partition> offsets = new ArrayList<>();
            for (int i = 0; i < ConsumerOffsetStore.PARTITIONS; i++) {
                offsets.add(new Partition(i, new File(tempDir, ConsumerOffsetStore.TOPIC).getAbsolutePath()));
            }
            broker.getTopics().put(ConsumerOffsetStore.TOPIC, offsets);
        }
        server = new NettyBrokerServer("127.0.0.1", 0, broker);
        server.start();
        port[0] = server.getPort();
        client = new SimpleKafkaClient("127.0.0.1", server.getPort());
        client.initialize();
    }

    private void restartBroker() throws Exception {
        client.close();
        server.stop();
        startBroker(true);
    }

    @AfterEach
    void teardown() {
        if (client != null) client.close();
        if (server != null) server.stop();
    }

    private List<Long> consume(String group, int expected) throws Exception {
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(client, "events", 0, group);
        List<Long> seen = Collections.synchronizedList(new ArrayList<>());
        consumer.startConsuming((msg, offset) -> seen.add(offset));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (seen.size() < expected && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        Thread.sleep(200); // let the batch's commit land
        consumer.stopConsuming();
        return new ArrayList<>(seen);
    }

    @Test
    void testConsumerResumesFromCommittedOffsetAfterBrokerRestart() throws Exception {
        startBroker(true);
        for (int i = 0; i < 5; i++) {
            client.send("events", 0, ("m" + i).getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L), consume("g1", 5));
        assertEquals(5, client.fetchCommittedOffset("g1", "events", 0));

        restartBroker();
        for (int i = 5; i < 8; i++) {
            client.send("events", 0, ("m" + i).getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(5, client.fetchCommittedOffset("g1", "events", 0), "Committed offset must survive restart");
        assertEquals(List.of(5L, 6L, 7L), consume("g1", 3), "Consumer must resume after its last commit");
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L), consume("other-group", 8),
                "Groups track positions independently");
    }

    @Test
    void testUncommittedGroupStartsFromBeginning() throws Exception {
        startBroker(true);
        assertEquals(-1, client.fetchCommittedOffset("fresh", "events", 0));
    }

    @Test
    void testLatestCommitSurvivesCompactionAndRestart() throws Exception {
        startBroker(true);
        for (int i = 1; i <= 50; i++) {
            client.commitOffset("g2", "events", 0, i, "");
            client.commitOffset("g3", "events", 0, i * 10, "");
        }
        // Broker down: compact its __consumer_offsets replica, then start it again
        client.close();
        server.stop();
        Partition replica = new Partition(ConsumerOffsetStore.partitionFor("g2"),
                new File(tempDir, ConsumerOffsetStore.TOPIC).getAbsolutePath());
        int before = replica.readRecords(0, Integer.MAX_VALUE).size();
        replica.compact(ConsumerOffsetStore::compactionKey);
        assertTrue(replica.readRecords(0, Integer.MAX_VALUE).size() < before);
        startBroker(true);
        assertEquals(50, client.fetchCommittedOffset("g2", "events", 0));
        assertEquals(500, client.fetchCommittedOffset("g3", "events", 0));
    }

    @Test
    void testNonCoordinatorRejectsOffsetRequests() throws Exception {
        startBroker(false);
        IOException e = assertThrows(IOException.class, () -> client.commitOffset("g1", "events", 0, 1, ""));
        // Without __consumer_offsets this broker can't name a coordinator (it isn't the controller)
        assertInstanceOf(BrokerErrorException.class, e);
        assertEquals(KafkaFrame.ERROR_COORDINATOR_NOT_AVAILABLE, ((BrokerErrorException) e).getErrorCode());
    }
}
