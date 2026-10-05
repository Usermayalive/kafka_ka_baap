package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.network.KafkaFrame;
import com.simplekafka.network.NettyBrokerServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

/** End-to-end over real Netty: SimpleKafkaClient/SimpleKafkaConsumer against NettyBrokerServer. */
public class ClientProtocolTest {

    @TempDir
    File tempDir;

    private NettyBrokerServer server;
    private SimpleKafkaClient client;
    private Partition partition;

    @BeforeEach
    void setup() throws Exception {
        partition = new Partition(0, tempDir.getAbsolutePath());
        int[] port = new int[1];
        SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "127.0.0.1", 0, 2181) {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public List<BrokerInfo> liveBrokers() {
                return List.of(new BrokerInfo(1, "127.0.0.1", port[0]));
            }
        };
        broker.getTopics().put("events", new ArrayList<>(List.of(partition)));
        server = new NettyBrokerServer("127.0.0.1", 0, broker);
        server.start();
        port[0] = server.getPort();

        client = new SimpleKafkaClient("127.0.0.1", server.getPort());
        client.initialize();
    }

    @AfterEach
    void teardown() {
        client.close();
        server.stop();
    }

    @Test
    void testMetadataDescribesTopicsAndBrokers() throws Exception {
        assertEquals(1, client.getNumPartitions("events"));
        assertEquals(server.getPort(), client.getBrokers().get(1).getPort());
    }

    @Test
    void testProduceAndFetchRoundTrip() throws Exception {
        for (int i = 0; i < 5; i++) {
            assertEquals(i, client.send("events", 0, ("m" + i).getBytes(StandardCharsets.UTF_8)));
        }
        List<SimpleKafkaClient.FetchedRecord> records = client.fetchRecords("events", 0, 2, 1024 * 1024);
        assertEquals(List.of(2L, 3L, 4L), records.stream().map(SimpleKafkaClient.FetchedRecord::offset).toList());
        assertEquals("m2", new String(records.get(0).payload(), StandardCharsets.UTF_8));
    }

    @Test
    void testFetchReportsCompactionGapsAndConsumerSkipsThem() throws Exception {
        for (String r : new String[]{"a:1", "b:1", "a:2", "c:1", "b:2"}) {
            client.send("events", 0, r.getBytes(StandardCharsets.UTF_8));
        }
        partition.compact(msg -> new String(msg, StandardCharsets.UTF_8).split(":")[0]);

        // Survivors: a:2@2, c:1@3, b:2@4
        assertEquals(List.of(2L, 3L, 4L), client.fetchRecords("events", 0, 0, 1024 * 1024).stream()
                .map(SimpleKafkaClient.FetchedRecord::offset).toList());

        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(client, "events", 0);
        List<Long> seen = Collections.synchronizedList(new ArrayList<>());
        consumer.startConsuming((msg, offset) -> seen.add(offset));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (seen.size() < 3 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        consumer.stopConsuming();

        assertEquals(List.of(2L, 3L, 4L), seen, "Consumer must report real offsets, not a contiguous count");
        assertEquals(5, consumer.getCommittedOffset());
    }

    @Test
    void testFetchSpansSegments() throws Exception {
        byte[] big = new byte[200 * 1024];
        for (int i = 0; i < 12; i++) { // ~2.4MB across 1MB segments
            client.send("events", 0, big);
        }
        List<SimpleKafkaClient.FetchedRecord> records = client.fetchRecords("events", 0, 0, 16 * 1024 * 1024);
        assertEquals(12, records.size(), "A single fetch must continue into the next segment");
        assertEquals(11, records.get(11).offset());
    }

    @Test
    void testCreateTopicRejectedByNonController() {
        IOException e = assertThrows(IOException.class, () -> client.createTopic("new-topic", 1, (short) 1));
        assertInstanceOf(BrokerErrorException.class, e.getCause());
        assertEquals(KafkaFrame.ERROR_NOT_CONTROLLER, ((BrokerErrorException) e.getCause()).getErrorCode());
    }

}
