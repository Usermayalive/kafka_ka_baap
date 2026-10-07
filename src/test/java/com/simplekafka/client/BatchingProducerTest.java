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

import java.io.DataInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class BatchingProducerTest {

    @TempDir
    File tempDir;

    private SimpleKafkaBroker broker;
    private NettyBrokerServer server;
    private SimpleKafkaClient client;
    private Partition partition;

    @BeforeEach
    void setup() throws Exception {
        int[] port = new int[1];
        broker = new SimpleKafkaBroker(1, "127.0.0.1", 0, 0) {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public List<BrokerInfo> liveBrokers() {
                return List.of(new BrokerInfo(1, "127.0.0.1", port[0]));
            }
            @Override protected String dataDir() {
                return tempDir.getAbsolutePath();
            }
        };
        partition = new Partition(0, new File(tempDir, "events").getAbsolutePath());
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

    private long produceBatchRequests() {
        return broker.getMetrics().scrape().lines()
                .filter(l -> l.startsWith("stormlog_request_latency_seconds_count{api=\"produce_batch\""))
                .mapToLong(l -> (long) Double.parseDouble(l.substring(l.lastIndexOf(' ') + 1))).sum();
    }

    @Test
    void testManyRecordsShipInFewBatchesWithContiguousOffsets() throws Exception {
        try (BatchingProducer producer = new BatchingProducer(client, BatchingProducer.Config.defaults())) {
            List<CompletableFuture<Long>> futures = new ArrayList<>();
            for (int i = 0; i < 2_000; i++) {
                futures.add(producer.send("events", 0, ("r" + i).getBytes(StandardCharsets.UTF_8)));
            }
            producer.flush();
            for (int i = 0; i < futures.size(); i++) {
                assertEquals(i, futures.get(i).get(5, TimeUnit.SECONDS), "Offsets follow send order");
            }
        }
        List<Partition.Record> records = partition.readRecords(0, Integer.MAX_VALUE);
        assertEquals(2_000, records.size());
        assertEquals("r1999", new String(records.get(1999).payload(), StandardCharsets.UTF_8));
        long requests = produceBatchRequests();
        assertTrue(requests < 100, "2,000 tiny records should need far fewer than 100 requests, took " + requests);
    }

    @Test
    void testLingerBoundsLatencyOfALoneRecord() throws Exception {
        try (BatchingProducer producer = new BatchingProducer(client, BatchingProducer.Config.defaults()
                .withBatching(16_384, 20))) {
            long start = System.nanoTime();
            assertEquals(0, producer.send("events", 0, "lonely".getBytes(StandardCharsets.UTF_8)).get(5, TimeUnit.SECONDS));
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(ms >= 15 && ms < 1_000, "A non-full batch ships after linger.ms (took " + ms + " ms)");
        }
    }

    @Test
    void testRetriedIdempotentBatchIsDeduplicatedAsAUnit() throws Exception {
        long[] idAndEpoch = client.initProducerId();
        byte[] request = SimpleKafkaClient.payload(o -> {
            o.writeUTF("raw");
            o.writeUTF("events");
            o.writeInt(0);
            o.writeLong(idAndEpoch[0]);
            o.writeShort((short) idAndEpoch[1]);
            o.writeInt(0); // base sequence
            o.writeInt(3);
            for (String v : new String[]{"a", "b", "c"}) {
                o.writeInt(1);
                o.write(v.getBytes(StandardCharsets.UTF_8));
            }
            o.writeByte(KafkaFrame.ACKS_ALL);
        });
        try (BrokerConnection conn = BrokerConnection.connect("127.0.0.1", server.getPort())) {
            DataInputStream first = conn.request(KafkaFrame.API_KEY_PRODUCE_BATCH, request);
            first.readUTF();
            first.readInt();
            assertEquals(0, first.readLong());
            assertEquals(3, first.readInt());
            assertFalse(first.readBoolean());

            DataInputStream retry = conn.request(KafkaFrame.API_KEY_PRODUCE_BATCH, request); // e.g. response was lost
            retry.readUTF();
            retry.readInt();
            assertEquals(0, retry.readLong(), "Retry returns the original base offset");
            retry.readInt();
            assertTrue(retry.readBoolean(), "and is flagged as a duplicate");
        }
        assertEquals(3, partition.readRecords(0, Integer.MAX_VALUE).size(), "Nothing appended twice");
    }

    @Test
    void testIdempotentProducerEndToEnd() throws Exception {
        try (BatchingProducer producer = new BatchingProducer(client, BatchingProducer.Config.defaults().withIdempotence(true))) {
            List<CompletableFuture<Long>> futures = new ArrayList<>();
            for (int i = 0; i < 1_000; i++) {
                futures.add(producer.send("events", 0, ("r" + i).getBytes(StandardCharsets.UTF_8)));
            }
            producer.flush();
            for (int i = 0; i < futures.size(); i++) {
                assertEquals(i, futures.get(i).get());
            }
        }
        assertEquals(1_000, partition.readRecords(0, Integer.MAX_VALUE).size());
    }

    @Test
    void testInvalidRecordRejectsItsWholeBatch() throws Exception {
        String schema = "{\"type\":\"record\",\"name\":\"E\",\"fields\":[{\"name\":\"n\",\"type\":\"int\"}]}";
        client.registerSchema("events", schema);
        try (BatchingProducer producer = new BatchingProducer(client, BatchingProducer.Config.defaults())) {
            CompletableFuture<Long> f = producer.send("events", 0, "not avro".getBytes(StandardCharsets.UTF_8));
            producer.flush();
            ExecutionException e = assertThrows(ExecutionException.class, f::get);
            assertInstanceOf(BrokerErrorException.class, e.getCause());
            assertEquals(KafkaFrame.ERROR_SCHEMA_VIOLATION, ((BrokerErrorException) e.getCause()).getErrorCode());
        }
        assertEquals(0, partition.getNextOffset());
    }
}
