package com.simplekafka.broker;

import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.metadata.MetadataQuorum;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** Three real brokers in one JVM: embedded Raft metadata quorum, Netty, real clients. */
public class ClusterTest {

    @TempDir
    File tempDir;

    private final Map<Integer, SimpleKafkaBroker> brokers = new ConcurrentHashMap<>();
    private List<MetadataQuorum.Peer> voters;

    @AfterEach
    void teardown() {
        brokers.values().forEach(SimpleKafkaBroker::stop);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private SimpleKafkaBroker newBroker(int id) throws IOException {
        int raftPort = voters.get(id - 1).port();
        SimpleKafkaBroker broker = new SimpleKafkaBroker(id, "127.0.0.1", 0, raftPort) {
            @Override protected String dataDir() {
                return new File(tempDir, "broker-" + id).getAbsolutePath();
            }
        };
        broker.setMetadataQuorum(voters, false);
        broker.setAdminPort(0);
        return broker;
    }

    /** Brokers must start concurrently: registering needs a quorum majority to be up. */
    private void startAll(List<SimpleKafkaBroker> toStart) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(toStart.size());
        List<Future<?>> futures = new ArrayList<>();
        for (SimpleKafkaBroker b : toStart) {
            futures.add(pool.submit(() -> {
                b.start();
                return null;
            }));
        }
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
        toStart.forEach(b -> brokers.put(b.getBrokerId(), b));
    }

    private static void await(String what, long timeoutMs, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("Timed out after " + timeoutMs + " ms waiting for: " + what);
            }
            Thread.sleep(50);
        }
    }

    private SimpleKafkaBroker controller() {
        return brokers.values().stream().filter(SimpleKafkaBroker::isController).findFirst().orElse(null);
    }

    private int leaderOf(SimpleKafkaBroker anyLive, String topic, int partition) {
        var t = anyLive.getMetadataLog().image().topicImage(topic);
        return t == null ? Integer.MIN_VALUE : t.partitions().get(partition).leader();
    }

    private void startThreeBrokerCluster() throws Exception {
        voters = List.of(new MetadataQuorum.Peer(1, "127.0.0.1", freePort()),
                new MetadataQuorum.Peer(2, "127.0.0.1", freePort()),
                new MetadataQuorum.Peer(3, "127.0.0.1", freePort()));
        startAll(List.of(newBroker(1), newBroker(2), newBroker(3)));
        await("controller elected", 15_000, () -> controller() != null);
        await("all brokers registered", 10_000, () -> controller().liveBrokers().size() == 3);
    }

    @Test
    void testSchemaRegisteredOnOneBrokerIsEnforcedByAllViaRaft() throws Exception {
        startThreeBrokerCluster();
        String schema = "{\"type\":\"record\",\"name\":\"Evt\",\"fields\":[{\"name\":\"n\",\"type\":\"int\"}]}";
        try (SimpleKafkaClient client = new SimpleKafkaClient("127.0.0.1", brokers.get(1).getPort())) {
            client.initialize();
            client.createTopic("events", 3, (short) 1);
            await("events visible everywhere", 10_000, () -> brokers.values().stream()
                    .allMatch(b -> b.hostedPartition("events", 0) != null || b.getMetadataLog().image().topicImage("events") != null));
            int id = client.registerSchema("events", schema); // committed via Raft from broker 1
            await("schema replicated", 10_000, () -> brokers.values().stream()
                    .allMatch(b -> b.metadataImage().schemaById(id) != null));

            org.apache.avro.Schema avro = new org.apache.avro.Schema.Parser().parse(schema);
            org.apache.avro.generic.GenericRecord rec = new org.apache.avro.generic.GenericData.Record(avro);
            rec.put("n", 7);
            for (int p = 0; p < 3; p++) { // partitions are led by brokers 1, 2 and 3
                client.send("events", p, com.simplekafka.schema.AvroSerde.serialize(id, avro, rec));
                final int partition = p;
                com.simplekafka.client.BrokerErrorException rejected = assertThrows(
                        com.simplekafka.client.BrokerErrorException.class,
                        () -> client.send("events", partition, "not avro".getBytes(StandardCharsets.UTF_8)));
                assertEquals(com.simplekafka.network.KafkaFrame.ERROR_SCHEMA_VIOLATION, rejected.getErrorCode(),
                        "Every partition leader enforces the replicated schema");
            }
        }
    }

    @Test
    void testClusterSurvivesBrokerCrashAndRecoversItsPartitions() throws Exception {
        startThreeBrokerCluster();

        SimpleKafkaBroker bootstrap = brokers.get(1);
        try (SimpleKafkaClient client = new SimpleKafkaClient("127.0.0.1", bootstrap.getPort())) {
            client.initialize();
            client.createTopic("orders", 3, (short) 1);
            await("orders visible on every broker", 10_000, () -> brokers.values().stream()
                    .allMatch(b -> b.getMetadataLog().image().topicImage("orders") != null));
            Set<Integer> leaders = new TreeSet<>();
            for (int p = 0; p < 3; p++) {
                leaders.add(leaderOf(bootstrap, "orders", p));
            }
            assertEquals(Set.of(1, 2, 3), leaders, "One partition led by each broker");

            for (int i = 0; i < 30; i++) {
                client.send("orders", i % 3, ("o" + i).getBytes(StandardCharsets.UTF_8));
            }

            // Crash the controller (the Raft leader) that also leads a partition: correlated failure
            SimpleKafkaBroker victim = controller();
            int victimId = victim.getBrokerId();
            int victimPartition = -1;
            for (int p = 0; p < 3; p++) {
                if (leaderOf(bootstrap, "orders", p) == victimId) {
                    victimPartition = p;
                }
            }
            long crashedAt = System.nanoTime();
            victim.crashForTest();
            brokers.remove(victimId);

            await("new controller", 10_000, () -> controller() != null && controller().getBrokerId() != victimId);
            System.out.println("Controller failover: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - crashedAt) + " ms");
            SimpleKafkaBroker survivor = brokers.values().iterator().next();
            final int offlinePartition = victimPartition;
            await("crashed broker's partition marked offline", 15_000,
                    () -> leaderOf(survivor, "orders", offlinePartition) == -1);

            // Surviving partitions keep serving; the client re-routes using fresh metadata
            try (SimpleKafkaClient survivorClient = new SimpleKafkaClient("127.0.0.1", survivor.getPort())) {
                survivorClient.initialize();
                for (int p = 0; p < 3; p++) {
                    if (p != offlinePartition) {
                        survivorClient.send("orders", p, "after-crash".getBytes(StandardCharsets.UTF_8));
                        assertEquals(11, survivorClient.fetchRecords("orders", p, 0, 1 << 20).size());
                    }
                }

                // The crashed broker restarts with its data and is re-elected leader of its partition
                SimpleKafkaBroker restarted = newBroker(victimId);
                restarted.start();
                brokers.put(victimId, restarted);
                await("partition back online on restarted broker", 15_000,
                        () -> leaderOf(survivor, "orders", offlinePartition) == victimId);
                survivorClient.refreshMetadata();
                List<SimpleKafkaClient.FetchedRecord> recovered =
                        survivorClient.fetchRecords("orders", offlinePartition, 0, 1 << 20);
                assertEquals(10, recovered.size(), "Data written before the crash is served again");
                survivorClient.send("orders", offlinePartition, "after-restart".getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
