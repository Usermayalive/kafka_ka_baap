package com.simplekafka.broker;

import com.simplekafka.client.BrokerErrorException;
import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.coordinator.ConsumerOffsetStore;
import com.simplekafka.metadata.MetadataImage;
import com.simplekafka.metadata.MetadataQuorum;
import com.simplekafka.network.KafkaFrame;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** Data replication across three real brokers (embedded Raft, Netty, replica fetchers). */
public class ReplicationTest {

    @TempDir
    File tempDir;

    private final Map<Integer, SimpleKafkaBroker> brokers = new ConcurrentHashMap<>();
    private List<MetadataQuorum.Peer> voters;
    private SimpleKafkaClient client;

    @BeforeEach
    void startCluster() throws Exception {
        voters = new ArrayList<>();
        for (int id = 1; id <= 3; id++) {
            try (ServerSocket s = new ServerSocket(0)) {
                voters.add(new MetadataQuorum.Peer(id, "127.0.0.1", s.getLocalPort()));
            }
        }
        ExecutorService pool = Executors.newFixedThreadPool(3);
        List<Future<SimpleKafkaBroker>> started = new ArrayList<>();
        for (int id = 1; id <= 3; id++) {
            SimpleKafkaBroker b = newBroker(id);
            started.add(pool.submit(() -> {
                b.start();
                return b;
            }));
        }
        for (Future<SimpleKafkaBroker> f : started) {
            try {
                SimpleKafkaBroker b = f.get(60, TimeUnit.SECONDS);
                brokers.put(b.getBrokerId(), b);
            } catch (TimeoutException e) {
                // Diagnose instead of guessing: where is startup stuck?
                Thread.getAllStackTraces().forEach((t, stack) -> {
                    if (t.getName().contains("pool") || t.getName().contains("controller") || t.getName().contains("main")) {
                        System.out.println("THREAD " + t.getName() + " " + t.getState());
                        for (StackTraceElement el : stack) {
                            System.out.println("    at " + el);
                        }
                    }
                });
                throw e;
            }
        }
        pool.shutdown();
        await("controller and 3 brokers", 20_000, () -> controller() != null && controller().liveBrokers().size() == 3);
        client = new SimpleKafkaClient("127.0.0.1", brokers.get(1).getPort());
        client.initialize();
    }

    @AfterEach
    void stopCluster() {
        if (client != null) {
            client.close();
        }
        brokers.values().forEach(SimpleKafkaBroker::stop);
    }

    private SimpleKafkaBroker newBroker(int id) throws IOException {
        SimpleKafkaBroker broker = new SimpleKafkaBroker(id, "127.0.0.1", 0, voters.get(id - 1).port()) {
            @Override protected String dataDir() {
                return new File(tempDir, "broker-" + id).getAbsolutePath();
            }
        };
        broker.setMetadataQuorum(voters, false);
        broker.setAdminPort(0);
        return broker;
    }

    private SimpleKafkaBroker controller() {
        return brokers.values().stream().filter(SimpleKafkaBroker::isController).findFirst().orElse(null);
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

    private MetadataImage.PartitionState state(String topic) {
        SimpleKafkaBroker any = brokers.values().iterator().next();
        MetadataImage.TopicImage t = any.getMetadataLog().image().topicImage(topic);
        return t == null ? null : t.partitions().get(0);
    }

    private void createReplicatedTopic(String topic) throws Exception {
        client.createTopic(topic, 1, (short) 3);
        await("all replicas hosting " + topic, 15_000, () -> brokers.values().stream()
                .allMatch(b -> b.hostedPartition(topic, 0) != null));
        await("ISR of 3", 15_000, () -> {
            SimpleKafkaBroker leader = brokers.get(state(topic).leader());
            Set<Integer> isr = leader.getReplicaManager().isrOf(topic, 0);
            return isr != null && isr.size() == 3;
        });
    }

    private static List<String> contents(Partition p) throws IOException {
        List<String> out = new ArrayList<>();
        for (Partition.Record r : p.readRecords(0, Integer.MAX_VALUE)) {
            out.add(r.offset() + ":" + new String(r.payload(), StandardCharsets.UTF_8));
        }
        return out;
    }

    private void send(String topic, String value, byte acks) throws IOException {
        client.send(topic, 0, value.getBytes(StandardCharsets.UTF_8), acks);
    }

    @Test
    void testReplicasConvergeAndLeaderCrashLosesNothingCommitted() throws Exception {
        createReplicatedTopic("orders");
        for (int i = 0; i < 50; i++) {
            send("orders", "o" + i, KafkaFrame.ACKS_ALL);
        }
        List<String> expected = contents(brokers.get(state("orders").leader()).hostedPartition("orders", 0));
        assertEquals(50, expected.size());
        for (SimpleKafkaBroker b : brokers.values()) {
            await("broker " + b.getBrokerId() + " has all 50", 10_000, () -> {
                try {
                    return contents(b.hostedPartition("orders", 0)).equals(expected);
                } catch (IOException e) {
                    return false;
                }
            });
        }

        int oldLeader = state("orders").leader();
        int oldEpoch = state("orders").leaderEpoch();
        long crashedAt = System.nanoTime();
        brokers.remove(oldLeader).crashForTest();
        await("a follower takes over", 15_000, () -> state("orders").leader() != oldLeader
                && state("orders").leader() > 0);
        System.out.println("Partition failover after leader crash: "
                + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - crashedAt) + " ms");
        assertTrue(state("orders").leaderEpoch() > oldEpoch);
        assertFalse(state("orders").isr().contains(oldLeader), "Dead leader leaves the ISR on failover");

        try (SimpleKafkaClient survivor = new SimpleKafkaClient("127.0.0.1",
                brokers.values().iterator().next().getPort())) {
            survivor.initialize();
            for (int i = 50; i < 60; i++) {
                survivor.send("orders", 0, ("o" + i).getBytes(StandardCharsets.UTF_8));
            }
            List<SimpleKafkaClient.FetchedRecord> all = survivor.fetchRecords("orders", 0, 0, 1 << 20);
            assertEquals(60, all.size(), "All 50 committed records survived, plus 10 new ones");
            for (int i = 0; i < 60; i++) {
                assertEquals(i, all.get(i).offset());
                assertEquals("o" + i, new String(all.get(i).payload(), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void testRestartedLeaderTruncatesDivergentTailAndRejoins() throws Exception {
        createReplicatedTopic("orders");
        for (int i = 0; i < 10; i++) {
            send("orders", "committed-" + i, KafkaFrame.ACKS_ALL);
        }
        int leaderId = state("orders").leader();
        SimpleKafkaBroker leader = brokers.get(leaderId);
        await("followers caught up", 10_000, () -> leader.hostedPartition("orders", 0).getHighWatermark() == 10);

        // Followers stall; the leader accepts acks=1 writes nobody else has, then dies
        brokers.values().stream().filter(b -> b != leader)
                .forEach(b -> b.getReplicaManager().setFetchingPausedForTest(true));
        for (int i = 0; i < 5; i++) {
            send("orders", "unreplicated-" + i, KafkaFrame.ACKS_LEADER);
        }
        assertEquals(15, leader.hostedPartition("orders", 0).getNextOffset());
        brokers.remove(leaderId).crashForTest();
        brokers.values().forEach(b -> b.getReplicaManager().setFetchingPausedForTest(false));

        await("new leader", 15_000, () -> state("orders").leader() != leaderId && state("orders").leader() > 0);
        try (SimpleKafkaClient survivor = new SimpleKafkaClient("127.0.0.1",
                brokers.values().iterator().next().getPort())) {
            survivor.initialize();
            for (int i = 0; i < 3; i++) {
                survivor.send("orders", 0, ("new-leader-" + i).getBytes(StandardCharsets.UTF_8));
            }
        }

        // The old leader returns: its 5 unreplicated records diverge from the new leader's 3
        SimpleKafkaBroker restarted = newBroker(leaderId);
        restarted.start();
        brokers.put(leaderId, restarted);
        SimpleKafkaBroker newLeader = brokers.get(state("orders").leader());
        List<String> expected = contents(newLeader.hostedPartition("orders", 0));
        assertEquals(13, expected.size());
        await("old leader converges on the new leader's log", 20_000, () -> {
            try {
                return contents(restarted.hostedPartition("orders", 0)).equals(expected);
            } catch (IOException | RuntimeException e) {
                return false;
            }
        });
        assertTrue(contents(restarted.hostedPartition("orders", 0)).stream().noneMatch(r -> r.contains("unreplicated")),
                "Uncommitted records of the old epoch are truncated, never served");
        await("old leader rejoins the ISR", 20_000,
                () -> newLeader.getReplicaManager().isrOf("orders", 0) != null
                        && newLeader.getReplicaManager().isrOf("orders", 0).contains(leaderId));
    }

    @Test
    void testHealthyFollowersStayInIsrUnderSteadyWrites() throws Exception {
        createReplicatedTopic("orders");
        SimpleKafkaBroker leader = brokers.get(state("orders").leader());
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ReplicaManager.REPLICA_LAG_MAX_MS * 2 + 1_000);
        int sent = 0;
        while (System.nanoTime() < until) { // the leader's log end moves between every fetch
            send("orders", "steady-" + sent++, KafkaFrame.ACKS_LEADER);
            Thread.sleep(1); // ~1,000 writes/s, under the default per-client quota
        }
        assertEquals(3, leader.getReplicaManager().isrOf("orders", 0).size(),
                "Followers keeping up with a moving log end must not be evicted (" + sent + " writes)");
    }

    @Test
    void testIdempotentBatchingProducerSurvivesLeaderCrashWithoutDuplicates() throws Exception {
        createReplicatedTopic("orders");
        int oldLeader = state("orders").leader();
        int bootstrapId = brokers.keySet().stream().filter(id -> id != oldLeader).findFirst().orElseThrow();
        try (SimpleKafkaClient producerClient = new SimpleKafkaClient("127.0.0.1", brokers.get(bootstrapId).getPort());
             com.simplekafka.client.BatchingProducer producer = new com.simplekafka.client.BatchingProducer(producerClient,
                     com.simplekafka.client.BatchingProducer.Config.defaults().withIdempotence(true))) {
            producerClient.initialize();
            List<CompletableFuture<Long>> futures = new ArrayList<>();
            for (int i = 0; i < 3_000; i++) {
                futures.add(producer.send("orders", 0, ("r" + i).getBytes(StandardCharsets.UTF_8)));
                if (i == 1_000) {
                    brokers.remove(oldLeader).crashForTest(); // mid-stream: in-flight batches must retry
                }
                if (i % 100 == 0) {
                    Thread.sleep(2);
                }
            }
            producer.flush();
            int failed = 0;
            for (CompletableFuture<Long> f : futures) {
                try {
                    f.get(1, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    failed++;
                }
            }
            SimpleKafkaBroker newLeader = brokers.get(state("orders").leader());
            List<String> log = contents(newLeader.hostedPartition("orders", 0));
            long distinct = log.stream().map(r -> r.substring(r.indexOf(':') + 1)).distinct().count();
            System.out.println("Batching producer across leader crash: " + log.size() + " records, " + failed + " failed sends");
            assertEquals(0, failed, "Replicated producer state lets every batch retry succeed on the new leader");
            assertEquals(3_000, log.size(), "Every record is in the log exactly once");
            assertEquals(log.size(), distinct, "No record appended twice (idempotent batch retries)");
        }
    }

    @Test
    void testConsumersOnlySeeCommittedRecords() throws Exception {
        createReplicatedTopic("orders");
        send("orders", "committed", KafkaFrame.ACKS_ALL);
        SimpleKafkaBroker leader = brokers.get(state("orders").leader());
        brokers.values().stream().filter(b -> b != leader)
                .forEach(b -> b.getReplicaManager().setFetchingPausedForTest(true));
        send("orders", "pending", KafkaFrame.ACKS_LEADER);

        assertEquals(1, client.fetchRecords("orders", 0, 0, 1 << 20).size(),
                "A record not yet on every in-sync replica is invisible to consumers");

        brokers.values().forEach(b -> b.getReplicaManager().setFetchingPausedForTest(false));
        await("pending record committed", 10_000, () -> {
            try {
                return client.fetchRecords("orders", 0, 0, 1 << 20).size() == 2;
            } catch (IOException e) {
                return false;
            }
        });
    }

    @Test
    void testMinInsyncReplicasRejectsAcksAllWhenIsrShrinks() throws Exception {
        brokers.values().forEach(b -> b.setMinInsyncReplicas(2));
        createReplicatedTopic("orders");
        send("orders", "ok", KafkaFrame.ACKS_ALL);
        SimpleKafkaBroker leader = brokers.get(state("orders").leader());
        brokers.values().stream().filter(b -> b != leader)
                .forEach(b -> b.getReplicaManager().setFetchingPausedForTest(true));

        // stormlog.replica.lag.max.ms=2000 in tests: lagging followers leave the ISR
        await("ISR shrinks to the leader", 15_000,
                () -> leader.getReplicaManager().isrOf("orders", 0).equals(Set.of(leader.getBrokerId())));
        BrokerErrorException rejected = assertThrows(BrokerErrorException.class,
                () -> send("orders", "needs-2-replicas", KafkaFrame.ACKS_ALL));
        assertEquals(KafkaFrame.ERROR_NOT_ENOUGH_REPLICAS, rejected.getErrorCode());
        send("orders", "leader-only-is-fine", KafkaFrame.ACKS_LEADER);

        brokers.values().forEach(b -> b.getReplicaManager().setFetchingPausedForTest(false));
        await("followers rejoin", 15_000, () -> leader.getReplicaManager().isrOf("orders", 0).size() == 3);
        send("orders", "accepted-again", KafkaFrame.ACKS_ALL);
    }

    private static final String USER_PII = """
            {"type":"record","name":"User","pii.entity":"userId","fields":[
              {"name":"userId","type":"string"},
              {"name":"email","type":"string","pii":true}]}""";

    @Test
    void testPiiStaysReadableAfterFailoverAndErasureReachesEveryReplica() throws Exception {
        byte[] masterKey = new byte[32];
        new java.security.SecureRandom().nextBytes(masterKey);
        for (SimpleKafkaBroker b : brokers.values()) {
            b.enableCryptoShredding(new com.simplekafka.gdpr.EnvMasterKeyProvider(masterKey)); // same key everywhere
        }
        createReplicatedTopic("users");
        int schemaId = client.registerSchema("users", USER_PII);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(USER_PII);
        org.apache.avro.generic.GenericRecord user = new org.apache.avro.generic.GenericData.Record(schema);
        user.put("userId", "u-1");
        user.put("email", "alice@example.com");
        client.send("users", 0, com.simplekafka.schema.AvroSerde.serialize(schemaId, schema, user));

        int oldLeader = state("users").leader();
        brokers.remove(oldLeader).crashForTest();
        await("new leader", 15_000, () -> state("users").leader() != oldLeader && state("users").leader() > 0);
        try (SimpleKafkaClient survivor = new SimpleKafkaClient("127.0.0.1",
                brokers.values().iterator().next().getPort())) {
            survivor.initialize();
            com.simplekafka.schema.AvroSerde serde = new com.simplekafka.schema.AvroSerde(survivor::getSchema);
            // A new leader's HW can trail records committed just before the crash until the
            // remaining follower fetches from it; a consumer simply polls again
            await("committed record visible on the new leader", 10_000, () -> {
                try {
                    return !survivor.fetch("users", 0, 0, 1 << 20).isEmpty();
                } catch (IOException e) {
                    return false;
                }
            });
            assertEquals("alice@example.com",
                    serde.deserialize(survivor.fetch("users", 0, 0, 1 << 20).get(0)).get("email").toString(),
                    "The new leader decrypts with the replicated data key");

            survivor.forgetEntity("u-1");
            assertEquals(com.simplekafka.gdpr.CryptoShredder.REDACTED,
                    serde.deserialize(survivor.fetch("users", 0, 0, 1 << 20).get(0)).get("email").toString());
            String entityKey = com.simplekafka.gdpr.CryptoShredder.entityKey("u-1");
            await("erased key gone from every broker's replicated image", 10_000, () -> brokers.values().stream()
                    .allMatch(b -> b.metadataImage().wrappedDek(entityKey + "-g0") == null));
        }
    }

    @Test
    void testCommittedOffsetsSurviveCoordinatorCrash() throws Exception {
        client.createTopic("orders", 1, (short) 3);
        client.commitOffset("billing", "orders", 0, 1, ""); // creates __consumer_offsets
        int offsetsPartition = ConsumerOffsetStore.partitionFor("billing");
        await("full ISR on the offsets partition", 20_000, () -> brokers.values().stream().anyMatch(b -> {
            Set<Integer> isr = b.getReplicaManager().isrOf(ConsumerOffsetStore.TOPIC, offsetsPartition);
            return isr != null && isr.size() == 3;
        }));
        client.commitOffset("billing", "orders", 0, 42, ""); // acknowledged once on all 3 replicas
        int coordinator = brokers.values().iterator().next().getMetadataLog().image()
                .topicImage(ConsumerOffsetStore.TOPIC).partitions().get(offsetsPartition).leader();
        assertEquals(3, brokers.values().iterator().next().getMetadataLog().image()
                .topicImage(ConsumerOffsetStore.TOPIC).partitions().get(offsetsPartition).replicas().size());

        brokers.remove(coordinator).crashForTest();
        try (SimpleKafkaClient survivor = new SimpleKafkaClient("127.0.0.1",
                brokers.values().iterator().next().getPort())) {
            survivor.initialize();
            await("a new coordinator answers", 20_000, () -> {
                try {
                    return survivor.fetchCommittedOffset("billing", "orders", 0) == 42;
                } catch (IOException e) {
                    return false;
                }
            });
        }
    }
}
