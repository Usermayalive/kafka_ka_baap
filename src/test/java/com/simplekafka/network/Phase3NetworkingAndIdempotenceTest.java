package com.simplekafka.network;

import com.simplekafka.broker.CompactionKeyExtractor;
import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.NettyIdempotentProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class Phase3NetworkingAndIdempotenceTest {

    @TempDir
    File tempDir;

    private NettyBrokerServer server;
    private SimpleKafkaBroker broker;
    private Partition testPartition;

    @BeforeEach
    void setup() throws Exception {
        // Create an in-memory/standalone broker mock with real partition storage
        testPartition = new Partition(0, tempDir.getAbsolutePath());
        broker = new SimpleKafkaBroker(1, "localhost", 0, 2181) {
            @Override
            public void start() {}
            @Override
            public void stop() {}
        };
        broker.getTopics().put("orders", new ArrayList<>(List.of(testPartition)));

        server = new NettyBrokerServer("127.0.0.1", 0, broker);
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void testNettyServerNegotiatesProducerIdAndProducesFrame() throws Exception {
        try (NettyIdempotentProducer producer = new NettyIdempotentProducer("client-1", "127.0.0.1", server.getPort())) {
            producer.connect();

            assertTrue(producer.getProducerId() > 0, "Producer ID must be assigned by broker");
            assertEquals((short) 0, producer.getEpoch(), "Initial epoch must be 0");

            byte[] message = "order-data-1".getBytes(StandardCharsets.UTF_8);
            NettyIdempotentProducer.ProduceRecordResult result = producer.produce("orders", 0, message);

            assertEquals("orders", result.topic());
            assertEquals(0, result.partition());
            assertEquals(0L, result.offset());
            assertFalse(result.isDuplicate());

            // Verify stored in partition
            List<byte[]> stored = testPartition.readMessages(0, 1024);
            assertEquals(1, stored.size());
            assertArrayEquals(message, stored.get(0));
        }
    }

    @Test
    void testEventLoopThreadIsolationOffloadsDiskOperations() throws Exception {
        AtomicReference<String> handlingThreadName = new AtomicReference<>();

        Partition instrumentedPartition = new Partition(0, new File(tempDir, "isolated").getAbsolutePath()) {
            @Override
            public long appendBuffered(byte[] message) throws java.io.IOException {
                handlingThreadName.set(Thread.currentThread().getName());
                return super.appendBuffered(message);
            }
        };
        broker.getTopics().put("isolated-orders", new ArrayList<>(List.of(instrumentedPartition)));

        try (NettyIdempotentProducer producer = new NettyIdempotentProducer("client-isolated", "127.0.0.1", server.getPort())) {
            producer.connect();
            producer.produce("isolated-orders", 0, "test-data".getBytes(StandardCharsets.UTF_8));

            assertNotNull(handlingThreadName.get());
            // Thread MUST start with "kafka-request-exec", NEVER "netty-worker"
            assertTrue(handlingThreadName.get().startsWith("kafka-request-exec"),
                    "Request execution must occur on dedicated thread pool, but executed on: " + handlingThreadName.get());
            assertFalse(handlingThreadName.get().contains("netty-worker"),
                    "Netty worker event loop thread must never execute storage write operations!");
        }
    }

    @Test
    void testConcurrentPipelinedRequestsOnSingleConnectionPreservesOrdering() throws Exception {
        try (NettyIdempotentProducer producer = new NettyIdempotentProducer("client-pipelined", "127.0.0.1", server.getPort())) {
            producer.connect();

            int requestCount = 20;
            List<CompletableFuture<NettyIdempotentProducer.ProduceRecordResult>> futures = new ArrayList<>();

            // Fire 20 asynchronous pipelined requests concurrently on the same connection
            for (int i = 0; i < requestCount; i++) {
                byte[] msg = ("pipelined-msg-" + i).getBytes(StandardCharsets.UTF_8);
                futures.add(producer.produceAsync("orders", 0, msg, i));
            }

            // Wait for all 20 in-flight requests to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);

            // Assert: zero OutOfOrderSequenceException, all succeeded in monotonic offset order 0..19
            for (int i = 0; i < requestCount; i++) {
                NettyIdempotentProducer.ProduceRecordResult res = futures.get(i).join();
                assertEquals((long) i, res.offset(), "Offset must match monotonic sequence order");
                assertFalse(res.isDuplicate());
            }

            // Verify partition has exactly 20 records in exact order
            List<byte[]> stored = testPartition.readMessages(0, 1024 * 1024);
            assertEquals(requestCount, stored.size());
            for (int i = 0; i < requestCount; i++) {
                assertArrayEquals(("pipelined-msg-" + i).getBytes(StandardCharsets.UTF_8), stored.get(i));
            }
        }
    }

    @Test
    void testIdempotentProducerDeduplicatesNetworkRetries() throws Exception {
        try (NettyIdempotentProducer producer = new NettyIdempotentProducer("client-idem", "127.0.0.1", server.getPort())) {
            producer.connect();

            byte[] msg0 = "message-seq-0".getBytes(StandardCharsets.UTF_8);
            byte[] msg1 = "message-seq-1".getBytes(StandardCharsets.UTF_8);
            byte[] msg2 = "message-seq-2".getBytes(StandardCharsets.UTF_8);

            // Step 1: Produce sequence 0
            NettyIdempotentProducer.ProduceRecordResult res0 = producer.produceWithSequence("orders", 0, msg0, 0);
            assertEquals(0L, res0.offset());
            assertFalse(res0.isDuplicate());

            // Step 2: Produce sequence 1
            NettyIdempotentProducer.ProduceRecordResult res1 = producer.produceWithSequence("orders", 0, msg1, 1);
            assertEquals(1L, res1.offset());
            assertFalse(res1.isDuplicate());

            // Step 3: Network retry of sequence 1 (simulating lost ACK)
            NettyIdempotentProducer.ProduceRecordResult retryRes1 = producer.produceWithSequence("orders", 0, msg1, 1);
            assertEquals(1L, retryRes1.offset(), "Duplicate produce must return the previously cached offset");
            assertTrue(retryRes1.isDuplicate(), "Broker must flag result as duplicate");

            // Step 4: Produce sequence 2
            NettyIdempotentProducer.ProduceRecordResult res2 = producer.produceWithSequence("orders", 0, msg2, 2);
            assertEquals(2L, res2.offset());
            assertFalse(res2.isDuplicate());

            // Verify the log contains EXACTLY 3 records, NOT 4!
            List<byte[]> allMessages = testPartition.readMessages(0, 1024 * 1024);
            assertEquals(3, allMessages.size(), "Duplicate sequence 1 must not be appended to log");
            assertArrayEquals(msg0, allMessages.get(0));
            assertArrayEquals(msg1, allMessages.get(1));
            assertArrayEquals(msg2, allMessages.get(2));
        }
    }

    @Test
    void testOutOfOrderSequenceNumberIsRejected() throws Exception {
        try (NettyIdempotentProducer producer = new NettyIdempotentProducer("client-gap", "127.0.0.1", server.getPort())) {
            producer.connect();

            // Send sequence 0
            producer.produceWithSequence("orders", 0, "msg-0".getBytes(), 0);

            // Send sequence 2 (skipping sequence 1)
            OutOfOrderSequenceException ex = assertThrows(OutOfOrderSequenceException.class, () -> {
                producer.produceWithSequence("orders", 0, "msg-2".getBytes(), 2);
            });

            assertTrue(ex.getMessage().contains("Sequence gap"), "Expected sequence gap error message, got: " + ex.getMessage());

            // Partition still only contains record 0
            List<byte[]> messages = testPartition.readMessages(0, 1024);
            assertEquals(1, messages.size());
        }
    }

    @Test
    void testZombieProducerFencingOnEpochBump() throws Exception {
        try (NettyIdempotentProducer producer = new NettyIdempotentProducer("client-zombie", "127.0.0.1", server.getPort())) {
            producer.connect();
            long pid = producer.getProducerId();

            // Send sequence 0 with epoch 0
            producer.produceWithSequence("orders", 0, "valid-msg-0".getBytes(), 0, (short) 0);

            // Simulate epoch bump on broker (e.g. failover or coordinator rebalance)
            server.getProducerIdManager().bumpEpoch(pid); // bump to epoch 1
            // Manually register epoch 1 in idempotence table for partition
            server.getIdempotenceManager().verifyAndApply("orders", 0, pid, (short) 1, 0, () -> testPartition.append("bump".getBytes()));

            // Now zombie producer sends sequence 1 with OLD epoch 0
            ProducerFencedException ex = assertThrows(ProducerFencedException.class, () -> {
                producer.produceWithSequence("orders", 0, "zombie-msg-1".getBytes(), 1, (short) 0);
            });

            assertTrue(ex.getMessage().contains("fenced"), "Expected fenced error message, got: " + ex.getMessage());
        }
    }

    @Test
    void testClientRateLimiterByteAndRequestQuotas() throws Exception {
        // Set low quota: 50 bytes/sec, 2 requests/sec
        ClientQuotaManager tightQuota = new ClientQuotaManager(50, 2);

        // First 2 requests within limit
        tightQuota.checkAndConsume("spammer-client", 20);
        tightQuota.checkAndConsume("spammer-client", 20);

        // 3rd request exceeds request rate quota
        QuotaExceededException qEx = assertThrows(QuotaExceededException.class, () -> {
            tightQuota.checkAndConsume("spammer-client", 10);
        });

        assertTrue(qEx.getMessage().contains("exceeded request rate quota") || qEx.getMessage().contains("exceeded byte rate quota"));
        assertTrue(qEx.getThrottleTimeMs() > 0);
    }

    @Test
    void testBrokerRestartRecoversProducerStateSnapshotAndDeduplicates() throws Exception {
        Partition restartPartition = new Partition(0, tempDir.getAbsolutePath() + File.separator + "recover-topic");
        File partDir = new File(restartPartition.getBaseDir());

        // 1. Initial IdempotenceManager produces records 0, 1, 2
        IdempotenceManager manager1 = new IdempotenceManager(1); // Flush snapshot on every update
        manager1.registerPartitionDir("recover-topic", 0, partDir);

        manager1.verifyAndApply("recover-topic", 0, 888L, (short) 0, 0, () -> restartPartition.append("msg-0".getBytes()));
        manager1.verifyAndApply("recover-topic", 0, 888L, (short) 0, 1, () -> restartPartition.append("msg-1".getBytes()));
        IdempotenceManager.IdempotentResult res2 = manager1.verifyAndApply(
                "recover-topic", 0, 888L, (short) 0, 2, () -> restartPartition.append("msg-2".getBytes())
        );
        assertEquals(2L, res2.offset());
        assertFalse(res2.isDuplicate());

        // Verify snapshot file exists on disk
        File snapshotFile = new File(partDir, IdempotenceManager.SNAPSHOT_FILENAME);
        assertTrue(snapshotFile.exists(), "Snapshot file .producer_state must exist after produces");

        // 2. Simulate broker crash / restart: create fresh IdempotenceManager with empty memory
        IdempotenceManager manager2 = new IdempotenceManager();
        // Load persisted snapshot on partition open
        manager2.registerPartitionDir("recover-topic", 0, partDir);

        // 3. Retry sequence 2 on manager2 (simulating producer reconnecting after broker reboot)
        IdempotenceManager.IdempotentResult retryRes = manager2.verifyAndApply(
                "recover-topic", 0, 888L, (short) 0, 2, () -> {
                    fail("Must NOT invoke appendAction on duplicate sequence after broker restart!");
                    return -1L;
                }
        );
        assertTrue(retryRes.isDuplicate(), "Sequence 2 must be recognized as duplicate from recovered snapshot");
        assertEquals(2L, retryRes.offset(), "Must return cached offset 2");

        // 4. Produce sequence 3 on manager2 -> accepted
        IdempotenceManager.IdempotentResult res3 = manager2.verifyAndApply(
                "recover-topic", 0, 888L, (short) 0, 3, () -> restartPartition.append("msg-3".getBytes())
        );
        assertFalse(res3.isDuplicate());
        assertEquals(3L, res3.offset());

        // Total records in partition must be 4 (offsets 0, 1, 2, 3), NOT 5!
        List<byte[]> msgs = restartPartition.readMessages(0, 1024);
        assertEquals(4, msgs.size(), "Partition must contain exactly 4 unique records");
    }

    @Test
    void testOutOfOrderRetryWithinWindowIsDeduplicated() throws Exception {
        try (NettyIdempotentProducer producer = new NettyIdempotentProducer("client-window", "127.0.0.1", server.getPort())) {
            producer.connect();

            // Send seq 0, 1, 2, 3, 4
            for (int i = 0; i <= 4; i++) {
                NettyIdempotentProducer.ProduceRecordResult res = producer.produceWithSequence(
                        "orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8), i
                );
                assertEquals((long) i, res.offset());
                assertFalse(res.isDuplicate());
            }

            // Simulate dropped ACK for seq 1: producer retries seq 1 (within 5-element window)
            NettyIdempotentProducer.ProduceRecordResult retry1 = producer.produceWithSequence(
                    "orders", 0, "msg-1".getBytes(StandardCharsets.UTF_8), 1
            );
            assertTrue(retry1.isDuplicate(), "In-window retry of seq 1 must be flagged as duplicate");
            assertEquals(1L, retry1.offset(), "In-window retry must return original cached offset 1");

            // Verify log contains exactly 5 records, NOT 6!
            List<byte[]> msgs = testPartition.readMessages(0, 1024 * 1024);
            assertEquals(5, msgs.size(), "Duplicate sequence 1 must not be appended to partition log");
        }
    }

    @Test
    void testOutOfWindowSequenceThrowsDuplicateSequenceException() throws Exception {
        try (NettyIdempotentProducer producer = new NettyIdempotentProducer("client-window-overflow", "127.0.0.1", server.getPort())) {
            producer.connect();

            // Send seq 0, 1, 2, 3, 4, 5
            for (int i = 0; i <= 5; i++) {
                producer.produceWithSequence("orders", 0, ("msg-" + i).getBytes(StandardCharsets.UTF_8), i);
            }

            // Now sliding window of size 5 holds seq [1, 2, 3, 4, 5]. Seq 0 has fallen out of the window!
            DuplicateSequenceException ex = assertThrows(DuplicateSequenceException.class, () -> {
                producer.produceWithSequence("orders", 0, "msg-0".getBytes(StandardCharsets.UTF_8), 0);
            });

            assertTrue(ex.getMessage().contains("too old") || ex.getMessage().contains("sliding window"),
                    "Expected too old / sliding window error, got: " + ex.getMessage());
        }
    }

    @Test
    void testProducerIdAllocationUniqueAcrossBrokers() throws Exception {
        ProducerIdManager broker1 = new ProducerIdManager((short) 1);
        ProducerIdManager broker2 = new ProducerIdManager((short) 2);

        int perBroker = 1000;
        Set<Long> allPids = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<?>> tasks = new ArrayList<>();

        for (int i = 0; i < perBroker; i++) {
            tasks.add(pool.submit(() -> allPids.add(broker1.generateProducerId().producerId())));
            tasks.add(pool.submit(() -> allPids.add(broker2.generateProducerId().producerId())));
        }

        for (Future<?> f : tasks) {
            f.get(5, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // Exactly 2000 unique PIDs, zero collisions!
        assertEquals(2000, allPids.size(), "All allocated PIDs across brokers must be unique");

        // High 16 bits encode brokerId
        for (Long pid : allPids) {
            short bId = (short) (pid >>> 48);
            assertTrue(bId == 1 || bId == 2, "Broker ID must be encoded in bits 48..63");
        }
    }

    @Test
    void testProducerIdMonotonicEpochIncrementAcrossBrokerRestarts() throws Exception {
        File stateFile = new File(tempDir, "broker-epoch.state");

        // Boot 1
        ProducerIdManager boot1 = new ProducerIdManager((short) 5, stateFile);
        long pid1 = boot1.generateProducerId().producerId();
        assertEquals(5, (short) (pid1 >>> 48));
        assertEquals(1, (short) ((pid1 >>> 32) & 0xFFFF)); // Epoch 1

        // Boot 2 (broker restart)
        ProducerIdManager boot2 = new ProducerIdManager((short) 5, stateFile);
        long pid2 = boot2.generateProducerId().producerId();
        assertEquals(5, (short) (pid2 >>> 48));
        assertEquals(2, (short) ((pid2 >>> 32) & 0xFFFF)); // Epoch 2

        assertNotEquals(pid1, pid2, "PIDs across broker restarts must never collide");
    }

    @Test
    void testCompactionPreservesOriginalOffsetsAndIndex() throws Exception {
        Partition compactPartition = new Partition(0, tempDir.getAbsolutePath() + File.separator + "compact-topic");

        // Write 10 keys x 2 versions = 20 messages
        // v1: keys 0..9 (offsets 0..9)
        for (int k = 0; k < 10; k++) {
            compactPartition.append(("key-" + k + ":v1").getBytes(StandardCharsets.UTF_8));
        }
        // v2: keys 0..9 (offsets 10..19)
        for (int k = 0; k < 10; k++) {
            compactPartition.append(("key-" + k + ":v2").getBytes(StandardCharsets.UTF_8));
        }

        assertEquals(20, compactPartition.readMessages(0, 1024 * 1024).size());

        // Compact using domain-agnostic CompactionKeyExtractor
        CompactionKeyExtractor extractor = msg -> {
            String s = new String(msg, StandardCharsets.UTF_8);
            return s.split(":")[0];
        };

        compactPartition.compact(extractor);

        // Verify (a): exactly 10 records remain
        List<byte[]> retained = compactPartition.readMessages(0, 1024 * 1024);
        assertEquals(10, retained.size(), "Exactly 10 unique key records must remain after compaction");

        for (int k = 0; k < 10; k++) {
            String content = new String(retained.get(k), StandardCharsets.UTF_8);
            assertEquals("key-" + k + ":v2", content, "Retained message must be the latest version (v2)");
        }

        // Verify (b) & (c): Original offsets are preserved (offsets 10..19) and index lookup works across offset gaps
        // Reading starting at offset 15 should read key-5:v2 through key-9:v2 (5 messages)
        List<byte[]> fromOffset15 = compactPartition.readMessages(15, 1024 * 1024);
        assertEquals(5, fromOffset15.size());
        assertEquals("key-5:v2", new String(fromOffset15.get(0), StandardCharsets.UTF_8));

        // Verify (d): Compacting a second time is idempotent
        compactPartition.compact(extractor);
        List<byte[]> retainedSecond = compactPartition.readMessages(0, 1024 * 1024);
        assertEquals(10, retainedSecond.size());
        assertEquals("key-0:v2", new String(retainedSecond.get(0), StandardCharsets.UTF_8));
    }

    @Test
    void testCompactionCrashRecoveryCleansUpIncompleteStaging() throws Exception {
        String topicBase = tempDir.getAbsolutePath() + File.separator + "crash-compact-topic";
        Partition part = new Partition(0, topicBase);

        // Write 10 clean records
        for (int i = 0; i < 10; i++) {
            part.append(("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }
        assertEquals(10, part.readMessages(0, 1024 * 1024).size());

        // Simulate a crash mid-compaction:
        // Create .compact.lock and .compact_staging with partial corrupt files
        File partDir = new File(part.getBaseDir());
        File lockFile = new File(partDir, ".compact.lock");
        lockFile.createNewFile();
        File stagingDir = new File(partDir, ".compact_staging");
        stagingDir.mkdirs();
        File junkFile = new File(stagingDir, "incomplete.tmp");
        junkFile.createNewFile();

        // Simulate broker reboot: re-initialize partition from disk
        Partition recoveredPart = new Partition(0, topicBase);

        // Assert: staging files and lock cleaned up, all 10 original records intact
        assertFalse(lockFile.exists(), ".compact.lock must be cleaned up on recovery");
        assertFalse(stagingDir.exists(), ".compact_staging must be cleaned up on recovery");

        List<byte[]> msgs = recoveredPart.readMessages(0, 1024 * 1024);
        assertEquals(10, msgs.size(), "All 10 original messages must remain intact after crash recovery");
        for (int i = 0; i < 10; i++) {
            assertEquals("msg-" + i, new String(msgs.get(i), StandardCharsets.UTF_8));
        }
    }

    @Test
    void testCompactionTombstoneDeletesKey() throws Exception {
        Partition part = new Partition(0, tempDir.getAbsolutePath() + File.separator + "tombstone-topic");

        // Write key-A and key-B
        part.append("key-A:value1".getBytes(StandardCharsets.UTF_8));
        part.append("key-B:value1".getBytes(StandardCharsets.UTF_8));

        // Tombstone for key-A: empty byte array payload
        part.append(new byte[0]);

        CompactionKeyExtractor extractor = msg -> {
            if (msg.length == 0) return "key-A"; // tombstone targeting key-A
            return new String(msg, StandardCharsets.UTF_8).split(":")[0];
        };

        part.compact(extractor);

        List<byte[]> remaining = part.readMessages(0, 1024 * 1024);
        assertEquals(1, remaining.size(), "Key-A must be deleted by tombstone, only Key-B remains");
        assertEquals("key-B:value1", new String(remaining.get(0), StandardCharsets.UTF_8));
    }

    @Test
    void testBoundedQueueRejectsOnOverload() throws Exception {
        CountDownLatch blockingLatch = new CountDownLatch(1);
        CountDownLatch reachedLatch = new CountDownLatch(1);

        Partition slowPartition = new Partition(0, new File(tempDir, "overload-part").getAbsolutePath()) {
            @Override
            public long appendBuffered(byte[] message) throws java.io.IOException {
                reachedLatch.countDown();
                try {
                    blockingLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.appendBuffered(message);
            }
        };

        SimpleKafkaBroker slowBroker = new SimpleKafkaBroker(2, "localhost", 0, 2181) {
            @Override public void start() {}
            @Override public void stop() {}
        };
        slowBroker.getTopics().put("slow-topic", new ArrayList<>(List.of(slowPartition)));

        // Server with queue capacity of ONLY 1
        NettyBrokerServer overloadedServer = new NettyBrokerServer(
                "127.0.0.1", 0, slowBroker,
                new ClientQuotaManager(), new IdempotenceManager(), new ProducerIdManager(), 1
        );
        overloadedServer.start();

        try {
            AtomicInteger rejectedCount = new AtomicInteger(0);
            int totalRequests = 30; // 16 worker threads + 1 queue = 17 capacity; 30 requests will trigger rejection!
            ExecutorService clientPool = Executors.newFixedThreadPool(totalRequests);

            for (int i = 0; i < totalRequests; i++) {
                final int seq = i;
                clientPool.submit(() -> {
                    try (NettyIdempotentProducer p = new NettyIdempotentProducer("client-ovl-" + seq, "127.0.0.1", overloadedServer.getPort())) {
                        p.connect();
                        p.produce("slow-topic", 0, ("data-" + seq).getBytes());
                    } catch (BrokerOverloadedException boe) {
                        rejectedCount.incrementAndGet();
                    } catch (Exception e) {
                        // ignore normal connection closure
                    }
                });
            }

            // Wait until workers begin blocking
            reachedLatch.await(3, TimeUnit.SECONDS);
            Thread.sleep(500); // let subsequent requests hit the full queue

            // Release workers
            blockingLatch.countDown();
            clientPool.shutdown();
            clientPool.awaitTermination(5, TimeUnit.SECONDS);

            assertTrue(rejectedCount.get() > 0,
                    "Expected at least one request to receive BrokerOverloadedException, but got " + rejectedCount.get());
        } finally {
            blockingLatch.countDown();
            overloadedServer.stop();
        }
    }

    @Test
    void testOversizedFrameIsClosed() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", server.getPort())) {
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            // Send a frame length of 20 MB (> 16 MB MAX_FRAME_BYTES limit)
            dos.writeInt(20 * 1024 * 1024);
            dos.flush();

            // Server's LengthFieldBasedFrameDecoder will throw TooLongFrameException and close channel
            InputStream in = socket.getInputStream();
            int readByte = in.read();
            assertEquals(-1, readByte, "Server must close the socket immediately upon oversized frame");
        }
    }

    @Test
    void testTwoProducersOnSamePartitionIndependentSequences() throws Exception {
        try (NettyIdempotentProducer prodA = new NettyIdempotentProducer("prod-A", "127.0.0.1", server.getPort());
             NettyIdempotentProducer prodB = new NettyIdempotentProducer("prod-B", "127.0.0.1", server.getPort())) {
            prodA.connect();
            prodB.connect();

            assertNotEquals(prodA.getProducerId(), prodB.getProducerId(), "Producers must have distinct PIDs");

            // Producer A: seq 0, seq 1
            NettyIdempotentProducer.ProduceRecordResult resA0 = prodA.produceWithSequence("orders", 0, "A0".getBytes(), 0);
            NettyIdempotentProducer.ProduceRecordResult resA1 = prodA.produceWithSequence("orders", 0, "A1".getBytes(), 1);

            // Producer B: seq 0, seq 1 (independent sequence space starting at 0!)
            NettyIdempotentProducer.ProduceRecordResult resB0 = prodB.produceWithSequence("orders", 0, "B0".getBytes(), 0);
            NettyIdempotentProducer.ProduceRecordResult resB1 = prodB.produceWithSequence("orders", 0, "B1".getBytes(), 1);

            assertFalse(resA0.isDuplicate());
            assertFalse(resA1.isDuplicate());
            assertFalse(resB0.isDuplicate());
            assertFalse(resB1.isDuplicate());

            // Retry A1 -> cached offset of A1
            NettyIdempotentProducer.ProduceRecordResult retryA1 = prodA.produceWithSequence("orders", 0, "A1".getBytes(), 1);
            assertTrue(retryA1.isDuplicate());
            assertEquals(resA1.offset(), retryA1.offset());

            // Retry B0 -> cached offset of B0
            NettyIdempotentProducer.ProduceRecordResult retryB0 = prodB.produceWithSequence("orders", 0, "B0".getBytes(), 0);
            assertTrue(retryB0.isDuplicate());
            assertEquals(resB0.offset(), retryB0.offset());

            // Total records: A0, A1, B0, B1 = 4 records
            List<byte[]> msgs = testPartition.readMessages(0, 1024);
            assertEquals(4, msgs.size());
        }
    }

    @Test
    void testTwoProducersOnSamePartitionOneFencedOtherSurvives() throws Exception {
        try (NettyIdempotentProducer prodA = new NettyIdempotentProducer("client-fenced", "127.0.0.1", server.getPort());
             NettyIdempotentProducer prodB = new NettyIdempotentProducer("client-healthy", "127.0.0.1", server.getPort())) {
            prodA.connect();
            prodB.connect();

            // Both produce sequence 0
            prodA.produceWithSequence("orders", 0, "A0".getBytes(), 0, (short) 0);
            prodB.produceWithSequence("orders", 0, "B0".getBytes(), 0, (short) 0);

            // Fence producer A on the broker
            server.getProducerIdManager().bumpEpoch(prodA.getProducerId());
            server.getIdempotenceManager().verifyAndApply("orders", 0, prodA.getProducerId(), (short) 1, 0, () -> testPartition.append("A-bump".getBytes()));

            // Producer A attempts produce with old epoch -> fenced
            assertThrows(ProducerFencedException.class, () -> {
                prodA.produceWithSequence("orders", 0, "A1".getBytes(), 1, (short) 0);
            });

            // Producer B continues producing normally -> success!
            NettyIdempotentProducer.ProduceRecordResult resB1 = prodB.produceWithSequence("orders", 0, "B1".getBytes(), 1, (short) 0);
            assertFalse(resB1.isDuplicate());
            assertTrue(resB1.offset() > 0);
        }
    }

    @Test
    void testProducerWithNonZeroFirstSequenceRejected() throws Exception {
        try (NettyIdempotentProducer producer = new NettyIdempotentProducer("client-non-zero-first", "127.0.0.1", server.getPort())) {
            producer.connect();

            OutOfOrderSequenceException ex = assertThrows(OutOfOrderSequenceException.class, () -> {
                producer.produceWithSequence("orders", 0, "first-message".getBytes(), 5);
            });

            assertTrue(ex.getMessage().contains("Expected sequence number 0 for new producer"),
                    "Expected sequence 0 rejection, got: " + ex.getMessage());
        }
    }

    @Test
    void testZombieProducerEqualOrLowerEpochFencing() throws Exception {
        try (NettyIdempotentProducer producer = new NettyIdempotentProducer("client-fenced-check", "127.0.0.1", server.getPort())) {
            producer.connect();
            long pid = producer.getProducerId();

            producer.produceWithSequence("orders", 0, "epoch-0-msg".getBytes(), 0, (short) 0);

            // Failover simulation: bump epoch on server for this PID
            server.getProducerIdManager().bumpEpoch(pid); // now epoch 1
            server.getIdempotenceManager().verifyAndApply("orders", 0, pid, (short) 1, 0, () -> testPartition.append("bump".getBytes()));

            // Zombie tries to send with epoch 0
            ProducerFencedException ex = assertThrows(ProducerFencedException.class, () -> {
                producer.produceWithSequence("orders", 0, "zombie".getBytes(), 1, (short) 0);
            });
            assertTrue(ex.getMessage().contains("fenced"));
        }
    }
}
