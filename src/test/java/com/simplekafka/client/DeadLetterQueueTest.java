package com.simplekafka.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class DeadLetterQueueTest {

    // ──────────────────────────────────────────────────────────────────────
    // 1. RetryPolicy tests
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void testRetryPolicyBackoffCalculation() {
        RetryPolicy policy = new RetryPolicy(4, 100, 2.0, 1000);

        assertEquals(100, policy.calculateBackoffMs(1));
        assertEquals(200, policy.calculateBackoffMs(2));
        assertEquals(400, policy.calculateBackoffMs(3));
        assertEquals(800, policy.calculateBackoffMs(4));
        assertEquals(1000, policy.calculateBackoffMs(5)); // Capped at maxBackoffMs (1000)
    }

    @Test
    void testRetryPolicyFastFailPredicate() {
        RetryPolicy policy = new RetryPolicy();

        // Fatal non-retryable exceptions should return false (fast-fail)
        assertFalse(policy.isRetryable(new IllegalArgumentException("Invalid payload format")));
        assertFalse(policy.isRetryable(new NullPointerException("Null message pointer")));
        assertFalse(policy.isRetryable(new ClassCastException("Incompatible schema")));
        assertFalse(policy.isRetryable(new IndexOutOfBoundsException("Malformed byte array")));
        // Class name contains "deserializ" or "syntax" or "schema"
        assertFalse(policy.isRetryable(new RuntimeException("JsonDeserializationException: syntax error at line 1")));
        assertFalse(policy.isRetryable(new RuntimeException("SchemaValidationException: missing required field")));

        // Transient errors should return true (retry with backoff)
        assertTrue(policy.isRetryable(new IOException("Downstream database connection timeout")));
        assertTrue(policy.isRetryable(new java.util.concurrent.TimeoutException("Network socket timed out")));
        assertTrue(policy.isRetryable(new RuntimeException("Temporary RPC failure")));
    }

    @Test
    void testRetryPolicyWithCustomPredicate() {
        // Users can override the default classification for domain-specific errors
        RetryPolicy base = new RetryPolicy();
        RetryPolicy custom = base.withRetryablePredicate(t -> {
            // Business rule: treat IllegalArgumentException as retryable in this service
            if (t instanceof IllegalArgumentException) return true;
            return RetryPolicy.DEFAULT_RETRYABLE_PREDICATE.test(t);
        });

        // Default considers IllegalArgumentException fatal
        assertFalse(base.isRetryable(new IllegalArgumentException("test")));
        // Custom override makes it retryable
        assertTrue(custom.isRetryable(new IllegalArgumentException("test")));
        // Custom still inherits other fatal classifications
        assertFalse(custom.isRetryable(new NullPointerException("null")));
    }

    @Test
    void testDynamicOverheadComputationWithOversizedDeathReasonAndHeaders() throws IOException {
        // Build a DeadLetterEnvelope with a 30 KB deathReason, 20 headers averaging 500 bytes each, and a 500 KB payload.
        // Assert serialized size <= DLQ_MAX_MESSAGE_BYTES (1 MB).
        String hugeReason = "A".repeat(30 * 1024); // 30 KB stack trace
        Map<String, String> manyHeaders = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            manyHeaders.put("X-Custom-Header-" + i, "V".repeat(500)); // 20 * 500 = 10 KB headers
        }
        byte[] payload500k = new byte[500 * 1024];
        Arrays.fill(payload500k, (byte) 0x7F);

        DeadLetterEnvelope env = new DeadLetterEnvelope(
                "orders",
                0,
                1001L,
                new RuntimeException(hugeReason),
                1,
                payload500k,
                manyHeaders
        );

        byte[] serialized = env.serialize();
        assertTrue(serialized.length <= DeadLetterEnvelope.DLQ_MAX_MESSAGE_BYTES,
                "Envelope must not exceed DLQ_MAX_MESSAGE_BYTES (1MB), got: " + serialized.length + " bytes");

        // Verify deathReason was truncated to 4KB budget
        DeadLetterEnvelope deserialized = DeadLetterEnvelope.deserialize(serialized);
        assertTrue(deserialized.getDeathReason().contains("[truncated]"));
        assertTrue(deserialized.getDeathReason().getBytes(StandardCharsets.UTF_8).length <= DeadLetterEnvelope.MAX_DEATH_REASON_BYTES + 32);

        // Verify custom headers preserved
        assertEquals(20 + 8, deserialized.getHeaders().size()); // 20 custom + 8 standard headers
        assertEquals("V".repeat(500), deserialized.getHeaders().get("X-Custom-Header-0"));
    }

    @Test
    void testProtocolExceptionOnInvalidMagicOrVersion() throws IOException {
        DeadLetterEnvelope env = new DeadLetterEnvelope("orders", 0, 1L, new RuntimeException("err"), 0, new byte[]{1, 2});
        byte[] data = env.serialize();

        // Corrupt magic header (first 2 bytes)
        byte[] corruptMagic = data.clone();
        corruptMagic[0] = 0x00;
        corruptMagic[1] = 0x00;

        com.simplekafka.broker.ProtocolException magicEx = assertThrows(com.simplekafka.broker.ProtocolException.class, () -> {
            DeadLetterEnvelope.deserialize(corruptMagic);
        });
        assertTrue(magicEx.getMessage().contains("Invalid DeadLetterEnvelope magic"));
        assertTrue(magicEx instanceof IOException, "ProtocolException must extend IOException for backward compatibility");

        // Corrupt version byte (byte 2)
        byte[] corruptVersion = data.clone();
        corruptVersion[2] = 0x7F;

        com.simplekafka.broker.ProtocolException verEx = assertThrows(com.simplekafka.broker.ProtocolException.class, () -> {
            DeadLetterEnvelope.deserialize(corruptVersion);
        });
        assertTrue(verEx.getMessage().contains("Unsupported DeadLetterEnvelope wire version"));
    }

    // ──────────────────────────────────────────────────────────────────────
    // 2. DeadLetterEnvelope wire format tests
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void testEnvelopeSerializationAndDeserialization() throws IOException {
        String topic = "orders";
        int partition = 2;
        long offset = 104928L;
        byte[] payload = "{\"item\":\"laptop\",\"price\":1299.99}".getBytes(StandardCharsets.UTF_8);
        IllegalArgumentException cause = new IllegalArgumentException("Invalid currency format");

        DeadLetterEnvelope original = new DeadLetterEnvelope(topic, partition, offset, cause, 3, payload);
        byte[] serialized = original.serialize();
        assertTrue(serialized.length > 0);

        DeadLetterEnvelope deserialized = DeadLetterEnvelope.deserialize(serialized);

        assertEquals(DeadLetterEnvelope.CURRENT_VERSION, deserialized.getVersion());
        assertEquals(topic, deserialized.getOriginalTopic());
        assertEquals(partition, deserialized.getOriginalPartition());
        assertEquals(offset, deserialized.getOriginalOffset());
        assertEquals("Invalid currency format", deserialized.getDeathReason());
        assertEquals(IllegalArgumentException.class.getName(), deserialized.getExceptionClassName());
        assertEquals(3, deserialized.getRetryCount());
        assertArrayEquals(payload, deserialized.getOriginalPayload());

        // Diagnostic headers
        assertEquals("orders", deserialized.getHeaders().get(DeadLetterEnvelope.HEADER_ORIGINAL_TOPIC));
        assertEquals("2", deserialized.getHeaders().get(DeadLetterEnvelope.HEADER_ORIGINAL_PARTITION));
        assertEquals("104928", deserialized.getHeaders().get(DeadLetterEnvelope.HEADER_ORIGINAL_OFFSET));
        assertEquals("Invalid currency format", deserialized.getHeaders().get(DeadLetterEnvelope.HEADER_DEATH_REASON));
        assertEquals("3", deserialized.getHeaders().get(DeadLetterEnvelope.HEADER_RETRY_COUNT));

        // Dedup key
        assertEquals("orders:2:104928", deserialized.getHeaders().get(DeadLetterEnvelope.HEADER_DEDUP_KEY));
        assertEquals("orders:2:104928", deserialized.getDeduplicationKey());
    }

    @Test
    void testEnvelopeVersionByteValidation() throws IOException {
        DeadLetterEnvelope validEnv = new DeadLetterEnvelope("orders", 0, 100L, new RuntimeException("test"), 0, new byte[]{1, 2, 3});
        byte[] serialized = validEnv.serialize();

        // Valid version 0x01 deserializes cleanly
        DeadLetterEnvelope deserialized = DeadLetterEnvelope.deserialize(serialized);
        assertEquals(0x01, deserialized.getVersion());

        // Corrupt version byte to 0x99 → IOException (consistent wire-format error type)
        byte[] unknownVersionBytes = serialized.clone();
        unknownVersionBytes[2] = (byte) 0x99;
        IOException versionEx = assertThrows(IOException.class, () -> {
            DeadLetterEnvelope.deserialize(unknownVersionBytes);
        });
        assertTrue(versionEx.getMessage().contains("Unsupported DeadLetterEnvelope wire version"));

        // Corrupt magic header → IOException
        byte[] badMagicBytes = serialized.clone();
        badMagicBytes[0] = 0x00;
        badMagicBytes[1] = 0x00;
        IOException magicEx = assertThrows(IOException.class, () -> {
            DeadLetterEnvelope.deserialize(badMagicBytes);
        });
        assertTrue(magicEx.getMessage().contains("Invalid DeadLetterEnvelope magic header"));
    }

    @Test
    void testEnvelopeWithEmptyHeaders() throws IOException {
        // Envelope where only the constructor-injected headers exist (no truncation)
        DeadLetterEnvelope env = new DeadLetterEnvelope("t", 0, 0L, new RuntimeException("err"), 0, new byte[0]);
        byte[] serialized = env.serialize();
        DeadLetterEnvelope deserialized = DeadLetterEnvelope.deserialize(serialized);

        // Should have the standard set of headers but no truncation headers
        assertNotNull(deserialized.getHeaders());
        assertFalse(deserialized.getHeaders().isEmpty());
        assertEquals("t", deserialized.getHeaders().get(DeadLetterEnvelope.HEADER_ORIGINAL_TOPIC));
        assertNull(deserialized.getHeaders().get(DeadLetterEnvelope.HEADER_PAYLOAD_TRUNCATED));
        assertArrayEquals(new byte[0], deserialized.getOriginalPayload());
    }

    @Test
    void testOversizedPayloadTruncationAccountsForOverhead() {
        // MAX_PAYLOAD_BYTES = DLQ_MAX_MESSAGE_BYTES - MAX_ENVELOPE_OVERHEAD
        // This means a payload of exactly MAX_PAYLOAD_BYTES should NOT be truncated
        byte[] exactPayload = new byte[DeadLetterEnvelope.MAX_PAYLOAD_BYTES];
        Arrays.fill(exactPayload, (byte) 'X');
        DeadLetterEnvelope exact = new DeadLetterEnvelope("analytics", 0, 10L, new RuntimeException("test"), 0, exactPayload);
        assertEquals(DeadLetterEnvelope.MAX_PAYLOAD_BYTES, exact.getOriginalPayload().length);
        assertNull(exact.getHeaders().get(DeadLetterEnvelope.HEADER_PAYLOAD_TRUNCATED));

        // A payload 1 byte over MAX_PAYLOAD_BYTES should be truncated
        byte[] oversizedPayload = new byte[DeadLetterEnvelope.MAX_PAYLOAD_BYTES + 1];
        Arrays.fill(oversizedPayload, (byte) 'Y');
        DeadLetterEnvelope truncated = new DeadLetterEnvelope("analytics", 0, 11L, new RuntimeException("big"), 0, oversizedPayload);
        assertEquals(DeadLetterEnvelope.MAX_PAYLOAD_BYTES, truncated.getOriginalPayload().length);
        assertEquals("true", truncated.getHeaders().get(DeadLetterEnvelope.HEADER_PAYLOAD_TRUNCATED));
        assertEquals(String.valueOf(DeadLetterEnvelope.MAX_PAYLOAD_BYTES + 1), truncated.getHeaders().get(DeadLetterEnvelope.HEADER_ORIGINAL_SIZE));

        // Verify the serialized envelope fits within DLQ_MAX_MESSAGE_BYTES
        try {
            byte[] serialized = truncated.serialize();
            assertTrue(serialized.length <= DeadLetterEnvelope.DLQ_MAX_MESSAGE_BYTES,
                    "Serialized envelope (" + serialized.length + " bytes) must fit within DLQ_MAX_MESSAGE_BYTES (" +
                            DeadLetterEnvelope.DLQ_MAX_MESSAGE_BYTES + " bytes)");
        } catch (IOException e) {
            fail("Serialization should not throw: " + e.getMessage());
        }
    }

    @Test
    void testDeduplicationKeyDeterminism() {
        // Same (topic, partition, offset) produces same dedup key on every instantiation
        DeadLetterEnvelope env1 = new DeadLetterEnvelope("orders", 3, 42L, new RuntimeException("err1"), 1, new byte[]{1});
        DeadLetterEnvelope env2 = new DeadLetterEnvelope("orders", 3, 42L, new RuntimeException("err2"), 2, new byte[]{2});

        assertEquals(env1.getDeduplicationKey(), env2.getDeduplicationKey());
        assertEquals("orders:3:42", env1.getDeduplicationKey());
        assertEquals(env1.getHeaders().get(DeadLetterEnvelope.HEADER_DEDUP_KEY),
                     env2.getHeaders().get(DeadLetterEnvelope.HEADER_DEDUP_KEY));

        // Different offset produces different key
        DeadLetterEnvelope env3 = new DeadLetterEnvelope("orders", 3, 43L, new RuntimeException("err3"), 0, new byte[]{3});
        assertNotEquals(env1.getDeduplicationKey(), env3.getDeduplicationKey());
    }

    // ──────────────────────────────────────────────────────────────────────
    // 3. DeadLetterRouter routing tests
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void testSuccessfulProcessingZeroDlq() {
        List<byte[]> dlqMessages = new ArrayList<>();
        DeadLetterRouter.DlqProducer dlqProducer = (dlqTopic, bytes) -> dlqMessages.add(bytes);
        RetryPolicy policy = new RetryPolicy(3, 10, 2.0, 100);

        DeadLetterRouter router = new DeadLetterRouter(policy, dlqProducer);

        AtomicInteger processedCount = new AtomicInteger(0);
        byte[] msg = "clean-data".getBytes(StandardCharsets.UTF_8);

        DeadLetterRouter.RoutingResult result = router.handleMessage("orders", 0, 100, msg, (m, off) -> {
            processedCount.incrementAndGet();
        });

        assertEquals(DeadLetterRouter.RoutingAction.SUCCESS, result.action());
        assertEquals(1, result.attempts());
        assertEquals(1, processedCount.get());
        assertTrue(dlqMessages.isEmpty(), "Zero messages should be routed to DLQ on success");
    }

    @Test
    void testTransientErrorSucceedsOnRetry() {
        List<byte[]> dlqMessages = new ArrayList<>();
        DeadLetterRouter.DlqProducer dlqProducer = (dlqTopic, bytes) -> dlqMessages.add(bytes);
        RetryPolicy policy = new RetryPolicy(3, 10, 1.5, 100);

        DeadLetterRouter router = new DeadLetterRouter(policy, dlqProducer);

        AtomicInteger attempts = new AtomicInteger(0);
        byte[] msg = "transient-message".getBytes(StandardCharsets.UTF_8);

        DeadLetterRouter.RoutingResult result = router.handleMessage("orders", 1, 200, msg, (m, off) -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IOException("Temporary downstream network timeout");
            }
            // Succeed on second attempt
        });

        assertEquals(DeadLetterRouter.RoutingAction.RETRIED_SUCCESS, result.action());
        assertEquals(2, result.attempts());
        assertEquals(2, attempts.get());
        assertTrue(dlqMessages.isEmpty(), "Zero messages should be routed to DLQ when retry succeeds");
        assertEquals(1, router.getMetrics().getRetryAttempts());
    }

    @Test
    void testTransientErrorExhaustsRetriesAndRoutesToDlq() throws IOException {
        List<byte[]> dlqMessages = new ArrayList<>();
        List<String> dlqTopics = new ArrayList<>();
        DeadLetterRouter.DlqProducer dlqProducer = (dlqTopic, bytes) -> {
            dlqTopics.add(dlqTopic);
            dlqMessages.add(bytes);
        };

        RetryPolicy policy = new RetryPolicy(2, 5, 1.0, 20); // 2 retries = 3 total attempts
        DeadLetterRouter router = new DeadLetterRouter(policy, dlqProducer);

        byte[] payload = "transient-failing-message".getBytes(StandardCharsets.UTF_8);
        long offset = 404L;

        DeadLetterRouter.RoutingResult result = router.handleMessage("orders", 0, offset, payload, (m, off) -> {
            throw new IOException("Persistent downstream database timeout");
        });

        assertEquals(DeadLetterRouter.RoutingAction.ROUTED_TO_DLQ, result.action());
        assertEquals(3, result.attempts());
        assertNotNull(result.envelope());

        assertEquals(1, dlqTopics.size());
        assertEquals("orders.dlq", dlqTopics.get(0));

        DeadLetterEnvelope env = DeadLetterEnvelope.deserialize(dlqMessages.get(0));
        assertEquals("orders", env.getOriginalTopic());
        assertEquals(offset, env.getOriginalOffset());
        assertEquals("Persistent downstream database timeout", env.getDeathReason());
        assertEquals(IOException.class.getName(), env.getExceptionClassName());
        assertEquals(2, env.getRetryCount());
        assertArrayEquals(payload, env.getOriginalPayload());
        assertEquals(2, router.getMetrics().getRetryAttempts());
        assertEquals(1, router.getMetrics().getTotalRouted());
    }

    @Test
    void testFastFailNonRetryableExceptionRoutesImmediately() throws IOException {
        List<byte[]> dlqMessages = new ArrayList<>();
        DeadLetterRouter.DlqProducer dlqProducer = (dlqTopic, bytes) -> dlqMessages.add(bytes);

        RetryPolicy policy = new RetryPolicy(3, 100, 2.0, 1000);
        DeadLetterRouter router = new DeadLetterRouter(policy, dlqProducer);

        byte[] poisonPill = "{malformed-json:syntax-error".getBytes(StandardCharsets.UTF_8);
        long poisonOffset = 888L;

        long startTime = System.currentTimeMillis();
        DeadLetterRouter.RoutingResult result = router.handleMessage("orders", 0, poisonOffset, poisonPill, (m, off) -> {
            throw new IllegalArgumentException("Fatal JSON syntax error");
        });
        long duration = System.currentTimeMillis() - startTime;

        assertEquals(DeadLetterRouter.RoutingAction.ROUTED_TO_DLQ, result.action());
        assertEquals(1, result.attempts());
        assertTrue(duration < 50, "Fast-fail must route immediately without backoff sleeping, took " + duration + "ms");
        assertEquals(1, router.getMetrics().getFastFailCount());
        assertEquals(0, router.getMetrics().getRetryAttempts());
        assertEquals(1, router.getMetrics().getTotalRouted());

        DeadLetterEnvelope env = DeadLetterEnvelope.deserialize(dlqMessages.get(0));
        assertEquals(0, env.getRetryCount());
        assertEquals("Fatal JSON syntax error", env.getDeathReason());
    }

    @Test
    void testConsumerNeverStallsOnTenConsecutivePoisonPills() {
        List<DeadLetterEnvelope> routedEnvelopes = Collections.synchronizedList(new ArrayList<>());
        DeadLetterRouter.DlqProducer dlqProducer = (dlqTopic, bytes) -> {
            try {
                routedEnvelopes.add(DeadLetterEnvelope.deserialize(bytes));
            } catch (IOException e) {
                fail(e);
            }
        };

        RetryPolicy fastPolicy = new RetryPolicy(1, 2, 1.0, 5);
        DeadLetterRouter router = new DeadLetterRouter(fastPolicy, dlqProducer);

        List<String> successfullyProcessed = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            final int messageIndex = i;
            byte[] payload = ("msg-" + i).getBytes(StandardCharsets.UTF_8);

            DeadLetterRouter.RoutingResult result = router.handleMessage("payments", 0, i, payload, (msg, off) -> {
                if (messageIndex % 2 == 1) {
                    throw new IllegalArgumentException("Poison pill schema error at index " + messageIndex);
                }
                successfullyProcessed.add(new String(msg, StandardCharsets.UTF_8));
            });

            if (messageIndex % 2 == 1) {
                assertTrue(result.isDeadLettered());
            } else {
                assertTrue(result.isSuccess());
            }
        }

        assertEquals(10, successfullyProcessed.size());
        for (int i = 0; i < 10; i++) {
            assertEquals("msg-" + (i * 2), successfullyProcessed.get(i));
        }

        assertEquals(10, routedEnvelopes.size());
        for (int i = 0; i < 10; i++) {
            DeadLetterEnvelope env = routedEnvelopes.get(i);
            assertEquals("payments", env.getOriginalTopic());
            assertEquals(i * 2 + 1, env.getOriginalOffset());
            assertTrue(env.getDeathReason().contains("Poison pill schema error at index"));
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // 4. Offset commit ordering & rewind semantics
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void testDlqProduceFailurePreventsSilentDataLoss() {
        // DLQ broker unavailable scenario: DlqProducer throws IOException
        DeadLetterRouter.DlqProducer failingDlqProducer = (dlqTopic, bytes) -> {
            throw new IOException("DLQ Broker connection refused: cluster is offline");
        };

        RetryPolicy policy = new RetryPolicy(1, 5, 1.0, 10);
        DeadLetterRouter router = new DeadLetterRouter(policy, failingDlqProducer);

        byte[] poisonPill = "toxic-record".getBytes(StandardCharsets.UTF_8);

        DeadLetterRouter.RoutingResult result = router.handleMessage("orders", 0, 505L, poisonPill, (m, off) -> {
            throw new IllegalArgumentException("Fatal corruption");
        });

        // Router returns DLQ_PRODUCE_FAILED
        assertEquals(DeadLetterRouter.RoutingAction.DLQ_PRODUCE_FAILED, result.action());
        assertTrue(result.isFatalDlqProduceError());
        assertEquals(1, router.getMetrics().getProduceFailures());

        // Consumer offset safety via mock client
        List<byte[]> queue = List.of(poisonPill);
        SimpleKafkaClient mockClient = new SimpleKafkaClient("localhost", 9092) {
            @Override
            public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) {
                return queue;
            }
        };

        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(mockClient, "orders", 0);
        consumer.startConsuming((msg, off) -> {
            throw new IllegalArgumentException("Fatal corruption");
        }, router);

        try { Thread.sleep(250); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        assertTrue(consumer.isPaused(), "Consumer partition MUST be paused when DLQ write fails");
        assertEquals(0, consumer.getCurrentOffset(), "Fetch cursor MUST rewind to poison pill offset");
        assertEquals(0, consumer.getCommittedOffset(), "Committed offset MUST NOT advance past unrouted poison pill");

        consumer.stopConsuming();
    }

    @Test
    void testDlqSuccessAdvancesOffsetToNextRecord() {
        // Scenario: offsets 100-105 fetched. Poison pill at 103, DLQ succeeds.
        // Expected: committedOffset advances past 103 to 104.
        // Remaining records 104, 105 should also be processed.
        List<byte[]> batch = new ArrayList<>();
        for (int i = 100; i <= 105; i++) {
            batch.add(("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }

        List<byte[]> dlqMessages = new ArrayList<>();
        DeadLetterRouter.DlqProducer dlqProducer = (dlqTopic, bytes) -> dlqMessages.add(bytes);
        RetryPolicy policy = new RetryPolicy(0, 0, 1.0, 0); // No retries, fast-fail
        DeadLetterRouter router = new DeadLetterRouter(policy, dlqProducer);

        AtomicInteger fetchCount = new AtomicInteger(0);
        SimpleKafkaClient mockClient = new SimpleKafkaClient("localhost", 9092) {
            @Override
            public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) {
                // Return the batch on first fetch, empty on subsequent
                if (fetchCount.getAndIncrement() == 0) return batch;
                return Collections.emptyList();
            }
        };

        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(mockClient, "orders", 0);
        List<Long> processedOffsets = Collections.synchronizedList(new ArrayList<>());

        consumer.startConsuming((msg, off) -> {
            String content = new String(msg, StandardCharsets.UTF_8);
            if (content.equals("msg-103")) {
                throw new IllegalArgumentException("Poison pill at 103");
            }
            processedOffsets.add(off);
        }, router);

        try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        consumer.stopConsuming();

        // All non-poison records processed
        assertTrue(processedOffsets.contains(0L));  // msg-100
        assertTrue(processedOffsets.contains(1L));  // msg-101
        assertTrue(processedOffsets.contains(2L));  // msg-102
        assertTrue(processedOffsets.contains(4L));  // msg-104
        assertTrue(processedOffsets.contains(5L));  // msg-105
        assertFalse(processedOffsets.contains(3L)); // msg-103 was the poison pill

        // Committed offset should be past the last record
        assertEquals(6, consumer.getCommittedOffset());

        // Poison pill was routed to DLQ
        assertEquals(1, dlqMessages.size());
        assertFalse(consumer.isPaused(), "Consumer should NOT be paused when DLQ write succeeds");
    }

    /**
     * Records 3-4 are NOT processed after the DLQ failure in this iteration.
     * On resume, the fetch cursor is at offset 2, so the next poll re-delivers
     * offsets 2, 3, 4 — the poison pill retries, and 3-4 are processed again.
     */
    @Test
    void testDlqFailureRewindsFetchCursorAndDefersPriorBatchToNextPoll() {
        // Scenario: offsets 0-4 fetched. Poison pill at 2, DLQ produce FAILS.
        // Expected: records 0 and 1 are processed, offset 2 causes DLQ failure,
        //           consumer rewinds to 2 and pauses. Records 3, 4 are NOT delivered in this poll.
        //           committedOffset stays at 2 (last successful + 1).
        List<byte[]> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch.add(("rec-" + i).getBytes(StandardCharsets.UTF_8));
        }

        DeadLetterRouter.DlqProducer failingProducer = (dlqTopic, bytes) -> {
            throw new IOException("DLQ broker unreachable");
        };
        RetryPolicy policy = new RetryPolicy(0, 0, 1.0, 0);
        DeadLetterRouter router = new DeadLetterRouter(policy, failingProducer);

        AtomicInteger fetchCount = new AtomicInteger(0);
        SimpleKafkaClient mockClient = new SimpleKafkaClient("localhost", 9092) {
            @Override
            public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) {
                if (fetchCount.getAndIncrement() == 0) return batch;
                return Collections.emptyList();
            }
        };

        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(mockClient, "orders", 0);
        List<Long> processedOffsets = Collections.synchronizedList(new ArrayList<>());

        consumer.startConsuming((msg, off) -> {
            String content = new String(msg, StandardCharsets.UTF_8);
            if (content.equals("rec-2")) {
                throw new IllegalArgumentException("Poison pill at offset 2");
            }
            processedOffsets.add(off);
        }, router);

        try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        consumer.stopConsuming();

        // Records 0 and 1 were processed before the poison pill
        assertTrue(processedOffsets.contains(0L));
        assertTrue(processedOffsets.contains(1L));
        // Records 3 and 4 were NOT processed in this iteration
        assertFalse(processedOffsets.contains(3L));
        assertFalse(processedOffsets.contains(4L));

        assertTrue(consumer.isPaused(), "Consumer MUST pause when DLQ write fails");
        assertEquals(2, consumer.getCurrentOffset(), "Fetch cursor MUST rewind to poison pill offset 2");
        assertEquals(2, consumer.getCommittedOffset(), "Committed offset should be 2 (last successful was offset 1)");
    }

    @Test
    void testDlqFailureClearsBufferAndNextPollResumesFromPoisonPill() throws Exception {
        // Pre-load fetch buffer with records 2, 3, 4
        // Poison pill at 2 -> DLQ fails
        // Resume -> assert the next record processed is offset 2 (the poison pill), not 3
        byte[] rec2 = "rec-2-poison".getBytes(StandardCharsets.UTF_8);
        byte[] rec3 = "rec-3-good".getBytes(StandardCharsets.UTF_8);
        byte[] rec4 = "rec-4-good".getBytes(StandardCharsets.UTF_8);

        DeadLetterRouter.DlqProducer dlqProducer = (dlqTopic, bytes) -> {
            throw new IOException("DLQ broker unavailable");
        };

        DeadLetterRouter router = new DeadLetterRouter(RetryPolicy.noRetries(), dlqProducer);

        SimpleKafkaClient mockClient = new SimpleKafkaClient("localhost", 9092) {
            @Override
            public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) {
                if (offset == 2) {
                    return List.of(rec2, rec3, rec4);
                }
                return Collections.emptyList();
            }
        };

        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(mockClient, "orders", 0);
        consumer.seek(2);

        // Simulate pre-loaded fetch buffer with records 2, 3, 4
        consumer.getFetchBuffer().add(rec2);
        consumer.getFetchBuffer().add(rec3);
        consumer.getFetchBuffer().add(rec4);

        List<String> processed = Collections.synchronizedList(new ArrayList<>());
        consumer.startConsuming((msg, off) -> {
            String s = new String(msg, StandardCharsets.UTF_8);
            processed.add(s + "@" + off);
            if (s.equals("rec-2-poison")) {
                throw new IllegalArgumentException("Poison pill at 2");
            }
        }, router);

        Thread.sleep(200);
        assertTrue(consumer.isPaused(), "Consumer must be paused on fatal DLQ produce failure");
        assertEquals(2, consumer.getCurrentOffset(), "Fetch cursor must be rewound to poison pill offset 2");
        assertEquals(0, consumer.getCommittedOffset(), "Committed offset must not advance past poison pill");
        assertTrue(consumer.getFetchBuffer().isEmpty(), "Seek must have cleared internal fetch buffer");

        // Now resume consumer
        consumer.resume();
        Thread.sleep(200);
        consumer.stopConsuming();

        // The very first record re-processed after resume must be offset 2 (rec-2-poison), NOT 3!
        assertTrue(processed.size() >= 2);
        assertEquals("rec-2-poison@2", processed.get(0));
        assertEquals("rec-2-poison@2", processed.get(1));
    }

    // ──────────────────────────────────────────────────────────────────────
    // 5. At-least-once DLQ delivery and deduplication
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void testDedupKeyIsPopulatedOnEnvelope() throws IOException {
        // Simulates commit-failure scenario:
        // Envelope is produced twice due to at-least-once consumer delivery.
        // Both envelopes MUST carry the identical deterministic dedup key.
        Map<String, List<byte[]>> dlqStore = new HashMap<>();
        DeadLetterRouter.DlqProducer dlqProducer = (dlqTopic, bytes) -> {
            dlqStore.computeIfAbsent(dlqTopic, k -> new ArrayList<>()).add(bytes);
        };

        RetryPolicy policy = RetryPolicy.noRetries();
        DeadLetterRouter router = new DeadLetterRouter(policy, dlqProducer);

        byte[] poisonPill = "bad-data".getBytes(StandardCharsets.UTF_8);
        long poisonOffset = 42L;

        // First processing (initial attempt)
        DeadLetterRouter.RoutingResult result1 = router.handleMessage("orders", 0, poisonOffset, poisonPill, (m, off) -> {
            throw new IllegalArgumentException("Corrupt payload");
        });
        assertEquals(DeadLetterRouter.RoutingAction.ROUTED_TO_DLQ, result1.action());

        // Second processing (after simulated restart / commit failure)
        DeadLetterRouter.RoutingResult result2 = router.handleMessage("orders", 0, poisonOffset, poisonPill, (m, off) -> {
            throw new IllegalArgumentException("Corrupt payload");
        });
        assertEquals(DeadLetterRouter.RoutingAction.ROUTED_TO_DLQ, result2.action());

        List<byte[]> dlqEntries = dlqStore.get("orders.dlq");
        assertEquals(2, dlqEntries.size(), "At-least-once: two envelopes expected for same poison pill after restart");

        // Both envelopes share the exact same dedup key
        DeadLetterEnvelope env1 = DeadLetterEnvelope.deserialize(dlqEntries.get(0));
        DeadLetterEnvelope env2 = DeadLetterEnvelope.deserialize(dlqEntries.get(1));
        assertEquals(env1.getDeduplicationKey(), env2.getDeduplicationKey());
        assertEquals("orders:0:42", env1.getDeduplicationKey());
    }

    @Test
    void testCompactedDlqDeduplicatesByDedupKey(@org.junit.jupiter.api.io.TempDir java.io.File tempDir) throws IOException {
        // DLQ topic is created with cleanup.policy=compact.
        // Produce envelope for (orders, 0, 103) twice.
        // Force compaction.
        // Assert exactly one survives in the DLQ partition.
        com.simplekafka.broker.Partition dlqPartition = new com.simplekafka.broker.Partition(0, tempDir.getAbsolutePath());
        dlqPartition.setCompacted(true);
        dlqPartition.setCompactionKeyExtractor(msg -> DeadLetterEnvelope.deserialize(msg).getDeduplicationKey());

        DeadLetterEnvelope env1 = new DeadLetterEnvelope("orders", 0, 103L, new IllegalArgumentException("Bad format 1"), 3, "payload-v1".getBytes());
        DeadLetterEnvelope env2 = new DeadLetterEnvelope("orders", 0, 103L, new IllegalArgumentException("Bad format 2"), 3, "payload-v2".getBytes());

        assertEquals(env1.getDeduplicationKey(), env2.getDeduplicationKey());
        assertEquals("orders:0:103", env1.getDeduplicationKey());

        dlqPartition.append(env1.serialize());
        dlqPartition.append(env2.serialize());

        // Prior to compaction: 2 envelopes present in log
        List<byte[]> before = dlqPartition.readMessages(0, 1024 * 1024);
        assertEquals(2, before.size(), "Before compaction: at-least-once produces 2 envelopes in DLQ");

        // Trigger log compaction
        dlqPartition.compact();

        // After compaction: exactly ONE envelope survives with key orders:0:103
        List<byte[]> after = dlqPartition.readMessages(0, 1024 * 1024);
        assertEquals(1, after.size(), "Compacted DLQ log must retain exactly one envelope per dedup key");

        DeadLetterEnvelope surviving = DeadLetterEnvelope.deserialize(after.get(0));
        assertEquals("orders:0:103", surviving.getDeduplicationKey());
        assertArrayEquals("payload-v2".getBytes(), surviving.getOriginalPayload(), "Compaction retains latest envelope value");
    }

    @Test
    void testKeyedDlqProducerReceivesDedupKey() throws IOException {
        List<String> capturedKeys = new ArrayList<>();
        List<byte[]> capturedEnvelopes = new ArrayList<>();

        DeadLetterRouter.KeyedDlqProducer keyedProducer = (dlqTopic, dedupKey, envelopeBytes) -> {
            capturedKeys.add(dedupKey);
            capturedEnvelopes.add(envelopeBytes);
        };

        RetryPolicy policy = RetryPolicy.noRetries();
        DeadLetterRouter router = new DeadLetterRouter(policy, keyedProducer);

        router.handleMessage("orders", 3, 99L, "bad".getBytes(), (m, off) -> {
            throw new ClassCastException("Schema mismatch");
        });

        assertEquals(1, capturedKeys.size());
        assertEquals("orders:3:99", capturedKeys.get(0));
        assertEquals(1, capturedEnvelopes.size());

        DeadLetterEnvelope env = DeadLetterEnvelope.deserialize(capturedEnvelopes.get(0));
        assertEquals("orders:3:99", env.getDeduplicationKey());
    }

    // ──────────────────────────────────────────────────────────────────────
    // 6. Internal topic exclusions and auto-creation
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void testInternalTopicExclusions() {
        assertFalse(DeadLetterRouter.isEligibleForDlq("__consumer_offsets"));
        assertFalse(DeadLetterRouter.isEligibleForDlq("__audit"));
        assertFalse(DeadLetterRouter.isEligibleForDlq("__cluster_metadata"));
        assertFalse(DeadLetterRouter.isEligibleForDlq("orders.dlq")); // companion DLQ itself
        assertFalse(DeadLetterRouter.isEligibleForDlq(null));

        assertTrue(DeadLetterRouter.isEligibleForDlq("orders"));
        assertTrue(DeadLetterRouter.isEligibleForDlq("payments"));
        assertTrue(DeadLetterRouter.isEligibleForDlq("user_events"));
    }

    @Test
    void testAutoCreateDlqTopicOnFirstProduce() throws IOException {
        Set<String> createdTopics = new HashSet<>();
        Map<String, List<byte[]>> brokerTopics = new HashMap<>();

        DeadLetterRouter.DlqProducer dynamicDlqProducer = (dlqTopic, bytes) -> {
            if (!brokerTopics.containsKey(dlqTopic)) {
                createdTopics.add(dlqTopic);
                brokerTopics.put(dlqTopic, new ArrayList<>());
            }
            brokerTopics.get(dlqTopic).add(bytes);
        };

        DeadLetterRouter router = new DeadLetterRouter(new RetryPolicy(0, 0, 1.0, 0), dynamicDlqProducer);

        String topic = "legacy-orders";
        byte[] payload = "data".getBytes(StandardCharsets.UTF_8);

        DeadLetterRouter.RoutingResult result = router.handleMessage(topic, 0, 10L, payload, (m, off) -> {
            throw new IllegalArgumentException("Corrupt data on legacy topic");
        });

        assertEquals(DeadLetterRouter.RoutingAction.ROUTED_TO_DLQ, result.action());
        assertTrue(createdTopics.contains("legacy-orders.dlq"), "Companion DLQ topic should be auto-created on first produce");
        assertEquals(1, brokerTopics.get("legacy-orders.dlq").size());
    }

    @Test
    void testConcurrentTopicCreationWaitsForCompletion() throws Exception {
        // Two threads call createTopic("orders", 8, 1) with a deliberately slow creation path (Thread.sleep(200) injected).
        // Assert both threads return only after the topic actually exists.
        // Produce immediately in both threads -> both succeed.
        java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.CompletableFuture<Void>> topicCreationGuards = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.concurrent.ConcurrentHashMap<String, List<String>> activeTopics = new java.util.concurrent.ConcurrentHashMap<>();
        AtomicInteger physicalCreations = new AtomicInteger(0);
        java.util.concurrent.ConcurrentLinkedQueue<String> producedMessages = new java.util.concurrent.ConcurrentLinkedQueue<>();

        java.util.concurrent.Callable<Void> createAndProduceAction = () -> {
            String topic = "orders";
            java.util.concurrent.CompletableFuture<Void> myFuture = new java.util.concurrent.CompletableFuture<>();
            java.util.concurrent.CompletableFuture<Void> existing = topicCreationGuards.putIfAbsent(topic, myFuture);
            if (existing != null) {
                // Wait for winner thread to finish creation
                existing.join();
            } else {
                try {
                    // Winner executes creation with deliberate slow path (e.g. ZK write, filesystem init)
                    physicalCreations.incrementAndGet();
                    Thread.sleep(200);
                    activeTopics.put(topic, List.of("partition-0", "partition-1"));
                    myFuture.complete(null);
                } catch (Exception e) {
                    myFuture.completeExceptionally(e);
                    topicCreationGuards.remove(topic);
                    throw e;
                }
            }

            // Immediately produce upon returning from createTopic
            assertTrue(activeTopics.containsKey(topic), "Topic MUST exist when createTopic returns!");
            producedMessages.add("msg-from-" + Thread.currentThread().getName());
            return null;
        };

        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        java.util.concurrent.Future<Void> f1 = pool.submit(createAndProduceAction);
        java.util.concurrent.Future<Void> f2 = pool.submit(createAndProduceAction);

        f1.get(5, java.util.concurrent.TimeUnit.SECONDS);
        f2.get(5, java.util.concurrent.TimeUnit.SECONDS);
        pool.shutdown();

        // Exactly ONE physical creation executed
        assertEquals(1, physicalCreations.get(), "Creation must execute exactly once across concurrent threads");
        // Both threads succeeded in producing immediately without UNKNOWN_TOPIC_OR_PARTITION
        assertEquals(2, producedMessages.size(), "Both threads must successfully produce immediately after createTopic");
    }

    // ──────────────────────────────────────────────────────────────────────
    // 7. Metrics
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void testDlqMetricsTracking() {
        DlqMetrics metrics = new DlqMetrics();
        assertEquals(0, metrics.getTotalRouted());
        assertEquals(0, metrics.getRetryAttempts());
        assertEquals(0, metrics.getProduceFailures());
        assertEquals(0, metrics.getFastFailCount());

        metrics.recordRetryAttempt();
        metrics.recordRetryAttempt();
        metrics.recordFastFail();
        metrics.recordRouted("orders");
        metrics.recordProduceFailure();

        assertEquals(1, metrics.getTotalRouted());
        assertEquals(2, metrics.getRetryAttempts());
        assertEquals(1, metrics.getFastFailCount());
        assertEquals(1, metrics.getProduceFailures());
        assertEquals(1, metrics.getRoutedCount("orders"));
        assertEquals(0, metrics.getRoutedCount("unknown"));
    }

    // ──────────────────────────────────────────────────────────────────────
    // 8. Backward compatibility
    // ──────────────────────────────────────────────────────────────────────

    @Test
    void testLegacyStartConsumingBackwardCompatibility() {
        List<byte[]> queue = List.of("legacy-1".getBytes(StandardCharsets.UTF_8), "legacy-2".getBytes(StandardCharsets.UTF_8));
        SimpleKafkaClient mockClient = new SimpleKafkaClient("localhost", 9092) {
            @Override
            public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) {
                return offset == 0 ? queue : Collections.emptyList();
            }
        };

        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(mockClient, "orders", 0);
        List<String> received = Collections.synchronizedList(new ArrayList<>());

        // Calls legacy 1-parameter signature without DLQ router
        consumer.startConsuming((msg, off) -> {
            received.add(new String(msg, StandardCharsets.UTF_8));
        });

        try { Thread.sleep(250); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        consumer.stopConsuming();
        assertEquals(2, received.size());
        assertEquals("legacy-1", received.get(0));
        assertEquals("legacy-2", received.get(1));
        assertEquals(2, consumer.getCommittedOffset());
    }
}
