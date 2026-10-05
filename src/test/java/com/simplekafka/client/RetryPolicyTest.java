package com.simplekafka.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class RetryPolicyTest {

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
    @org.junit.jupiter.api.Tag("timing-sensitive")
    void testBackoffTimingMatchesPolicy() {
        // Uses real policy: 100ms initial, 2x multiplier, 2 retries
        // Expected wall-clock: attempt 1 fails → sleep 100ms → attempt 2 fails → sleep 200ms → attempt 3 fails → DLQ
        // Total sleep should be >= 290ms and < 500ms
        RetryPolicy policy = new RetryPolicy(2, 100, 2.0, 500);
        DeadLetterRouter router = new DeadLetterRouter(policy, (dlqTopic, bytes) -> {});

        byte[] msg = "timing-test".getBytes(StandardCharsets.UTF_8);
        AtomicInteger attempts = new AtomicInteger(0);

        long startMs = System.currentTimeMillis();
        DeadLetterRouter.RoutingResult result = router.handleMessage("orders", 0, 0, msg, (m, off) -> {
            attempts.incrementAndGet();
            throw new IOException("Transient downstream failure");
        });
        long elapsed = System.currentTimeMillis() - startMs;

        assertEquals(DeadLetterRouter.RoutingAction.ROUTED_TO_DLQ, result.action());
        assertEquals(3, result.attempts());
        // 100ms (after attempt 1) + 200ms (after attempt 2) = 300ms nominal
        assertTrue(elapsed >= 290, "Expected >= 290ms wall-clock for 2 retry backoffs, got " + elapsed + "ms");
        assertTrue(elapsed < 500, "Expected < 500ms, got " + elapsed + "ms");
    }
}
