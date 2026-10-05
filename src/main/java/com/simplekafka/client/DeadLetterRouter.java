package com.simplekafka.client;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client-Side Dead Letter Queue (DLQ) router with exponential backoff retries,
 * fast-fail exception classification, and strict offset commit safety.
 *
 * CRITICAL DELIVERY GUARANTEE:
 * - If routing to the DLQ succeeds, returns ROUTED_TO_DLQ, allowing the consumer offset to advance.
 * - If routing to the DLQ fails (e.g. broker unreachable), returns DLQ_PRODUCE_FAILED.
 *   The consumer MUST NOT commit the offset, pausing the partition instead to prevent silent data loss.
 *
 * BLOCKING-RETRY TRADEOFF & MITIGATIONS:
 * Retries execute synchronously on the consumer thread. With default settings (3 retries,
 * 100ms initial, 2x multiplier), a failing record blocks the thread for ~300ms + handler time.
 * In a partition with 1,000 poison pills, synchronous backoff could stall consumption for 5+ minutes,
 * causing consumer lag spikes and cascading group rebalances.
 *
 * To mitigate this:
 * 1. Fast-Fail Classification: Definitively fatal errors (DeserializationException, ClassCastException,
 *    IllegalArgumentException, schema violations) bypass retries completely (0ms backoff) and route
 *    directly to the DLQ on attempt 1.
 * 2. Partition Pause on DLQ Failure: When the DLQ broker itself is down, the consumer pauses the
 *    partition rather than dropping records or spinning indefinitely.
 * 3. Future Work (Async Retry Queue): For high-volume transient failures, parking failed records
 *    in an in-memory bounded scheduled retry executor keeps the main consumer loop hot.
 *
 * AT-LEAST-ONCE DELIVERY & DEDUPLICATION:
 * If routing to the DLQ succeeds but the subsequent consumer offset commit fails (e.g. consumer crash
 * or network partition), the re-started consumer will re-process the uncommitted poison pill and route
 * it to the DLQ a second time.
 * DLQ delivery semantics are therefore AT-LEAST-ONCE. To enable downstream idempotent processing or
 * log compaction, each envelope is stamped with a deterministic deduplication key
 * {@code X-Dedup-Key: <topic>:<partition>:<offset>} accessible via {@link DeadLetterEnvelope#getDeduplicationKey()}.
 */
public class DeadLetterRouter {
    private static final Logger LOGGER = Logger.getLogger(DeadLetterRouter.class.getName());

    private static final Set<String> INTERNAL_TOPIC_EXCLUSIONS = Set.of(
            "__consumer_offsets",
            "__audit",
            "__cluster_metadata"
    );

    @FunctionalInterface
    public interface DlqProducer {
        void sendToDlq(String dlqTopic, byte[] envelopeBytes) throws IOException;
    }

    @FunctionalInterface
    public interface KeyedDlqProducer extends DlqProducer {
        void sendToDlq(String dlqTopic, String dedupKey, byte[] envelopeBytes) throws IOException;

        @Override
        default void sendToDlq(String dlqTopic, byte[] envelopeBytes) throws IOException {
            sendToDlq(dlqTopic, null, envelopeBytes);
        }
    }

    @FunctionalInterface
    public interface CheckedMessageHandler {
        void handle(byte[] message, long offset) throws Exception;
    }

    public enum RoutingAction {
        SUCCESS,
        RETRIED_SUCCESS,
        ROUTED_TO_DLQ,
        DLQ_PRODUCE_FAILED
    }

    public record RoutingResult(
            RoutingAction action,
            int attempts,
            DeadLetterEnvelope envelope,
            Throwable lastError
    ) {
        public boolean isSuccess() {
            return action == RoutingAction.SUCCESS || action == RoutingAction.RETRIED_SUCCESS;
        }

        public boolean isDeadLettered() {
            return action == RoutingAction.ROUTED_TO_DLQ;
        }

        public boolean isFatalDlqProduceError() {
            return action == RoutingAction.DLQ_PRODUCE_FAILED;
        }
    }

    private final RetryPolicy retryPolicy;
    private final DlqProducer dlqProducer;
    private final DlqMetrics metrics;

    public DeadLetterRouter(RetryPolicy retryPolicy, DlqProducer dlqProducer) {
        this(retryPolicy, dlqProducer, new DlqMetrics());
    }

    public DeadLetterRouter(RetryPolicy retryPolicy, DlqProducer dlqProducer, DlqMetrics metrics) {
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy cannot be null");
        this.dlqProducer = Objects.requireNonNull(dlqProducer, "dlqProducer cannot be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
    }

    public static boolean isEligibleForDlq(String topic) {
        if (topic == null || topic.endsWith(".dlq")) {
            return false;
        }
        for (String exclusion : INTERNAL_TOPIC_EXCLUSIONS) {
            if (topic.equals(exclusion) || topic.startsWith("__")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Executes message processing with exponential backoff retries.
     *
     * FAST-FAIL OPTIMIZATION:
     * - If the exception is non-retryable (syntax error, bad deserialization, schema violation),
     *   retries are skipped immediately to prevent stalling consumer partition throughput.
     *
     * OFFSET ORDERING & FAILURE SAFETY:
     * - Returns ROUTED_TO_DLQ ONLY IF the DLQ write succeeds.
     * - Returns DLQ_PRODUCE_FAILED if publishing to DLQ throws, signalling the consumer to pause
     *   and NOT commit the offset.
     */
    public RoutingResult handleMessage(String topic, int partition, long offset, byte[] message, CheckedMessageHandler handler) {
        int attempts = 0;
        Throwable lastError = null;

        while (attempts <= retryPolicy.getMaxRetries()) {
            attempts++;
            try {
                handler.handle(message, offset);
                RoutingAction action = (attempts == 1) ? RoutingAction.SUCCESS : RoutingAction.RETRIED_SUCCESS;
                return new RoutingResult(action, attempts, null, null);
            } catch (Throwable t) {
                lastError = t;

                // Check if exception is fatal (non-retryable): fast-fail immediately
                if (!retryPolicy.isRetryable(t)) {
                    LOGGER.log(Level.WARNING, String.format(
                            "Fatal non-retryable exception at [%s-p%d@%d] (%s). Fast-failing immediately to DLQ without retries.",
                            topic, partition, offset, t.getClass().getSimpleName()), t);
                    metrics.recordFastFail();
                    break;
                }

                // If retryable and attempts remain, backoff and retry
                if (attempts <= retryPolicy.getMaxRetries()) {
                    metrics.recordRetryAttempt();
                    long backoffMs = retryPolicy.calculateBackoffMs(attempts);
                    LOGGER.log(Level.WARNING, String.format(
                            "Transient error processing msg at [%s-p%d@%d] (attempt %d/%d). Retrying in %d ms...",
                            topic, partition, offset, attempts, retryPolicy.getMaxRetries() + 1, backoffMs), t);
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        // Retries exhausted or fast-failed: construct DLQ envelope
        DeadLetterEnvelope envelope = new DeadLetterEnvelope(topic, partition, offset, lastError, attempts - 1, message);
        String dlqTopic = topic + ".dlq";

        LOGGER.log(Level.SEVERE, String.format(
                "Poison pill detected at [%s-p%d@%d] after %d attempts. Attempting delivery to DLQ [%s]: %s",
                topic, partition, offset, attempts, dlqTopic, envelope.getDeathReason()), lastError);

        try {
            byte[] envelopeBytes = envelope.serialize();
            if (dlqProducer instanceof KeyedDlqProducer keyedProducer) {
                keyedProducer.sendToDlq(dlqTopic, envelope.getDeduplicationKey(), envelopeBytes);
            } else {
                dlqProducer.sendToDlq(dlqTopic, envelopeBytes);
            }
            metrics.recordRouted(topic);
            return new RoutingResult(RoutingAction.ROUTED_TO_DLQ, attempts, envelope, lastError);
        } catch (Throwable dlqWriteError) {
            // DLQ produce failed! DO NOT commit offset. Signal fatal failure to caller.
            metrics.recordProduceFailure();
            LOGGER.log(Level.SEVERE, String.format(
                    "CRITICAL: Failed to publish poison pill to DLQ [%s]. Offset %d will NOT be committed to prevent silent data loss!",
                    dlqTopic, offset), dlqWriteError);
            return new RoutingResult(RoutingAction.DLQ_PRODUCE_FAILED, attempts, envelope, dlqWriteError);
        }
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public DlqMetrics getMetrics() {
        return metrics;
    }
}
