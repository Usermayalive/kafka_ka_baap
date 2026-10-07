package com.simplekafka.client;

import com.simplekafka.network.KafkaFrame;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A producer that batches records per partition, Kafka-style. {@link #send} appends to the
 * partition's open batch and returns a future for the record's offset; a sender thread ships a
 * batch as one {@code PRODUCE_BATCH} request when it reaches {@code batchBytes} or has waited
 * {@code lingerMs}. Up to {@code maxInFlight} batches are outstanding at once.
 *
 * Retries: leadership changes and lost connections are retried (after a metadata refresh) until
 * {@code deliveryTimeoutMs}. Without idempotence a retried batch can be duplicated (at-least-once)
 * and batches can reorder. With {@code idempotent=true} each batch carries a sequence range fixed
 * when the batch is closed, the broker deduplicates retries by range, and each partition has one
 * batch in flight, so retries can't reorder: exactly-once per partition within a producer session.
 */
public class BatchingProducer implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(BatchingProducer.class.getName());

    public record Config(int batchBytes, long lingerMs, int maxInFlight, byte acks, boolean idempotent,
                         long deliveryTimeoutMs) {
        /** Kafka 4.x producer defaults: 16 KB batches, 5 ms linger, 5 in flight, acks=all. */
        public static Config defaults() {
            return new Config(16_384, 5, 5, KafkaFrame.ACKS_ALL, false, 30_000);
        }

        public Config withAcks(byte acks) {
            return new Config(batchBytes, lingerMs, maxInFlight, acks, idempotent, deliveryTimeoutMs);
        }

        public Config withIdempotence(boolean idempotent) {
            return new Config(batchBytes, lingerMs, maxInFlight, acks, idempotent, deliveryTimeoutMs);
        }

        public Config withBatching(int batchBytes, long lingerMs) {
            return new Config(batchBytes, lingerMs, maxInFlight, acks, idempotent, deliveryTimeoutMs);
        }
    }

    private record TopicPartition(String topic, int partition) {}

    private static final class Batch {
        final TopicPartition tp;
        final List<byte[]> records = new ArrayList<>();
        final List<CompletableFuture<Long>> futures = new ArrayList<>();
        final long createdNanos = System.nanoTime();
        long deadlineNanos;
        int baseSequence = -1;
        int bytes;

        Batch(TopicPartition tp) {
            this.tp = tp;
        }
    }

    /** Per-partition state: the open batch, closed batches waiting to ship, sequence numbers, in-flight flag. */
    private static final class Accumulator {
        Batch open;
        final ArrayDeque<Batch> ready = new ArrayDeque<>();
        int nextSequence;
        boolean batchInFlight;
        final ArrayDeque<Batch> retries = new ArrayDeque<>();
    }

    private final SimpleKafkaClient client;
    private final Config config;
    private final long producerId;
    private final short epoch;
    private final Map<TopicPartition, Accumulator> accumulators = new ConcurrentHashMap<>();
    private final Semaphore inFlight;
    private final AtomicInteger outstanding = new AtomicInteger(); // records accepted but not completed
    /** Bytes buffered but not yet acknowledged; send() blocks above this (Kafka's buffer.memory). */
    static final long BUFFER_MEMORY = 32L * 1024 * 1024;
    private final java.util.concurrent.atomic.AtomicLong bufferedBytes = new java.util.concurrent.atomic.AtomicLong();
    private final Object wake = new Object();
    private final Thread sender;
    private volatile boolean running = true;

    public BatchingProducer(SimpleKafkaClient client, Config config) throws IOException {
        this.client = client;
        this.config = config;
        this.inFlight = new Semaphore(config.maxInFlight());
        if (config.idempotent()) {
            long[] idAndEpoch = client.initProducerId();
            this.producerId = idAndEpoch[0];
            this.epoch = (short) idAndEpoch[1];
        } else {
            this.producerId = KafkaFrame.NO_PRODUCER_ID;
            this.epoch = 0;
        }
        this.sender = Thread.ofPlatform().daemon().name("batching-producer-sender").start(this::runSender);
    }

    /** Queues a record; the future completes with its offset once acknowledged per {@code acks}. */
    public CompletableFuture<Long> send(String topic, int partition, byte[] value) {
        if (!running) {
            return CompletableFuture.failedFuture(new IllegalStateException("Producer is closed"));
        }
        TopicPartition tp = new TopicPartition(topic, partition);
        Accumulator acc = accumulators.computeIfAbsent(tp, k -> new Accumulator());
        // Backpressure: block the caller instead of buffering without bound
        while (bufferedBytes.get() > BUFFER_MEMORY && running) {
            synchronized (wake) {
                try {
                    wake.wait(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return CompletableFuture.failedFuture(e);
                }
            }
        }
        CompletableFuture<Long> future = new CompletableFuture<>();
        boolean full = false;
        synchronized (acc) {
            if (acc.open != null && acc.open.bytes + value.length + 4 > config.batchBytes() && !acc.open.records.isEmpty()) {
                acc.ready.add(acc.open); // close the full batch; this record starts the next one
                acc.open = null;
                full = true;
            }
            if (acc.open == null) {
                acc.open = new Batch(tp);
            }
            acc.open.records.add(value);
            acc.open.futures.add(future);
            acc.open.bytes += value.length + 4;
        }
        bufferedBytes.addAndGet(value.length + 4);
        outstanding.incrementAndGet();
        if (full) {
            synchronized (wake) {
                wake.notifyAll();
            }
        }
        return future;
    }

    /** Sends everything queued and waits until every record is acknowledged or failed. */
    public void flush() throws InterruptedException {
        for (Accumulator acc : accumulators.values()) {
            synchronized (acc) {
                if (acc.open != null) {
                    acc.ready.add(acc.open); // ship now regardless of linger
                    acc.open = null;
                }
            }
        }
        synchronized (wake) {
            wake.notifyAll();
        }
        while (outstanding.get() > 0) {
            Thread.sleep(1);
        }
    }

    private void runSender() {
        while (running) {
            boolean sentSomething = false;
            long now = System.nanoTime();
            for (Accumulator acc : accumulators.values()) {
                Batch ready = null;
                synchronized (acc) {
                    if (config.idempotent() && acc.batchInFlight) {
                        continue; // one batch per partition in flight: retries can't reorder
                    }
                    if (!acc.retries.isEmpty()) {
                        ready = acc.retries.poll();
                    } else {
                        if (!acc.ready.isEmpty()) {
                            ready = acc.ready.poll();
                        } else if (acc.open != null && (acc.open.bytes >= config.batchBytes() || acc.open.deadlineNanos < 0
                                || now - acc.open.createdNanos >= TimeUnit.MILLISECONDS.toNanos(config.lingerMs()))) {
                            ready = acc.open;
                            acc.open = null;
                        }
                    }
                    if (ready != null && ready.deadlineNanos <= 0) { // first send of this batch (not a retry)
                        ready.deadlineNanos = now + TimeUnit.MILLISECONDS.toNanos(config.deliveryTimeoutMs());
                        if (config.idempotent()) {
                            ready.baseSequence = acc.nextSequence; // fixed for every retry of this batch
                            acc.nextSequence += ready.records.size();
                        }
                    }
                    if (ready != null && config.idempotent()) {
                        acc.batchInFlight = true;
                    }
                }
                if (ready != null) {
                    try {
                        inFlight.acquire();
                    } catch (InterruptedException e) {
                        return;
                    }
                    dispatch(ready, acc);
                    sentSomething = true;
                }
            }
            if (!sentSomething) {
                synchronized (wake) {
                    try {
                        wake.wait(1);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
    }

    private void dispatch(Batch batch, Accumulator acc) {
        byte[] request;
        BrokerConnection conn;
        try {
            request = SimpleKafkaClient.payload(o -> {
                o.writeUTF(client.clientId());
                o.writeUTF(batch.tp.topic());
                o.writeInt(batch.tp.partition());
                o.writeLong(producerId);
                o.writeShort(epoch);
                o.writeInt(Math.max(0, batch.baseSequence));
                o.writeInt(batch.records.size());
                for (byte[] record : batch.records) {
                    o.writeInt(record.length);
                    o.write(record);
                }
                o.writeByte(config.acks());
            });
            conn = client.leaderConnectionFor(batch.tp.topic(), batch.tp.partition());
        } catch (IOException e) {
            onFailure(batch, acc, e, true);
            return;
        }
        conn.sendAsync(KafkaFrame.API_KEY_PRODUCE_BATCH, request).whenComplete((response, error) -> {
            if (error != null) {
                onFailure(batch, acc, error, true); // connection-level: retriable
                return;
            }
            try {
                DataInputStream in = BrokerConnection.body(response);
                in.readUTF();
                in.readInt();
                long baseOffset = in.readLong();
                for (int i = 0; i < batch.futures.size(); i++) {
                    batch.futures.get(i).complete(baseOffset + i);
                }
                finished(batch, acc);
            } catch (BrokerErrorException e) {
                boolean retriable = e.getErrorCode() == KafkaFrame.ERROR_NOT_LEADER
                        || e.getErrorCode() == KafkaFrame.ERROR_LEADER_NOT_AVAILABLE
                        || e.getErrorCode() == KafkaFrame.ERROR_REQUEST_TIMED_OUT
                        || e.getErrorCode() == KafkaFrame.ERROR_NOT_ENOUGH_REPLICAS
                        || e.getErrorCode() == KafkaFrame.ERROR_BROKER_OVERLOADED
                        || e.getErrorCode() == KafkaFrame.ERROR_QUOTA_EXCEEDED;
                onFailure(batch, acc, e, retriable);
            } catch (IOException e) {
                onFailure(batch, acc, e, false);
            }
        });
    }

    private void onFailure(Batch batch, Accumulator acc, Throwable error, boolean retriable) {
        if (retriable && System.nanoTime() < batch.deadlineNanos && running) {
            LOGGER.log(Level.FINE, "Retrying batch for " + batch.tp + " after: " + error);
            CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS).execute(() -> {
                try {
                    client.refreshMetadata();
                } catch (IOException refreshFailed) {
                    // the bootstrap broker may be the one that's down; the retry tries again
                }
                synchronized (acc) {
                    acc.retries.addFirst(batch);
                    acc.batchInFlight = false;
                }
                inFlight.release();
                synchronized (wake) {
                    wake.notifyAll();
                }
            });
            return;
        }
        Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        batch.futures.forEach(f -> f.completeExceptionally(cause));
        finished(batch, acc);
    }

    private void finished(Batch batch, Accumulator acc) {
        synchronized (acc) {
            acc.batchInFlight = false;
        }
        bufferedBytes.addAndGet(-batch.bytes);
        outstanding.addAndGet(-batch.futures.size());
        inFlight.release();
        synchronized (wake) {
            wake.notifyAll();
        }
    }

    @Override
    public void close() {
        try {
            flush();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        running = false;
        sender.interrupt();
    }
}
