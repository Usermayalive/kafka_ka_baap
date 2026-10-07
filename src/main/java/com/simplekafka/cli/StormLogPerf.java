package com.simplekafka.cli;

import com.simplekafka.client.BrokerConnection;
import com.simplekafka.network.KafkaFrame;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer load generator against a running broker (the StormLog counterpart of
 * kafka-producer-perf-test): one connection, a fixed window of in-flight requests, one record
 * per request (StormLog's client does not batch).
 *
 * <pre>
 * StormLogPerf HOST:PORT TOPIC NUM_RECORDS RECORD_SIZE IN_FLIGHT [idempotent|plain|batched] [RECORDS_PER_SEC] [all|1]
 * </pre>
 * "batched" uses {@link com.simplekafka.client.BatchingProducer} with Kafka's default batching
 * (16 KB, linger 5 ms; IN_FLIGHT batches): the like-for-like comparison with Kafka's producer.
 * With RECORDS_PER_SEC, sends are paced on a fixed schedule (like kafka-producer-perf-test
 * --throughput), so latency is measured below saturation instead of including queueing.
 * Prints one CSV line: records,record_size,seconds,records_per_sec,mb_per_sec,p50_ms,p99_ms,errors
 */
public class StormLogPerf {

    public static void main(String[] args) throws Exception {
        String[] hostPort = args[0].split(":");
        String topic = args[1];
        int records = Integer.parseInt(args[2]);
        int recordSize = Integer.parseInt(args[3]);
        int inFlight = Integer.parseInt(args[4]);
        boolean idempotent = args.length > 5 && args[5].equals("idempotent");
        long ratePerSec = args.length > 6 ? Long.parseLong(args[6]) : 0;
        long intervalNanos = ratePerSec > 0 ? 1_000_000_000L / ratePerSec : 0;
        byte acks = args.length > 7 && args[7].equals("all") ? KafkaFrame.ACKS_ALL : KafkaFrame.ACKS_LEADER;

        if (args.length > 5 && args[5].equals("batched")) {
            runBatched(hostPort[0], Integer.parseInt(hostPort[1]), topic, records, recordSize, inFlight, intervalNanos, acks);
            System.exit(0);
        }
        try (BrokerConnection conn = BrokerConnection.connect(hostPort[0], Integer.parseInt(hostPort[1]))) {
            long producerId = KafkaFrame.NO_PRODUCER_ID;
            if (idempotent) {
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                new DataOutputStream(b).writeUTF("perf");
                producerId = conn.request(KafkaFrame.API_KEY_INIT_PRODUCER_ID, b.toByteArray()).readLong();
            }
            byte[] payload = new byte[recordSize];
            Arrays.fill(payload, (byte) 'x');
            long[] latencies = new long[records];
            Semaphore window = new Semaphore(inFlight);
            AtomicLong errors = new AtomicLong();

            long start = System.nanoTime();
            for (int i = 0; i < records; i++) {
                if (intervalNanos > 0) {
                    long due = start + i * intervalNanos;
                    while (System.nanoTime() < due) {
                        Thread.onSpinWait();
                    }
                }
                window.acquire();
                final int index = i;
                final long sentAt = System.nanoTime();
                conn.sendAsync(KafkaFrame.API_KEY_PRODUCE, request(topic, producerId, i, payload, acks)).whenComplete((resp, err) -> {
                    if (err != null || ByteBuffer.wrap(resp.getPayload()).getShort() != KafkaFrame.ERROR_NONE) {
                        errors.incrementAndGet();
                    }
                    latencies[index] = System.nanoTime() - sentAt;
                    window.release();
                });
            }
            window.acquire(inFlight);
            double seconds = (System.nanoTime() - start) / 1e9;

            Arrays.sort(latencies);
            double recordsPerSec = records / seconds;
            System.out.printf(Locale.ROOT, "%d,%d,%.3f,%.1f,%.3f,%.3f,%.3f,%d%n", records, recordSize, seconds,
                    recordsPerSec, recordsPerSec * recordSize / (1024.0 * 1024.0),
                    latencies[records / 2] / 1e6, latencies[(int) (records * 0.99)] / 1e6, errors.get());
        }
        System.exit(0);
    }

    private static void runBatched(String host, int port, String topic, int records, int recordSize, int inFlight,
                                   long intervalNanos, byte acks) throws Exception {
        byte[] payload = new byte[recordSize];
        Arrays.fill(payload, (byte) 'x');
        long[] latencies = new long[records];
        AtomicLong errors = new AtomicLong();
        com.simplekafka.client.BatchingProducer.Config config = com.simplekafka.client.BatchingProducer.Config.defaults()
                .withAcks(acks);
        config = new com.simplekafka.client.BatchingProducer.Config(config.batchBytes(), config.lingerMs(), inFlight,
                acks, false, config.deliveryTimeoutMs());
        try (com.simplekafka.client.SimpleKafkaClient client = new com.simplekafka.client.SimpleKafkaClient(host, port, "perf");
             com.simplekafka.client.BatchingProducer producer = new com.simplekafka.client.BatchingProducer(client, config)) {
            client.initialize();
            long start = System.nanoTime();
            for (int i = 0; i < records; i++) {
                if (intervalNanos > 0) {
                    long due = start + i * intervalNanos;
                    while (System.nanoTime() < due) {
                        Thread.onSpinWait();
                    }
                }
                final int index = i;
                final long sentAt = System.nanoTime();
                producer.send(topic, 0, payload).whenComplete((offset, err) -> {
                    if (err != null) {
                        errors.incrementAndGet();
                    }
                    latencies[index] = System.nanoTime() - sentAt;
                });
            }
            producer.flush();
            double seconds = (System.nanoTime() - start) / 1e9;
            Arrays.sort(latencies);
            double recordsPerSec = records / seconds;
            System.out.printf(Locale.ROOT, "%d,%d,%.3f,%.1f,%.3f,%.3f,%.3f,%d%n", records, recordSize, seconds,
                    recordsPerSec, recordsPerSec * recordSize / (1024.0 * 1024.0),
                    latencies[records / 2] / 1e6, latencies[(int) (records * 0.99)] / 1e6, errors.get());
        }
    }

    private static byte[] request(String topic, long producerId, int sequence, byte[] payload, byte acks) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(payload.length + 64);
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeUTF("perf");
        out.writeUTF(topic);
        out.writeInt(0);
        out.writeLong(producerId);
        out.writeShort(0);
        out.writeInt(sequence);
        out.writeInt(payload.length);
        out.write(payload);
        out.writeByte(acks);
        return bytes.toByteArray();
    }
}
