package com.simplekafka.storage;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.BrokerConnection;
import com.simplekafka.network.*;
import com.simplekafka.storage.WriteAheadLog.DurabilityMode;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end produce benchmark through the broker's real path: client connections → Netty
 * framing → per-connection SerializingExecutor → quota check → idempotence → Partition.append.
 *
 * Every connection produces to the same single partition, so the numbers include contention
 * on the partition write lock (and, in PHYSICAL_FSYNC mode, show group commit amortization).
 * Quotas are raised so they don't cap the measurement; the defaults (5,000 req/s per client)
 * would otherwise be the bottleneck.
 *
 * Run: java ... com.simplekafka.storage.BrokerBenchmark [csvOut]
 */
public class BrokerBenchmark {

    public record Config(DurabilityMode mode, boolean idempotent, int connections, int inFlight, int payloadBytes) {}

    public record Result(Config config, long ops, double seconds, double opsPerSec, double mbPerSec,
                         double p50Ms, double p99Ms, long errors) {}

    private static final long WARMUP_MS = Long.getLong("bench.warmupMs", 1_000);
    private static final long MEASURE_MS = Long.getLong("bench.measureMs", 3_000);

    public static void main(String[] args) throws Exception {
        File csv = new File(args.length > 0 ? args[0] : "target/bench/broker-e2e.csv");
        List<Config> configs = new ArrayList<>();
        for (DurabilityMode mode : DurabilityMode.values()) {
            for (boolean idempotent : new boolean[]{false, true}) {
                for (int connections : new int[]{1, 4, 16}) {
                    for (int inFlight : new int[]{1, 16}) {
                        configs.add(new Config(mode, idempotent, connections, inFlight, 1024));
                    }
                }
            }
        }
        for (int payload : new int[]{64, 1024, 16 * 1024}) {
            configs.add(new Config(DurabilityMode.OS_PAGE_CACHE, true, 16, 16, payload));
        }

        printHeader();
        List<Result> results = new ArrayList<>();
        for (Config c : configs) {
            Result r = run(c);
            results.add(r);
            printRow(r);
        }
        writeCsv(csv, results);
        System.out.println("CSV written to " + csv.getAbsolutePath());
        System.exit(0);
    }

    public static Result run(Config c) throws Exception {
        File dir = Files.createTempDirectory("broker-bench").toFile();
        Partition partition = new Partition(0, dir.getAbsolutePath(), c.mode());
        int[] port = new int[1];
        SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "127.0.0.1", 0, 2181) {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public List<BrokerInfo> liveBrokers() {
                return List.of(new BrokerInfo(1, "127.0.0.1", port[0]));
            }
        };
        broker.getTopics().put("bench", new ArrayList<>(List.of(partition)));
        NettyBrokerServer server = new NettyBrokerServer("127.0.0.1", 0, broker,
                new ClientQuotaManager(Long.MAX_VALUE / 4, Long.MAX_VALUE / 4), new IdempotenceManager(),
                new ProducerIdManager(), NettyBrokerServer.DEFAULT_REQUEST_QUEUE_CAPACITY);
        server.start();
        port[0] = server.getPort();

        byte[] payload = new byte[c.payloadBytes()];
        Arrays.fill(payload, (byte) 0x2A);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean measuring = new AtomicBoolean(false);
        AtomicLong ops = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        List<long[]> latencyBuffers = Collections.synchronizedList(new ArrayList<>());
        List<Thread> workers = new ArrayList<>();
        List<BrokerConnection> connections = new ArrayList<>();

        try {
            for (int i = 0; i < c.connections(); i++) {
                BrokerConnection conn = BrokerConnection.connect("127.0.0.1", server.getPort());
                connections.add(conn);
                String clientId = "bench-" + i;
                long producerId = c.idempotent() ? initProducerId(conn, clientId) : KafkaFrame.NO_PRODUCER_ID;
                Thread t = Thread.ofPlatform().name("bench-conn-" + i).start(() ->
                        drive(conn, clientId, producerId, payload, c.inFlight(), running, measuring, ops, errors, latencyBuffers));
                workers.add(t);
            }

            Thread.sleep(WARMUP_MS);
            long startOps = ops.get();
            measuring.set(true);
            long start = System.nanoTime();
            Thread.sleep(MEASURE_MS);
            measuring.set(false);
            long elapsedNs = System.nanoTime() - start;
            long measuredOps = ops.get() - startOps;
            running.set(false);
            for (Thread t : workers) {
                t.join(30_000);
            }

            long[] all = latencyBuffers.stream().flatMapToLong(Arrays::stream).filter(v -> v > 0).sorted().toArray();
            double seconds = elapsedNs / 1e9;
            double p50 = all.length == 0 ? 0 : all[(int) (all.length * 0.50)] / 1e6;
            double p99 = all.length == 0 ? 0 : all[Math.min(all.length - 1, (int) (all.length * 0.99))] / 1e6;
            double opsPerSec = measuredOps / seconds;
            double mbPerSec = opsPerSec * c.payloadBytes() / (1024.0 * 1024.0);
            return new Result(c, measuredOps, seconds, opsPerSec, mbPerSec, p50, p99, errors.get());
        } finally {
            connections.forEach(BrokerConnection::close);
            server.stop();
            deleteRecursively(dir);
        }
    }

    private static void drive(BrokerConnection conn, String clientId, long producerId, byte[] payload, int inFlight,
                              AtomicBoolean running, AtomicBoolean measuring, AtomicLong ops, AtomicLong errors,
                              List<long[]> latencyBuffers) {
        Semaphore window = new Semaphore(inFlight);
        long[] latencies = new long[1 << 16];
        int[] count = {0};
        int sequence = 0;
        try {
            while (running.get()) {
                window.acquire();
                long sentAt = System.nanoTime();
                boolean inWindow = measuring.get();
                conn.sendAsync(KafkaFrame.API_KEY_PRODUCE, produceRequest(clientId, producerId, sequence++, payload))
                        .whenComplete((resp, err) -> {
                            if (err != null || resp.getPayload().length < 2
                                    || ByteBuffer.wrap(resp.getPayload()).getShort() != KafkaFrame.ERROR_NONE) {
                                errors.incrementAndGet();
                            } else {
                                ops.incrementAndGet();
                                if (inWindow) {
                                    synchronized (count) {
                                        if (count[0] < latencies.length) {
                                            latencies[count[0]++] = System.nanoTime() - sentAt;
                                        }
                                    }
                                }
                            }
                            window.release();
                        });
            }
            window.acquire(inFlight); // drain outstanding requests
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            errors.incrementAndGet();
        }
        synchronized (count) {
            latencyBuffers.add(Arrays.copyOf(latencies, count[0]));
        }
    }

    private static long initProducerId(BrokerConnection conn, String clientId) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new DataOutputStream(bytes).writeUTF(clientId);
        return conn.request(KafkaFrame.API_KEY_INIT_PRODUCER_ID, bytes.toByteArray()).readLong();
    }

    private static byte[] produceRequest(String clientId, long producerId, int sequence, byte[] payload) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(payload.length + 64);
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeUTF(clientId);
        out.writeUTF("bench");
        out.writeInt(0);
        out.writeLong(producerId);
        out.writeShort(0);
        out.writeInt(sequence);
        out.writeInt(payload.length);
        out.write(payload);
        return bytes.toByteArray();
    }

    private static void printHeader() {
        System.out.println("Durability      | Idem | Conns | InFlight | Payload |   Throughput   | Bandwidth   | p50 ms | p99 ms | Errors");
        System.out.println("----------------|------|-------|----------|---------|----------------|-------------|--------|--------|-------");
    }

    private static void printRow(Result r) {
        Config c = r.config();
        System.out.printf(Locale.ROOT, "%-15s | %-4s | %5d | %8d | %7s | %,10.0f op/s | %7.2f MB/s | %6.2f | %6.2f | %d%n",
                c.mode(), c.idempotent() ? "yes" : "no", c.connections(), c.inFlight(), formatBytes(c.payloadBytes()),
                r.opsPerSec(), r.mbPerSec(), r.p50Ms(), r.p99Ms(), r.errors());
    }

    private static void writeCsv(File csv, List<Result> results) throws IOException {
        csv.getParentFile().mkdirs();
        try (PrintWriter w = new PrintWriter(csv, StandardCharsets.UTF_8)) {
            w.println("durability,idempotent,connections,in_flight,payload_bytes,ops,seconds,ops_per_sec,mb_per_sec,p50_ms,p99_ms,errors");
            for (Result r : results) {
                Config c = r.config();
                w.printf(Locale.ROOT, "%s,%b,%d,%d,%d,%d,%.3f,%.1f,%.3f,%.3f,%.3f,%d%n", c.mode(), c.idempotent(),
                        c.connections(), c.inFlight(), c.payloadBytes(), r.ops(), r.seconds(), r.opsPerSec(),
                        r.mbPerSec(), r.p50Ms(), r.p99Ms(), r.errors());
            }
        }
    }

    private static String formatBytes(int bytes) {
        return bytes >= 1024 ? (bytes / 1024) + " KB" : bytes + " B";
    }

    private static void deleteRecursively(File f) {
        File[] children = f.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        f.delete();
    }
}
