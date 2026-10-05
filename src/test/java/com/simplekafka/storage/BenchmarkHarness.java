package com.simplekafka.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Benchmark Harness with:
 * 1. JIT Warmup (3 rounds of 10K msgs) to trigger C2 compilation
 * 2. Latency percentiles: Average, p50, p95, p99, p99.9
 * 3. Multi-payload characterization: 64B, 1KB, 16KB
 * 4. Durability Comparison: OS_PAGE_CACHE vs PHYSICAL_FSYNC
 */
public class BenchmarkHarness {

    public static void main(String[] args) throws Exception {
        System.out.println("==========================================================================================");
        System.out.println("🚀 STORMLOG STORAGE ENGINE BENCHMARK (Java 25 LTS - Stable FFM)");
        System.out.println("==========================================================================================\n");

        Path tempDir = Files.createTempDirectory("stormlog-benchmark-");
        File baseDir = tempDir.toFile();

        try {
            // -------------------------------------------------------------
            // SCENARIO 1: Index Footprint for 100,000 Records
            // -------------------------------------------------------------
            int recordCount = 100_000;
            byte[] sampleMsg = "{\"event\":\"order_created\",\"user_id\":104928,\"amount\":49.99}".getBytes(StandardCharsets.UTF_8);

            System.out.println("--- Scenario 1: Index Footprint for " + recordCount + " Records ---");

            File denseIndexFile = new File(baseDir, "dense.index");
            long denseStart = System.nanoTime();
            try (FileOutputStream fos = new FileOutputStream(denseIndexFile)) {
                ByteBuffer buf = ByteBuffer.allocate(16);
                long pos = 0;
                for (int i = 0; i < recordCount; i++) {
                    buf.clear();
                    buf.putLong(i);
                    buf.putLong(pos);
                    fos.write(buf.array());
                    pos += 4 + sampleMsg.length;
                }
            }
            long denseDurationMs = (System.nanoTime() - denseStart) / 1_000_000;
            long denseBytes = denseIndexFile.length();

            File sparseIndexFile = new File(baseDir, "sparse.sindex");
            long sparseStart = System.nanoTime();
            SparseOffsetIndex sparseIndex = new SparseOffsetIndex(new ArrayList<>(), sparseIndexFile);
            long curPos = 0;
            for (int i = 0; i < recordCount; i++) {
                sparseIndex.maybeAddEntry(i, curPos);
                curPos += 4 + sampleMsg.length;
            }
            sparseIndex.flush();
            long sparseDurationMs = (System.nanoTime() - sparseStart) / 1_000_000;
            long sparseBytes = sparseIndexFile.length();

            System.out.printf("v1 Dense Index:   %,10d bytes (%,d entries) | Build: %3d ms%n",
                    denseBytes, recordCount, denseDurationMs);
            System.out.printf("v2 Sparse Index:  %,10d bytes (%,d entries)   | Build: %3d ms%n",
                    sparseBytes, sparseIndex.getEntryCount(), sparseDurationMs);
            double reduction = (1.0 - ((double) sparseBytes / denseBytes)) * 100.0;
            System.out.printf("💡 Storage Reduction: %.2f%% (~%.1fx smaller index footprint)%n%n",
                    reduction, (double) denseBytes / sparseBytes);

            // -------------------------------------------------------------
            // WARMUP
            // -------------------------------------------------------------
            System.out.println("--- JIT Warmup (C2 compilation & TLAB stabilization) ---");
            File warmupDir = new File(baseDir, "wal-warmup");
            warmupDir.mkdirs();
            byte[] warmupPayload = new byte[1024];
            for (int round = 1; round <= 3; round++) {
                try (WriteAheadLog wal = new WriteAheadLog(warmupDir, 50 * 1024 * 1024)) {
                    List<CompletableFuture<Long>> list = new ArrayList<>(10_000);
                    for (int i = 0; i < 10_000; i++) list.add(wal.appendAsync(warmupPayload));
                    CompletableFuture.allOf(list.toArray(new CompletableFuture[0])).join();
                }
                System.out.println("  ✓ Warmup iteration " + round + " complete (10,000 msgs)");
            }
            System.out.println("  ✓ Hot paths compiled by C2 JIT.\n");

            // -------------------------------------------------------------
            // SCENARIO 2: OS_PAGE_CACHE Mode (Kafka standard default)
            // -------------------------------------------------------------
            System.out.println("--- Scenario 2: High-Throughput OS Page-Cache Mode (Kafka Default) ---");
            System.out.println("Payload | Total Msgs | Elapsed | Throughput    | Bandwidth   | p50 Lat | p95 Lat | p99 Lat");
            System.out.println("------------------------------------------------------------------------------------------");
            runDetailedBenchmark(new File(baseDir, "wal-page-64b"), 64, 50_000, WriteAheadLog.DurabilityMode.OS_PAGE_CACHE);
            runDetailedBenchmark(new File(baseDir, "wal-page-1kb"), 1024, 30_000, WriteAheadLog.DurabilityMode.OS_PAGE_CACHE);
            runDetailedBenchmark(new File(baseDir, "wal-page-16kb"), 16384, 10_000, WriteAheadLog.DurabilityMode.OS_PAGE_CACHE);
            System.out.println();

            // -------------------------------------------------------------
            // SCENARIO 3: PHYSICAL_FSYNC Mode (force fsync per batch)
            // -------------------------------------------------------------
            System.out.println("--- Scenario 3: Physical Fsync Mode (Durability against Kernel Crash) ---");
            System.out.println("Payload | Total Msgs | Elapsed | Throughput    | Bandwidth   | p50 Lat | p95 Lat | p99 Lat");
            System.out.println("------------------------------------------------------------------------------------------");
            runDetailedBenchmark(new File(baseDir, "wal-sync-64b"), 64, 10_000, WriteAheadLog.DurabilityMode.PHYSICAL_FSYNC);
            runDetailedBenchmark(new File(baseDir, "wal-sync-1kb"), 1024, 10_000, WriteAheadLog.DurabilityMode.PHYSICAL_FSYNC);
            runDetailedBenchmark(new File(baseDir, "wal-sync-16kb"), 16384, 5_000, WriteAheadLog.DurabilityMode.PHYSICAL_FSYNC);
            System.out.println();

            // -------------------------------------------------------------
            // SCENARIO 4: End-to-end through the broker (Netty -> idempotence -> Partition)
            // Scenarios 2-3 measure WriteAheadLog in isolation; the broker does not use it.
            // This is the number that belongs in the README. Full matrix: BrokerBenchmark.main.
            // -------------------------------------------------------------
            System.out.println("--- Scenario 4: End-to-End Broker Produce Path (1 KB, single partition) ---");
            for (WriteAheadLog.DurabilityMode mode : WriteAheadLog.DurabilityMode.values()) {
                for (boolean idempotent : new boolean[]{false, true}) {
                    BrokerBenchmark.Result r = BrokerBenchmark.run(new BrokerBenchmark.Config(mode, idempotent, 16, 16, 1024));
                    System.out.printf("%-15s idempotent=%-5b 16 conns x 16 in-flight | %,10.0f op/s | p50 %6.2f ms | p99 %6.2f ms%n",
                            mode, idempotent, r.opsPerSec(), r.p50Ms(), r.p99Ms());
                }
            }

            System.out.println("==========================================================================================");

        } finally {
            deleteRecursively(baseDir);
        }
    }

    private static void runDetailedBenchmark(File walDir, int payloadSize, int count, WriteAheadLog.DurabilityMode mode) throws Exception {
        walDir.mkdirs();
        byte[] payload = new byte[payloadSize];
        Arrays.fill(payload, (byte) 0x2A);

        try (WriteAheadLog wal = new WriteAheadLog(walDir, 500 * 1024 * 1024, mode)) {
            List<CompletableFuture<Long>> futures = new ArrayList<>(count);
            long[] sendTimestamps = new long[count];
            long[] latenciesNs = new long[count];

            long overallStart = System.nanoTime();
            for (int i = 0; i < count; i++) {
                sendTimestamps[i] = System.nanoTime();
                final int idx = i;
                CompletableFuture<Long> f = wal.appendAsync(payload);
                f.thenAccept(off -> latenciesNs[idx] = System.nanoTime() - sendTimestamps[idx]);
                futures.add(f);
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            long overallElapsedNanos = System.nanoTime() - overallStart;

            double elapsedSec = overallElapsedNanos / 1_000_000_000.0;
            double opsPerSec = count / elapsedSec;
            long totalBytes = (long) count * (4 + payloadSize);
            double mbPerSec = (totalBytes / (1024.0 * 1024.0)) / elapsedSec;

            Arrays.sort(latenciesNs);
            double p50Us = latenciesNs[(int) (count * 0.50)] / 1000.0;
            double p95Us = latenciesNs[(int) (count * 0.95)] / 1000.0;
            double p99Us = latenciesNs[(int) (count * 0.99)] / 1000.0;

            String label = payloadSize >= 1024 ? (payloadSize / 1024) + " KB" : payloadSize + " B";
            System.out.printf("%-7s | %,10d | %5d ms | %,11.0f op/s | %9.2f MB/s | %5.2f ms | %5.2f ms | %5.2f ms%n",
                    label, count, (overallElapsedNanos / 1_000_000), opsPerSec, mbPerSec,
                    p50Us / 1000.0, p95Us / 1000.0, p99Us / 1000.0);
        }
    }

    private static void deleteRecursively(File dir) {
        if (dir == null || !dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteRecursively(f);
                else f.delete();
            }
        }
        dir.delete();
    }
}
