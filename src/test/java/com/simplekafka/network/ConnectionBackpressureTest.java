package com.simplekafka.network;

import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class ConnectionBackpressureTest {

    @TempDir
    File tempDir;

    private static byte[] produceFrame(int correlationId, long producerId, int sequence, byte[] message) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        DataOutputStream p = new DataOutputStream(payload);
        p.writeUTF("pipeliner");
        p.writeUTF("bp-topic");
        p.writeInt(0);
        p.writeLong(producerId);
        p.writeShort(0);
        p.writeInt(sequence);
        p.writeInt(message.length);
        p.write(message);

        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        DataOutputStream f = new DataOutputStream(frame);
        f.writeInt(2 + 2 + 4 + payload.size());
        f.writeShort(KafkaFrame.API_KEY_PRODUCE);
        f.writeShort(1);
        f.writeInt(correlationId);
        f.write(payload.toByteArray());
        return frame.toByteArray();
    }

    /**
     * One client pipelines ~22 MB of produce requests while the partition is stalled. With
     * per-connection backpressure the broker stops reading, TCP flow control blocks the client's
     * writes, and the broker's in-memory backlog stays bounded. Without it, the broker reads
     * everything into its per-connection queue and the writer finishes immediately.
     */
    @Test
    void testPipelinedBurstOnOneConnectionIsThrottled() throws Exception {
        CountDownLatch stall = new CountDownLatch(1);
        Partition slowPartition = new Partition(0, tempDir.getAbsolutePath()) {
            @Override
            public long appendBuffered(byte[] message) throws IOException {
                try {
                    stall.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.appendBuffered(message);
            }
        };
        SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "localhost", 0, 2181) {
            @Override public void start() {}
            @Override public void stop() {}
        };
        broker.getTopics().put("bp-topic", new ArrayList<>(List.of(slowPartition)));

        NettyBrokerServer server = new NettyBrokerServer("127.0.0.1", 0, broker,
                new ClientQuotaManager(10L * 1024 * 1024 * 1024, 10_000_000), new IdempotenceManager(),
                new ProducerIdManager(), NettyBrokerServer.DEFAULT_REQUEST_QUEUE_CAPACITY);
        server.start();

        int requests = 20_000;
        byte[] message = new byte[1024];
        ExecutorService io = Executors.newFixedThreadPool(2);
        try (Socket socket = new Socket("127.0.0.1", server.getPort())) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            Future<?> writer = io.submit(() -> {
                for (int i = 0; i < requests; i++) {
                    out.write(produceFrame(i, 77L, i, message));
                }
                out.flush();
                return null;
            });

            Map<Integer, Integer> responsesPerCorrelationId = new ConcurrentHashMap<>();
            AtomicInteger errors = new AtomicInteger();
            Future<?> reader = io.submit(() -> {
                for (int i = 0; i < requests; i++) {
                    int len = in.readInt();
                    in.readShort(); // apiKey
                    in.readShort(); // apiVersion
                    int correlationId = in.readInt();
                    byte[] body = new byte[len - 8];
                    in.readFully(body);
                    if (new DataInputStream(new ByteArrayInputStream(body)).readShort() != KafkaFrame.ERROR_NONE) {
                        errors.incrementAndGet();
                    }
                    responsesPerCorrelationId.merge(correlationId, 1, Integer::sum);
                }
                return null;
            });

            assertThrows(TimeoutException.class, () -> writer.get(2, TimeUnit.SECONDS),
                    "Broker must stop reading a backlogged connection, blocking the client's writes");

            stall.countDown();
            writer.get(60, TimeUnit.SECONDS);
            reader.get(60, TimeUnit.SECONDS);

            assertEquals(requests, responsesPerCorrelationId.size(), "Every request must be answered");
            assertTrue(responsesPerCorrelationId.values().stream().allMatch(c -> c == 1),
                    "Exactly one response per correlation ID");
            assertEquals(0, errors.get(), "In-order pipelined sequences must all be accepted");
        } finally {
            stall.countDown();
            io.shutdownNow();
            server.stop();
        }
    }

    /** A rejected task must never run, otherwise the client gets BROKER_OVERLOADED plus a real response. */
    @Test
    void testRejectedTaskNeverRuns() {
        Executor rejectingPool = task -> {
            throw new RejectedExecutionException("saturated");
        };
        SerializingExecutor executor = new SerializingExecutor(rejectingPool);
        AtomicInteger runs = new AtomicInteger();

        assertThrows(RejectedExecutionException.class, () -> executor.execute(runs::incrementAndGet));
        assertEquals(0, executor.getQueueDepth(), "Rejected task must be withdrawn from the queue");

        // Previously the rejected task stayed queued and ran along with the next accepted one
        List<Runnable> accepted = new ArrayList<>();
        SerializingExecutor recovering = new SerializingExecutor(new Executor() {
            boolean first = true;
            @Override
            public void execute(Runnable r) {
                if (first) {
                    first = false;
                    throw new RejectedExecutionException("saturated");
                }
                accepted.add(r);
            }
        });
        assertThrows(RejectedExecutionException.class, () -> recovering.execute(runs::incrementAndGet));
        recovering.execute(() -> {});
        accepted.forEach(Runnable::run);
        assertEquals(0, runs.get(), "Rejected task must not run later");
    }

    /** When the pool can't take the runner back, it keeps draining instead of stranding tasks. */
    @Test
    void testRunnerKeepsDrainingWhenResubmitIsRejected() {
        List<Runnable> submitted = new ArrayList<>();
        AtomicInteger submissions = new AtomicInteger();
        SerializingExecutor executor = new SerializingExecutor(r -> {
            if (submissions.incrementAndGet() > 1) {
                throw new RejectedExecutionException("saturated");
            }
            submitted.add(r);
        });
        AtomicInteger runs = new AtomicInteger();
        executor.execute(() -> {
            runs.incrementAndGet();
            executor.execute(runs::incrementAndGet); // queued while the runner is active
        });
        submitted.get(0).run();
        assertEquals(2, runs.get(), "Task queued behind the active runner must still run");
        assertEquals(0, executor.getQueueDepth());
    }
}
