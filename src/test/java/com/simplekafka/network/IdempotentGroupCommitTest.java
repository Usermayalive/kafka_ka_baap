package com.simplekafka.network;

import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.NettyIdempotentProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Idempotent producers must not hold the per-partition idempotence lock while waiting for
 * fsync; otherwise every producer serializes behind every other producer's flush and group
 * commit never batches (measured: ~130 op/s regardless of connection count).
 */
public class IdempotentGroupCommitTest {

    @TempDir
    File tempDir;

    @Test
    void testFsyncWaitDoesNotBlockOtherProducersOnSamePartition() throws Exception {
        CountDownLatch firstFlushHeld = new CountDownLatch(1);
        CountDownLatch releaseFlush = new CountDownLatch(1);
        CountDownLatch secondAppended = new CountDownLatch(1);
        AtomicInteger appends = new AtomicInteger();

        Partition partition = new Partition(0, tempDir.getAbsolutePath()) {
            @Override
            public long appendBuffered(byte[] message) throws IOException {
                long offset = super.appendBuffered(message);
                if (appends.incrementAndGet() == 2) {
                    secondAppended.countDown();
                }
                return offset;
            }

            @Override
            public void awaitDurable(long offset) throws IOException {
                if (offset == 0) { // first producer's flush is slow
                    firstFlushHeld.countDown();
                    try {
                        releaseFlush.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                super.awaitDurable(offset);
            }
        };
        SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "localhost", 0, 2181) {
            @Override public void start() {}
            @Override public void stop() {}
        };
        broker.getTopics().put("t", new ArrayList<>(List.of(partition)));
        NettyBrokerServer server = new NettyBrokerServer("127.0.0.1", 0, broker);
        server.start();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (NettyIdempotentProducer a = new NettyIdempotentProducer("a", "127.0.0.1", server.getPort());
             NettyIdempotentProducer b = new NettyIdempotentProducer("b", "127.0.0.1", server.getPort())) {
            a.connect();
            b.connect();
            Future<?> first = pool.submit(() -> a.produce("t", 0, "a0".getBytes(StandardCharsets.UTF_8)));
            assertTrue(firstFlushHeld.await(5, TimeUnit.SECONDS));

            Future<?> second = pool.submit(() -> b.produce("t", 0, "b0".getBytes(StandardCharsets.UTF_8)));
            assertTrue(secondAppended.await(3, TimeUnit.SECONDS),
                    "Second producer must append while the first waits for fsync");

            releaseFlush.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            releaseFlush.countDown();
            pool.shutdownNow();
            server.stop();
        }
    }
}
