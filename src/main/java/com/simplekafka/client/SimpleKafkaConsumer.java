package com.simplekafka.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleKafkaConsumer {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());
    private static final int MAX_BYTES = 1024 * 1024;
    private static final long POLL_INTERVAL_MS = 100;

    private final SimpleKafkaClient client;
    private final String groupId;
    private long lastCommitted = -1;
    private final String topic;
    private final int partition;
    private long currentOffset;      // Fetch cursor for the next poll
    private long committedOffset;    // High watermark of successfully processed / routed records
    private final AtomicBoolean running;
    private final AtomicBoolean paused;
    private Thread consumerThread;

    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this(new SimpleKafkaClient(bootstrapBroker, bootstrapPort), topic, partition);
    }

    SimpleKafkaConsumer(SimpleKafkaClient client, String topic, int partition) {
        this(client, topic, partition, null);
    }

    /**
     * @param groupId consumer group whose committed offset this consumer resumes from and
     *                commits to after each processed batch; null for no offset storage
     */
    public SimpleKafkaConsumer(SimpleKafkaClient client, String topic, int partition, String groupId) {
        this.groupId = groupId;
        this.client = client;
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = 0;
        this.committedOffset = 0;
        this.running = new AtomicBoolean(false);
        this.paused = new AtomicBoolean(false);
    }

    private final java.util.Queue<byte[]> fetchBuffer = new java.util.concurrent.ConcurrentLinkedQueue<>();

    public void initialize() throws IOException {
        client.initialize();
    }

    public boolean isPaused() {
        return paused.get();
    }

    public void pause() {
        paused.set(true);
    }

    public void resume() {
        paused.set(false);
    }

    public java.util.Queue<byte[]> getFetchBuffer() {
        return fetchBuffer;
    }

    public List<byte[]> poll() throws IOException {
        List<byte[]> payloads = new java.util.ArrayList<>();
        for (SimpleKafkaClient.FetchedRecord r : pollRecords()) {
            payloads.add(r.payload());
        }
        return payloads;
    }

    /**
     * Fetches the next batch and advances the cursor past the last record's offset. Offsets
     * come from the broker, so compaction gaps are skipped correctly. Records injected via
     * getFetchBuffer() are numbered consecutively from the current cursor.
     */
    public List<SimpleKafkaClient.FetchedRecord> pollRecords() throws IOException {
        List<SimpleKafkaClient.FetchedRecord> records;
        if (!fetchBuffer.isEmpty()) {
            List<byte[]> buffered = new java.util.ArrayList<>();
            while (!fetchBuffer.isEmpty()) {
                buffered.add(fetchBuffer.poll());
            }
            records = SimpleKafkaClient.contiguous(currentOffset, buffered);
        } else {
            records = client.fetchRecords(topic, partition, currentOffset, MAX_BYTES);
        }
        if (!records.isEmpty()) {
            currentOffset = records.get(records.size() - 1).offset() + 1;
        }
        return records;
    }

    public void startConsuming(MessageHandler handler) {
        startConsuming(handler, null);
    }

    /**
     * Consumes records from the partition with strict DLQ delivery and offset commit ordering:
     * 1. handler.handle(msg, offset) executes.
     * 2. If handler throws, dlqRouter applies retry policy with backoff.
     * 3. If retries exhaust or error is fatal, dlqRouter serializes envelope and produces to {topic}.dlq.
     * 4. IF DLQ produce succeeds -> committedOffset advances to msgOffset + 1.
     * 5. IF DLQ produce fails -> committedOffset DOES NOT advance (remains at poison pill offset).
     *    Fetch cursor (currentOffset) is rewound to msgOffset, remaining in-flight buffer is discarded,
     *    and the partition is paused to prevent silent data loss.
     */
    public void startConsuming(MessageHandler handler, DeadLetterRouter dlqRouter) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                try {
                    resumeFromCommitted();
                    while (running.get()) {
                        if (paused.get()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                            continue;
                        }

                        List<SimpleKafkaClient.FetchedRecord> messages = pollRecords();

                        for (SimpleKafkaClient.FetchedRecord record : messages) {
                            long msgOffset = record.offset();
                            byte[] msg = record.payload();
                            if (dlqRouter != null) {
                                DeadLetterRouter.RoutingResult result = dlqRouter.handleMessage(topic, partition, msgOffset, msg, (m, off) -> handler.handle(m, off));
                                if (result.isFatalDlqProduceError()) {
                                    LOGGER.severe(String.format("FATAL: Failed to route poison pill at [%s-p%d@%d] to DLQ. Rewinding fetch cursor and pausing consumption at offset %d to prevent silent data loss!",
                                            topic, partition, msgOffset, msgOffset));
                                    seek(msgOffset); // Rewind fetch cursor; remaining batch messages are discarded
                                    this.paused.set(true); // Pause partition to prevent advancing
                                    break;
                                } else {
                                    this.committedOffset = msgOffset + 1;
                                }
                            } else {
                                handler.handle(msg, msgOffset);
                                this.committedOffset = msgOffset + 1;
                            }
                        }

                        commitSync();
                        if (messages.isEmpty()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        LOGGER.log(Level.SEVERE, "Error in consumer loop", e);
                    }
                    running.set(false);
                }
            });
            consumerThread.setDaemon(true);
            consumerThread.start();
            LOGGER.info("Started consuming from topic: " + topic + ", partition: " + partition + (dlqRouter != null ? " [DLQ active]" : ""));
        }
    }

    public void stopConsuming() {
        if (running.compareAndSet(true, false)) {
            if (consumerThread != null) {
                try {
                    consumerThread.interrupt();
                    consumerThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            LOGGER.info("Stopped consuming from topic: " + topic + ", partition: " + partition);
        }
    }

    /** Positions the fetch cursor at the group's committed offset, if any. */
    public void resumeFromCommitted() throws IOException {
        if (groupId == null) {
            return;
        }
        long committed = client.fetchCommittedOffset(groupId, topic, partition);
        if (committed >= 0) {
            seek(committed);
            committedOffset = committed;
            lastCommitted = committed;
            LOGGER.info("Group " + groupId + " resuming " + topic + "-" + partition + " at offset " + committed);
        }
    }

    /**
     * Commits the next offset to process (Kafka convention: last processed + 1) if it advanced.
     * Only offsets past fully handled or dead-lettered records are ever committed.
     */
    public void commitSync() throws IOException {
        if (groupId != null && committedOffset > lastCommitted) {
            long toCommit = committedOffset;
            client.commitOffset(groupId, topic, partition, toCommit, "");
            lastCommitted = toCommit;
        }
    }

    public void seek(long offset) {
        this.currentOffset = offset;
        this.fetchBuffer.clear();
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public long getCommittedOffset() {
        return committedOffset;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: SimpleKafkaConsumer <host> <port> <topic> <partition>");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        int partition = Integer.parseInt(args[3]);
        
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(host, port, topic, partition);
        consumer.initialize();
        
        consumer.startConsuming((message, offset) -> {
            System.out.println("Received message: '" + new String(message) + "' at offset: " + offset);
        });
        
        // Keep main thread alive
        Thread.currentThread().join();
    }
}
