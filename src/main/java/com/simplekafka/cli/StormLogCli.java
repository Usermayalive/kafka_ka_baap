package com.simplekafka.cli;

import com.simplekafka.client.GroupConsumer;
import com.simplekafka.client.SimpleKafkaClient;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Command-line client for demos and operations.
 *
 * <pre>
 * java -cp stormlog.jar com.simplekafka.cli.StormLogCli --bootstrap HOST:PORT COMMAND ...
 *   describe
 *   create-topic TOPIC PARTITIONS [REPLICATION_FACTOR]
 *   produce TOPIC PARTITION MESSAGE
 *   produce-batch TOPIC COUNT              round-robin across partitions
 *   consume TOPIC PARTITION [FROM_OFFSET]  print what's there and exit
 *   consume-group GROUP TOPIC SECONDS      join a consumer group and print what it receives
 *   commit GROUP TOPIC PARTITION OFFSET
 *   consume-with-dlq TOPIC PARTITION SECONDS  handler rejects records containing "POISON";
 *                                         they're dead-lettered to TOPIC.dlq, then the DLQ is printed
 *   forget-entity ENTITY_ID
 * </pre>
 */
public class StormLogCli {

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || !args[0].equals("--bootstrap")) {
            System.err.println("Usage: StormLogCli --bootstrap HOST:PORT COMMAND [ARGS] (see class javadoc)");
            System.exit(2);
        }
        String[] hostPort = args[1].split(":");
        String command = args[2];
        try (SimpleKafkaClient client = new SimpleKafkaClient(hostPort[0], Integer.parseInt(hostPort[1]), "stormlog-cli")) {
            client.initialize();
            switch (command) {
                case "describe" -> describe(client);
                case "create-topic" -> {
                    short rf = args.length > 5 ? Short.parseShort(args[5]) : 1;
                    client.createTopic(args[3], Integer.parseInt(args[4]), rf);
                    System.out.println("Created " + args[3] + " with " + args[4] + " partitions, replication factor " + rf);
                    describe(client);
                }
                case "produce" -> {
                    long offset = client.send(args[3], Integer.parseInt(args[4]), args[5].getBytes(StandardCharsets.UTF_8));
                    System.out.println(args[3] + "-" + args[4] + "@" + offset);
                }
                case "produce-batch" -> {
                    client.setLeaderRetryDeadlineMs(2_000); // report an offline partition rather than wait it out
                    int partitions = client.getNumPartitions(args[3]);
                    int count = Integer.parseInt(args[4]);
                    int failed = 0;
                    for (int i = 0; i < count; i++) {
                        try {
                            client.send(args[3], i % partitions, ("message-" + i).getBytes(StandardCharsets.UTF_8));
                        } catch (java.io.IOException e) {
                            failed++;
                        }
                    }
                    System.out.println("Produced " + (count - failed) + "/" + count + " to " + args[3]
                            + (failed > 0 ? " (" + failed + " failed: partition offline)" : ""));
                }
                case "consume" -> {
                    long from = args.length > 5 ? Long.parseLong(args[5]) : 0;
                    for (SimpleKafkaClient.FetchedRecord r : client.fetchRecords(args[3], Integer.parseInt(args[4]), from, 1 << 20)) {
                        System.out.println(args[3] + "-" + args[4] + "@" + r.offset() + "  "
                                + new String(r.payload(), StandardCharsets.UTF_8));
                    }
                }
                case "consume-group" -> {
                    AtomicInteger received = new AtomicInteger();
                    GroupConsumer member = new GroupConsumer(client, args[3], "cli-" + ProcessHandle.current().pid(),
                            Set.of(args[4]), (msg, offset) -> {
                                received.incrementAndGet();
                                System.out.println("@" + offset + "  " + new String(msg, StandardCharsets.UTF_8));
                            }, 10_000, 500);
                    member.start();
                    Thread.sleep(Long.parseLong(args[5]) * 1000);
                    System.out.println("Group " + args[3] + " member owned " + member.assignment() + ", received " + received.get());
                    member.leaveGroup();
                }
                case "commit" -> {
                    client.commitOffset(args[3], args[4], Integer.parseInt(args[5]), Long.parseLong(args[6]), "cli");
                    System.out.println("Committed " + args[3] + " " + args[4] + "-" + args[5] + " -> " + args[6]);
                }
                case "consume-with-dlq" -> consumeWithDlq(client, args[3], Integer.parseInt(args[4]),
                        Long.parseLong(args[5]));
                case "forget-entity" -> {
                    client.forgetEntity(args[3]);
                    System.out.println("Forgot entity " + args[3]);
                }
                default -> {
                    System.err.println("Unknown command " + command);
                    System.exit(2);
                }
            }
        }
        System.exit(0);
    }

    private static void consumeWithDlq(SimpleKafkaClient client, String topic, int partition, long seconds)
            throws Exception {
        AtomicInteger processed = new AtomicInteger();
        com.simplekafka.client.DeadLetterRouter router = new com.simplekafka.client.DeadLetterRouter(
                new com.simplekafka.client.RetryPolicy(2, 50, 2.0, 200),
                (dlqTopic, envelope) -> client.send(dlqTopic, 0, envelope));
        com.simplekafka.client.SimpleKafkaConsumer consumer =
                new com.simplekafka.client.SimpleKafkaConsumer(client, topic, partition, null);
        consumer.startConsuming((msg, offset) -> {
            String text = new String(msg, StandardCharsets.UTF_8);
            if (text.contains("POISON")) {
                throw new IllegalArgumentException("Unparseable order at offset " + offset + ": " + text);
            }
            processed.incrementAndGet();
        }, router);
        Thread.sleep(seconds * 1000);
        consumer.stopConsuming();
        System.out.println("Processed " + processed.get() + " records; consumer position " + consumer.getCommittedOffset()
                + " (not stuck behind the poison pill)");
        String dlq = topic + ".dlq";
        client.refreshMetadata();
        for (SimpleKafkaClient.FetchedRecord r : client.fetchRecords(dlq, 0, 0, 1 << 20)) {
            com.simplekafka.client.DeadLetterEnvelope e = com.simplekafka.client.DeadLetterEnvelope.deserialize(r.payload());
            System.out.println(dlq + "@" + r.offset() + "  from " + e.getOriginalTopic() + "-" + e.getOriginalPartition()
                    + "@" + e.getOriginalOffset() + "  " + e.getExceptionClassName() + ": " + e.getDeathReason()
                    + "  payload=\"" + new String(e.getOriginalPayload(), StandardCharsets.UTF_8) + "\"");
        }
    }

    private static void describe(SimpleKafkaClient client) throws Exception {
        client.refreshMetadata();
        System.out.println("Brokers: " + client.getBrokers().values());
        for (Map.Entry<String, List<SimpleKafkaClient.PartitionLayout>> t : client.topicLayouts().entrySet()) {
            if (t.getKey().startsWith("__")) {
                continue;
            }
            StringBuilder line = new StringBuilder("  " + t.getKey() + ":");
            for (SimpleKafkaClient.PartitionLayout p : t.getValue()) {
                line.append(" p").append(p.partition()).append("->")
                        .append(p.leader() < 0 ? "OFFLINE" : "broker" + p.leader())
                        .append(p.replicas().size() > 1 ? " (replicas " + p.replicas() + ")" : "");
            }
            System.out.println(line);
        }
    }
}
