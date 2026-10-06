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
 *   create-topic TOPIC PARTITIONS
 *   produce TOPIC PARTITION MESSAGE
 *   produce-batch TOPIC COUNT              round-robin across partitions
 *   consume TOPIC PARTITION [FROM_OFFSET]  print what's there and exit
 *   consume-group GROUP TOPIC SECONDS      join a consumer group and print what it receives
 *   commit GROUP TOPIC PARTITION OFFSET
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
                    client.createTopic(args[3], Integer.parseInt(args[4]), (short) 1);
                    System.out.println("Created " + args[3] + " with " + args[4] + " partitions");
                    describe(client);
                }
                case "produce" -> {
                    long offset = client.send(args[3], Integer.parseInt(args[4]), args[5].getBytes(StandardCharsets.UTF_8));
                    System.out.println(args[3] + "-" + args[4] + "@" + offset);
                }
                case "produce-batch" -> {
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
                        .append(p.leader() < 0 ? "OFFLINE" : "broker" + p.leader());
            }
            System.out.println(line);
        }
    }
}
