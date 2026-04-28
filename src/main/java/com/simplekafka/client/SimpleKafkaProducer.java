package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class SimpleKafkaProducer {
    private final SimpleKafkaClient client;
    private final String topic;
    private final Random random;

    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.random = new Random();
    }

    public void initialize() throws IOException {
        client.initialize();
        try {
            client.getNumPartitions(topic);
        } catch (IOException e) {
            System.out.println("Topic not found, creating topic: " + topic);
            client.createTopic(topic, 3, (short) 1);
            try { Thread.sleep(2000); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
            client.refreshMetadata();
        }
    }

    public long send(String message) throws IOException {
        int numPartitions = client.getNumPartitions(topic);
        if (numPartitions == 0) {
            throw new IOException("No partitions available for topic: " + topic);
        }
        int partition = random.nextInt(numPartitions);
        return send(message, partition);
    }

    public long send(String message, int partition) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        return client.send(topic, partition, data);
    }

    public void close() {
        // In a real implementation this would close connections and release resources.
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: SimpleKafkaProducer <host> <port> <topic>");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        
        SimpleKafkaProducer producer = new SimpleKafkaProducer(host, port, topic);
        producer.initialize();
        
        for (int i = 0; i < 10; i++) {
            String message = "Test message " + i;
            long offset = producer.send(message);
            System.out.println("Sent message: '" + message + "' at offset: " + offset);
            Thread.sleep(500);
        }
        producer.close();
    }
}
