package com.simplekafka.observability;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.coordinator.ConsumerOffsetStore;
import com.simplekafka.network.NettyBrokerServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class ObservabilityTest {

    @TempDir
    File tempDir;

    private SimpleKafkaBroker broker;
    private NettyBrokerServer server;
    private AdminHttpServer admin;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void setup() throws Exception {
        int[] port = new int[1];
        broker = new SimpleKafkaBroker(1, "127.0.0.1", 0, 2181) {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public List<BrokerInfo> liveBrokers() {
                return List.of(new BrokerInfo(1, "127.0.0.1", port[0]));
            }
            @Override protected String dataDir() {
                return tempDir.getAbsolutePath();
            }
        };
        List<Partition> offsets = new ArrayList<>();
        for (int i = 0; i < ConsumerOffsetStore.PARTITIONS; i++) {
            offsets.add(new Partition(i, new File(tempDir, ConsumerOffsetStore.TOPIC).getAbsolutePath()));
        }
        broker.getTopics().put(ConsumerOffsetStore.TOPIC, offsets);
        server = new NettyBrokerServer("127.0.0.1", 0, broker);
        server.start();
        port[0] = server.getPort();
        admin = new AdminHttpServer("127.0.0.1", 0, broker.getMetrics(), broker::partitionLag);
    }

    @AfterEach
    void teardown() {
        admin.close();
        server.stop();
        broker.getGroupCoordinator().close();
    }

    private String get(String path) throws Exception {
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + admin.port() + path)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    private static long seriesCount(String exposition) {
        return exposition.lines().filter(l -> !l.isBlank() && !l.startsWith("#")).count();
    }

    private void addTopic(String name) {
        broker.getTopics().put(name, new ArrayList<>(List.of(new Partition(0, new File(tempDir, name).getAbsolutePath()))));
    }

    @Test
    void testSeriesStayUnder500For100GroupsTimes100Topics() throws Exception {
        for (int t = 0; t < 100; t++) {
            addTopic("topic-" + t);
            broker.getMetrics().recordProduce("topic-" + t, 100);
            broker.getMetrics().recordRequest("produce", 1_000, false);
        }
        ConsumerOffsetStore store = broker.getOffsetStore();
        for (int g = 0; g < 100; g++) {
            for (int t = 0; t < 100; t++) { // worst case: every group consumes every topic
                store.commit("group-" + g, "topic-" + t, 0, 0, "");
            }
        }

        String metrics = get("/metrics");
        long series = seriesCount(metrics);
        System.out.println("Prometheus series for 100 groups x 100 topics: " + series);
        assertTrue(series < 500, "Expected < 500 series for 100 groups x 100 topics, got " + series);
        assertTrue(metrics.contains("group=\"" + CardinalityLimiter.OVERFLOW + "\""),
                "Group x topic pairs beyond the budget must aggregate into __other__");
        assertTrue(metrics.contains("stormlog_topic_records_in_total"));
    }

    @Test
    void testLagIsReportedInMetricsAndAdminDrillDown() throws Exception {
        addTopic("orders");
        try (SimpleKafkaClient client = new SimpleKafkaClient("127.0.0.1", server.getPort())) {
            client.initialize();
            for (int i = 0; i < 10; i++) {
                client.send("orders", 0, ("o" + i).getBytes(StandardCharsets.UTF_8));
            }
            client.commitOffset("billing", "orders", 0, 3, "");
        }

        String metrics = get("/metrics");
        assertTrue(metrics.lines().anyMatch(l -> l.startsWith("stormlog_group_lag_records{")
                        && l.contains("group=\"billing\"") && l.contains("topic=\"orders\"") && l.endsWith(" 7.0")),
                "Lag gauge for billing/orders must be 7:\n" + metrics);
        assertTrue(metrics.contains("stormlog_request_latency_seconds_count{api=\"produce\""));

        String lag = get("/admin/lag?group=billing");
        assertTrue(lag.contains("\"committedOffset\": 3"), lag);
        assertTrue(lag.contains("\"logEndOffset\": 10"), lag);
        assertTrue(lag.contains("\"lag\": 7"), lag);
    }

    @Test
    void testGroupLifecycleEventsLandInAuditTopic() throws Exception {
        addTopic("orders");
        var joined = broker.getGroupCoordinator().join("billing", "", "svc-1", Set.of("orders"), 10_000);
        broker.getGroupCoordinator().leave("billing", joined.memberId());

        Partition audit = broker.hostedPartition(AuditLog.TOPIC, 0);
        assertNotNull(audit, "__audit must be exposed as a topic");
        List<String> events = audit.readRecords(0, Integer.MAX_VALUE).stream()
                .map(r -> new String(r.payload(), StandardCharsets.UTF_8)).toList();
        assertTrue(events.stream().anyMatch(e -> e.contains("\"event\":\"GROUP_MEMBER_JOINED\"") && e.contains("svc-1")), events.toString());
        assertTrue(events.stream().anyMatch(e -> e.contains("\"event\":\"GROUP_REBALANCED\"")), events.toString());
        assertTrue(events.stream().anyMatch(e -> e.contains("\"event\":\"GROUP_MEMBER_LEFT\"")), events.toString());
    }

    @Test
    void testLimiterAdmitsFirstNThenOverflows() {
        CardinalityLimiter limiter = new CardinalityLimiter(2);
        assertEquals("a", limiter.limit("a"));
        assertEquals("b", limiter.limit("b"));
        assertEquals(CardinalityLimiter.OVERFLOW, limiter.limit("c"));
        assertEquals("a", limiter.limit("a"), "Admitted values stay admitted");
    }
}
