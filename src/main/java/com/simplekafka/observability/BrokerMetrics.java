package com.simplekafka.observability;

import io.micrometer.core.instrument.*;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Broker metrics with bounded cardinality.
 *
 * Label policy: per topic and per (group, topic), never per partition or per client. Each
 * labeled family is capped by a {@link CardinalityLimiter}; overflow aggregates into
 * {@code __other__}. Per-partition detail is served on demand by {@code GET /admin/lag}.
 *
 * Series budget: request timers (~3 x APIs) + 2 x {@value #MAX_TOPICS} topic counters +
 * {@value #MAX_GROUP_TOPICS} lag gauges + a few broker gauges, i.e. under 500 regardless of
 * how many topics or groups clients create.
 */
public class BrokerMetrics {
    static final int MAX_TOPICS = 120;
    static final int MAX_GROUP_TOPICS = 150;

    public record LagRow(String group, String topic, long lag) {}

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final CardinalityLimiter topics = new CardinalityLimiter(MAX_TOPICS);
    private final CardinalityLimiter groupTopics = new CardinalityLimiter(MAX_GROUP_TOPICS);
    private final Map<String, Timer> requestTimers = new ConcurrentHashMap<>();
    private final Map<String, Counter[]> topicCounters = new ConcurrentHashMap<>();
    private final MultiGauge groupLag;
    private volatile Supplier<List<LagRow>> lagSource = List::of;

    public BrokerMetrics(int brokerId) {
        registry.config().commonTags("broker", String.valueOf(brokerId));
        groupLag = MultiGauge.builder("stormlog.group.lag.records")
                .description("Committed-offset lag summed over a group's partitions of a topic (locally hosted partitions)")
                .register(registry);
    }

    public void recordRequest(String api, long nanos, boolean error) {
        requestTimers.computeIfAbsent(api + (error ? "|error" : "|ok"), k -> Timer.builder("stormlog.request.latency")
                        .description("Broker-side request handling time")
                        .tag("api", api)
                        .tag("outcome", error ? "error" : "ok")
                        .register(registry))
                .record(nanos, TimeUnit.NANOSECONDS);
    }

    public void recordProduce(String topic, int bytes) {
        Counter[] counters = topicCounters.computeIfAbsent(topics.limit(topic), t -> new Counter[]{
                Counter.builder("stormlog.topic.records.in").tag("topic", t).register(registry),
                Counter.builder("stormlog.topic.bytes.in").baseUnit("bytes").tag("topic", t).register(registry)});
        counters[0].increment();
        counters[1].increment(bytes);
    }

    public void gauge(String name, String description, Supplier<Number> value) {
        Gauge.builder(name, value).description(description).register(registry);
    }

    public void setLagSource(Supplier<List<LagRow>> lagSource) {
        this.lagSource = lagSource;
    }

    /** Recomputes lag gauges, then renders Prometheus text exposition. */
    public String scrape() {
        Map<List<String>, Long> aggregated = new TreeMap<>(Comparator.comparing((List<String> l) -> l.get(0))
                .thenComparing(l -> l.get(1)));
        for (LagRow row : lagSource.get()) {
            boolean admitted = !CardinalityLimiter.OVERFLOW.equals(groupTopics.limit(row.group() + "\u0000" + row.topic()));
            List<String> key = admitted ? List.of(row.group(), row.topic())
                    : List.of(CardinalityLimiter.OVERFLOW, CardinalityLimiter.OVERFLOW);
            aggregated.merge(key, row.lag(), Long::sum);
        }
        List<MultiGauge.Row<?>> rows = new ArrayList<>();
        aggregated.forEach((key, lag) -> rows.add(MultiGauge.Row.of(Tags.of("group", key.get(0), "topic", key.get(1)), lag)));
        groupLag.register(rows, true);
        return registry.scrape();
    }

    public MeterRegistry registry() {
        return registry;
    }
}
