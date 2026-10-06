package com.simplekafka.observability;

import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * GET /metrics            Prometheus text exposition (bounded cardinality)
 * GET /admin/lag[?group=] per-partition lag drill-down as JSON (unbounded, on demand only)
 */
public class AdminHttpServer implements AutoCloseable {
    public record PartitionLag(String group, String topic, int partition, long committedOffset,
                               long logEndOffset, long lag) {}

    private final HttpServer server;

    public AdminHttpServer(String host, int port, BrokerMetrics metrics, Function<String, List<PartitionLag>> lagByGroup)
            throws IOException {
        server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.setExecutor(Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "admin-http");
            t.setDaemon(true);
            return t;
        }));
        server.createContext("/metrics", ex -> respond(ex, 200, "text/plain; version=0.0.4; charset=utf-8", metrics.scrape()));
        server.createContext("/admin/lag", ex -> {
            String group = query(ex).get("group");
            List<PartitionLag> rows = lagByGroup.apply(group);
            respond(ex, 200, "application/json", new GsonBuilder().setPrettyPrinting().create().toJson(rows));
        });
        server.start();
    }

    /** A handler returns [status, json body]; query parameters are pre-parsed. */
    @FunctionalInterface
    public interface Route {
        Object[] handle(String method, Map<String, String> params) throws Exception;
    }

    /** Registers an extra admin endpoint (e.g. quorum membership). */
    public void route(String path, Route route) {
        server.createContext(path, ex -> {
            try {
                Object[] result = route.handle(ex.getRequestMethod(), query(ex));
                respond(ex, (Integer) result[0], "application/json", (String) result[1]);
            } catch (Exception e) {
                respond(ex, 500, "application/json", "{\"error\":\"" + String.valueOf(e.getMessage()).replace("\"", "'") + "\"}");
            }
        });
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> params = new HashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw != null) {
            for (String pair : raw.split("&")) {
                String[] kv = pair.split("=", 2);
                params.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8),
                        kv.length > 1 ? URLDecoder.decode(kv[1], StandardCharsets.UTF_8) : "");
            }
        }
        return params;
    }

    private static void respond(HttpExchange ex, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
