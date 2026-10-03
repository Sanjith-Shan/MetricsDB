package io.metricsdb.bench;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** Benchmark queries dumped by {@code tsbs-dump}: one JSON-lines file per query type. */
final class Queries {
    record Q(String type, String label, String path) {}

    static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5)).build();

    static List<Q> load(String dir, String only) throws IOException {
        List<Q> out = new ArrayList<>();
        try (Stream<Path> s = Files.list(Path.of(dir))) {
            for (Path p : s.sorted().toList()) {
                String name = p.getFileName().toString();
                if (!name.endsWith(".jsonl")) continue;
                String type = name.substring(0, name.length() - 6);
                if (only != null && !only.isEmpty() && !List.of(only.split(",")).contains(type)) continue;
                for (String line : Files.readAllLines(p)) {
                    if (line.isBlank()) continue;
                    JsonNode n = Env.JSON.readTree(line);
                    out.add(new Q(type, n.get("label").asText(), n.get("path").asText()));
                }
            }
        }
        return out;
    }

    record Resp(int status, String body, long micros) {}

    static Resp get(String base, String path, Duration timeout) {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + path)).timeout(timeout).GET().build();
        long t0 = System.nanoTime();
        try {
            HttpResponse<String> r = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return new Resp(r.statusCode(), r.body(), (System.nanoTime() - t0) / 1000);
        } catch (Exception e) {
            return new Resp(-1, e.toString(), (System.nanoTime() - t0) / 1000);
        }
    }
}
