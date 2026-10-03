package io.metricsdb.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The same benchmark-shaped data written to MetricsDB and to a real VictoriaMetrics container;
 * the benchmark's eleven PromQL query shapes are sent to both and the answers must match: same
 * series, same timestamps, values within 1e-9 relative.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class VictoriaMetricsParityIT {
    static final long T0 = 1_790_812_800L;
    static final String[] CPU = {"usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait",
            "usage_irq", "usage_softirq", "usage_steal", "usage_guest", "usage_guest_nice"};

    @Container
    static final GenericContainer<?> VM = new GenericContainer<>("victoriametrics/victoria-metrics:v1.153.0")
            .withExposedPorts(8428)
            .withCommand("-retentionPeriod=100y", "-search.latencyOffset=0s")
            .waitingFor(Wait.forHttp("/health").forPort(8428));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("metricsdb.data-dir", () -> {
            try { return Files.createTempDirectory("mdb-parity").toString(); } catch (Exception e) { throw new RuntimeException(e); }
        });
    }

    @Autowired TestRestTemplate http;
    final HttpClient client = HttpClient.newHttpClient();
    final ObjectMapper json = new ObjectMapper();

    String vm() { return "http://" + VM.getHost() + ":" + VM.getMappedPort(8428); }

    @Test
    void answersMatchVictoriaMetrics() throws Exception {
        Random r = new Random(5);
        StringBuilder sb = new StringBuilder();
        int hosts = 10;
        int[][] state = new int[hosts][CPU.length];
        for (long t = T0; t < T0 + 6 * 3600; t += 10) {
            for (int h = 0; h < hosts; h++) {
                sb.append("cpu,hostname=host_").append(h).append(",region=r").append(h % 3).append(' ');
                for (int f = 0; f < CPU.length; f++) {
                    state[h][f] = Math.max(0, Math.min(100, state[h][f] + r.nextInt(5) - 2));
                    if (f > 0) sb.append(',');
                    sb.append(CPU[f]).append('=').append(state[h][f]).append('i');
                }
                sb.append(' ').append(t).append("000000000\n");
            }
        }
        String body = sb.toString();
        assertEquals(204, http.postForEntity("/write", new HttpEntity<>(body), String.class).getStatusCode().value());
        HttpResponse<String> w = client.send(HttpRequest.newBuilder(URI.create(vm() + "/write"))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(204, w.statusCode());
        client.send(HttpRequest.newBuilder(URI.create(vm() + "/internal/force_flush")).build(), HttpResponse.BodyHandlers.ofString());
        Thread.sleep(2000);

        List<String[]> queries = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            long start = T0 + 1800 + r.nextInt(3600);
            String h1 = "host_" + r.nextInt(hosts);
            String h8 = String.join("|", "host_1", "host_2", "host_3", "host_4", "host_5", "host_6", "host_7", "host_8");
            queries.add(q("max(max_over_time(cpu_usage_user{hostname='" + h1 + "'}[1m])) by (__name__)", start, start + 3600, 60));
            queries.add(q("max(max_over_time({__name__=~'cpu_(usage_user|usage_system|usage_idle|usage_nice|usage_iowait)', hostname=~'" + h8 + "'}[1m])) by (__name__)", start, start + 3600, 60));
            queries.add(q("max(max_over_time({__name__=~'cpu_(usage_user|usage_system)', hostname='" + h1 + "'}[1m])) by (__name__)", start - 1800, start + 3 * 3600, 60));
            queries.add(q("max(max_over_time({__name__=~'cpu_(" + String.join("|", CPU) + ")', hostname=~'" + h8 + "'}[1h])) by (__name__)", start, start + 4 * 3600, 3600));
            queries.add(q("avg(avg_over_time({__name__=~'cpu_(usage_user|usage_system|usage_idle|usage_nice|usage_iowait)'}[1h])) by (__name__, hostname)", start, start + 4 * 3600, 3600));
            queries.add(q("avg(avg_over_time(cpu_usage_user{}[1h])) by (__name__, hostname)", start, start + 4 * 3600, 3600));
        }
        int mismatches = 0, points = 0;
        List<String> why = new ArrayList<>();
        for (String[] q : queries) {
            String path = "/api/v1/query_range?query=" + URLEncoder.encode(q[0], StandardCharsets.UTF_8) + "&start=" + q[1] + "&end=" + q[2] + "&step=" + q[3];
            String a = client.send(HttpRequest.newBuilder(URI.create(http.getRootUri() + path)).build(), HttpResponse.BodyHandlers.ofString()).body();
            String b = client.send(HttpRequest.newBuilder(URI.create(vm() + path)).build(), HttpResponse.BodyHandlers.ofString()).body();
            Map<String, JsonNode> ma = series(json.readTree(a)), mb = series(json.readTree(b));
            if (!ma.keySet().equals(mb.keySet())) { mismatches++; why.add(q[0] + ": series " + ma.keySet() + " vs " + mb.keySet()); continue; }
            for (String k : ma.keySet()) {
                JsonNode va = ma.get(k), vb = mb.get(k);
                if (va.size() != vb.size()) { mismatches++; why.add(q[0] + ": " + va.size() + " vs " + vb.size() + " points"); break; }
                boolean bad = false;
                for (int i = 0; i < va.size() && !bad; i++) {
                    points++;
                    double x = Double.parseDouble(va.get(i).get(1).asText()), y = Double.parseDouble(vb.get(i).get(1).asText());
                    if (va.get(i).get(0).asDouble() != vb.get(i).get(0).asDouble() || Math.abs(x - y) > 1e-9 * Math.max(1, Math.abs(y))) {
                        bad = true;
                        why.add(q[0] + " @" + va.get(i).get(0) + ": " + x + " vs " + vb.get(i).get(0) + ":" + y);
                    }
                }
                if (bad) { mismatches++; break; }
            }
        }
        assertTrue(points > 1000, "compared " + points + " points");
        assertEquals(0, mismatches, String.join("\n", why));
    }

    static String[] q(String query, long start, long end, long step) {
        return new String[]{query, String.valueOf(start), String.valueOf(end), String.valueOf(step)};
    }

    static Map<String, JsonNode> series(JsonNode resp) {
        Map<String, JsonNode> out = new TreeMap<>();
        for (JsonNode s : resp.path("data").path("result")) {
            TreeMap<String, String> m = new TreeMap<>();
            s.get("metric").fields().forEachRemaining(f -> m.put(f.getKey(), f.getValue().asText()));
            out.put(m.toString(), s.get("values"));
        }
        return out;
    }
}
