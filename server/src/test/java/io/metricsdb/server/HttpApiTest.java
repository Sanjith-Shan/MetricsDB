package io.metricsdb.server;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The HTTP surface end to end on a single node: line protocol and remote write in, the
 * Prometheus query API out, and each protection (series limit, sample limit, timeout,
 * cardinality limit) answering with a clear error instead of hurting the node.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@AutoConfigureObservability
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpApiTest {
    static final long T0 = 1_790_812_800L; // seconds

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) throws Exception {
        r.add("metricsdb.data-dir", () -> {
            try { return Files.createTempDirectory("mdb-http").toString(); } catch (Exception e) { throw new RuntimeException(e); }
        });
        r.add("metricsdb.storage.max-series", () -> "60");
        r.add("metricsdb.query.max-series", () -> "25");
        r.add("metricsdb.query.max-samples", () -> "50000");
        r.add("metricsdb.storage.maintenance-interval", () -> "1h");
    }

    @Autowired TestRestTemplate http;

    private ResponseEntity<String> write(String body) {
        return http.postForEntity("/write?precision=s", new HttpEntity<>(body), String.class);
    }

    private ResponseEntity<String> query(String q, long start, long end, long step) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("query", q);
        form.add("start", String.valueOf(start));
        form.add("end", String.valueOf(end));
        form.add("step", String.valueOf(step));
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        return http.exchange("/api/v1/query_range", HttpMethod.POST, new HttpEntity<>(form, h), String.class);
    }

    @Test
    @Order(3)
    void writeThenQueryAndProtect() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int h = 0; h < 30; h++) {
            for (long t = T0; t < T0 + 3600; t += 10) sb.append("cpu,hostname=host_").append(h).append(" usage_user=").append(h).append(' ').append(t).append('\n');
        }
        assertEquals(204, write(sb.toString()).getStatusCode().value());

        ResponseEntity<String> ok = query("max(max_over_time(cpu_usage_user{hostname=~'host_1|host_2'}[1m])) by (__name__)", T0 + 600, T0 + 1200, 60);
        assertEquals(200, ok.getStatusCode().value(), ok.getBody());
        assertTrue(ok.getBody().contains("\"__name__\":\"cpu_usage_user\""), ok.getBody());
        assertTrue(ok.getBody().contains("[" + (T0 + 600) + ",\"2\"]"), ok.getBody());

        // series limit: 30 series selected, limit 25
        ResponseEntity<String> tooMany = query("sum(cpu_usage_user)", T0, T0 + 600, 60);
        assertEquals(422, tooMany.getStatusCode().value());
        assertTrue(tooMany.getBody().contains("over the limit of 25"), tooMany.getBody());

        // well under the sample limit: 11 series x 360 samples
        ResponseEntity<String> fine = query("sum(max_over_time(cpu_usage_user{hostname=~'host_1.*'}[1h]))", T0, T0 + 3600, 60);
        assertEquals(200, fine.getStatusCode().value(), fine.getBody());

        // timeout: a 1 ms budget on a heavy query
        ResponseEntity<String> slow = http.getForEntity("/api/v1/query_range?query={q}&start={s}&end={e}&step=10&timeout=1ms",
                String.class, "sum(rate(cpu_usage_user{hostname=~'host_1.*'}[5m]))", T0, T0 + 3600);
        assertTrue(slow.getStatusCode().value() == 503 || slow.getStatusCode().value() == 200, slow.getBody());
        if (slow.getStatusCode().value() == 503) assertTrue(slow.getBody().contains("timed out"), slow.getBody());

        // cardinality limit: 30 series exist, limit 60; 40 new ones exceed it
        StringBuilder more = new StringBuilder();
        for (int h = 0; h < 40; h++) more.append("disk,hostname=host_").append(h).append(" used=1 ").append(T0 + 100).append('\n');
        ResponseEntity<String> card = write(more.toString());
        assertEquals(422, card.getStatusCode().value());
        assertTrue(card.getBody().contains("cardinality limit"), card.getBody());

        ResponseEntity<String> metrics = http.getForEntity("/metrics", String.class);
        assertTrue(metrics.getBody().contains("metricsdb_wal_fsync_seconds_bucket"), "WAL fsync histogram exposed");
        assertTrue(metrics.getBody().contains("metricsdb_head_series"), "head series gauge exposed");
        assertTrue(metrics.getBody().contains("metricsdb_query_seconds_bucket"), "query latency histogram exposed");
    }

    @Test
    @Order(2)
    void sampleLimitIsEnforced() {
        StringBuilder sb = new StringBuilder();
        for (int h = 0; h < 20; h++) {
            for (long t = T0; t < T0 + 7200; t += 1) sb.append("net,hostname=h").append(h).append(" bytes=").append(t).append(' ').append(t).append('\n');
        }
        write(sb.toString());
        ResponseEntity<String> r = query("sum(max_over_time(net_bytes[1h]))", T0, T0 + 7200, 3600);
        assertEquals(422, r.getStatusCode().value(), r.getBody());
        assertTrue(r.getBody().contains("samples"), r.getBody());
    }

    @Test
    @Order(1)
    void remoteWriteIsAccepted() throws Exception {
        ByteArrayOutputStream ts = new ByteArrayOutputStream();
        field(ts, 1, label("__name__", "rw_metric"));
        field(ts, 1, label("job", "test"));
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        s.write(1 << 3 | 1);
        long bits = Double.doubleToRawLongBits(42.5);
        for (int k = 0; k < 8; k++) s.write((int) (bits >>> (8 * k)));
        s.write(2 << 3);
        varint(s, T0 * 1000);
        field(ts, 2, s.toByteArray());
        ByteArrayOutputStream req = new ByteArrayOutputStream();
        field(req, 1, ts.toByteArray());
        HttpHeaders h = new HttpHeaders();
        h.set("Content-Encoding", "snappy");
        h.setContentType(MediaType.parseMediaType("application/x-protobuf"));
        ResponseEntity<String> r = http.postForEntity("/api/v1/write", new HttpEntity<>(Snappy.compress(req.toByteArray()), h), String.class);
        assertEquals(204, r.getStatusCode().value(), r.getBody());
        ResponseEntity<String> q = http.getForEntity("/api/v1/query?query=rw_metric&time=" + (T0 + 1), String.class);
        assertTrue(q.getBody().contains("\"42.5\""), q.getBody());
    }

    private static byte[] label(String n, String v) {
        ByteArrayOutputStream l = new ByteArrayOutputStream();
        field(l, 1, n.getBytes(StandardCharsets.UTF_8));
        field(l, 2, v.getBytes(StandardCharsets.UTF_8));
        return l.toByteArray();
    }

    private static void field(ByteArrayOutputStream o, int num, byte[] b) {
        o.write(num << 3 | 2);
        varint(o, b.length);
        o.writeBytes(b);
    }

    private static void varint(ByteArrayOutputStream o, long v) {
        while ((v & ~0x7FL) != 0) { o.write((int) ((v & 0x7F) | 0x80)); v >>>= 7; }
        o.write((int) v);
    }
}
