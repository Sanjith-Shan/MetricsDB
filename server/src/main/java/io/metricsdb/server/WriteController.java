package io.metricsdb.server;

import io.metricsdb.ingest.Exposition;
import io.metricsdb.ingest.LineProtocol;
import io.metricsdb.ingest.RemoteWrite;
import io.metricsdb.storage.WriteBatch;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/** Ingest endpoints: InfluxDB line protocol, Prometheus remote write, and Prometheus text format. */
@RestController
public class WriteController {
    private static final int CHUNK_SAMPLES = 50_000;
    private final NodeConfig.Node node;
    private final NodeMetrics metrics;
    private final ThreadLocal<LineProtocol> parsers = ThreadLocal.withInitial(LineProtocol::new);

    public WriteController(NodeConfig.Node node, NodeMetrics metrics) {
        this.node = node;
        this.metrics = metrics;
    }

    private static byte[] body(HttpServletRequest req) throws IOException {
        InputStream in = req.getInputStream();
        String enc = req.getHeader("Content-Encoding");
        if (enc != null && enc.equalsIgnoreCase("gzip")) in = new GZIPInputStream(in);
        return in.readAllBytes();
    }

    @PostMapping({"/write", "/api/v2/write", "/influx/write"})
    public ResponseEntity<String> write(HttpServletRequest req, @RequestParam(defaultValue = "ns") String precision) throws IOException {
        long t0 = System.nanoTime();
        byte[] b = body(req);
        long div = switch (precision) {
            case "ns", "n" -> 1_000_000L;
            case "us", "u" -> 1_000L;
            case "ms" -> 1L;
            case "s" -> -1_000L;
            default -> throw new IllegalArgumentException("unknown precision " + precision);
        };
        WriteBatch batch = new WriteBatch(Math.max(64, b.length / 40));
        LineProtocol.Stats st = parsers.get().parse(b, b.length, div, batch);
        ResponseEntity<String> resp = store(batch, "influx");
        metrics.recordNanos("metricsdb.ingest.request", System.nanoTime() - t0, "protocol", "influx");
        if (st.errors > 0 && resp.getStatusCode().is2xxSuccessful()) {
            return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_JSON)
                    .body(Json.error("bad_data", st.errors + " lines failed to parse; first: " + st.firstError));
        }
        return resp;
    }

    @PostMapping("/api/v1/write")
    public ResponseEntity<String> remoteWrite(HttpServletRequest req) throws IOException {
        long t0 = System.nanoTime();
        byte[] b = req.getInputStream().readAllBytes();
        WriteBatch batch = new WriteBatch(1024);
        try {
            RemoteWrite.decode(b, batch);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Json.error("bad_data", "remote write: " + e.getMessage()));
        }
        ResponseEntity<String> resp = store(batch, "remote_write");
        metrics.recordNanos("metricsdb.ingest.request", System.nanoTime() - t0, "protocol", "remote_write");
        return resp;
    }

    @PostMapping("/api/v1/import/prometheus")
    public ResponseEntity<String> importText(HttpServletRequest req) throws IOException {
        WriteBatch batch = new WriteBatch(1024);
        Exposition.parse(new String(body(req), StandardCharsets.UTF_8), System.currentTimeMillis(), null, batch);
        return store(batch, "prometheus_text");
    }

    /** Large bodies are stored in slices so one request never pins a huge WAL record. */
    ResponseEntity<String> store(WriteBatch batch, String protocol) {
        Ingester.Result total = new Ingester.Result(0, 0, 0, 0, null);
        for (int from = 0; from < batch.n; from += CHUNK_SAMPLES) {
            int to = Math.min(batch.n, from + CHUNK_SAMPLES);
            WriteBatch part = slice(batch, from, to);
            total = Ingester.Result.merge(total, node.ingester().write(part));
        }
        metrics.counter("metricsdb.ingest.requests", "protocol", protocol).increment();
        if (total.unavailable() > 0) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).contentType(MediaType.APPLICATION_JSON)
                    .body(Json.error("unavailable", total.detail()));
        }
        if (total.overCardinality() > 0) {
            metrics.counter("metricsdb.ingest.rejected", "reason", "cardinality").increment(total.overCardinality());
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).contentType(MediaType.APPLICATION_JSON)
                    .body(Json.error("cardinality_limit", total.detail()));
        }
        if (total.tooOld() > 0) {
            metrics.counter("metricsdb.ingest.rejected", "reason", "too_old").increment(total.tooOld());
            // Prometheus treats out-of-bounds samples as a client error that must not be retried
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                    .body(Json.error("too_old", total.detail()));
        }
        return ResponseEntity.noContent().build();
    }

    static WriteBatch slice(WriteBatch b, int from, int to) {
        if (from == 0 && to == b.n) return b;
        WriteBatch out = new WriteBatch(to - from);
        for (int i = from; i < to; i++) out.add(b.labels[i], b.t[i], b.v[i]);
        return out;
    }
}
