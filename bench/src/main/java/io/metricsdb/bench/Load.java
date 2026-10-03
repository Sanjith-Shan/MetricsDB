package io.metricsdb.bench;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.HdrHistogram.Histogram;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Replays a line-protocol file into any database with an InfluxDB-style /write endpoint, with
 * fixed batches and a fixed number of concurrent writers, and records the latency of every
 * batch. The same tool and the same file drive MetricsDB, VictoriaMetrics and InfluxDB, so the
 * only difference between runs is the database. Failed batches are retried and counted.
 */
public final class Load {
    record Batch(byte[] data, int len, long samples, int lines) {}

    public static void run(Args a) throws Exception {
        String file = a.req("file");
        String url = a.req("url");
        int workers = a.i("workers", 4);
        int batchLines = a.i("batch-lines", 1000);
        long limitLines = a.l("limit-lines", Long.MAX_VALUE);
        String label = a.str("label", "load");
        HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(5)).build();
        ArrayBlockingQueue<Batch> q = new ArrayBlockingQueue<>(workers * 4);
        Batch poison = new Batch(new byte[0], 0, 0, 0);
        Histogram lat = new Histogram(60_000_000L, 3);
        AtomicLong samples = new AtomicLong(), lines = new AtomicLong(), retries = new AtomicLong(), errors = new AtomicLong();
        String[] firstError = new String[1];
        Thread[] ws = new Thread[workers];
        long t0 = System.nanoTime();
        for (int w = 0; w < workers; w++) {
            ws[w] = Thread.ofPlatform().start(() -> {
                try {
                    while (true) {
                        Batch b = q.take();
                        if (b == poison) return;
                        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(120))
                                .POST(HttpRequest.BodyPublishers.ofByteArray(b.data, 0, b.len)).build();
                        long s = System.nanoTime();
                        int attempt = 0;
                        while (true) {
                            try {
                                HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
                                if (r.statusCode() / 100 == 2) break;
                                errors.incrementAndGet();
                                synchronized (firstError) { if (firstError[0] == null) firstError[0] = r.statusCode() + " " + r.body(); }
                                if (r.statusCode() / 100 == 4) break; // a client error will not succeed on retry
                            } catch (Exception e) {
                                errors.incrementAndGet();
                                synchronized (firstError) { if (firstError[0] == null) firstError[0] = e.toString(); }
                            }
                            retries.incrementAndGet();
                            Thread.sleep(Math.min(2000, 50L << Math.min(5, attempt++)));
                        }
                        synchronized (lat) { lat.recordValue(Math.min(lat.getHighestTrackableValue(), (System.nanoTime() - s) / 1000)); }
                        samples.addAndGet(b.samples);
                        lines.addAndGet(b.lines);
                    }
                } catch (InterruptedException ignored) {
                }
            });
        }
        // reader: cut the file into batches of whole lines, counting fields as samples
        try (InputStream in = new BufferedInputStream(new FileInputStream(file), 1 << 20)) {
            byte[] buf = new byte[1 << 16];
            byte[] cur = new byte[1 << 20];
            int len = 0, nLines = 0;
            long nSamples = 0, total = 0;
            int lineStart = 0;
            int r;
            outer:
            while ((r = in.read(buf)) > 0) {
                for (int i = 0; i < r; i++) {
                    if (len == cur.length) cur = Arrays.copyOf(cur, cur.length * 2);
                    cur[len++] = buf[i];
                    if (buf[i] == '\n') {
                        nSamples += fields(cur, lineStart, len - 1);
                        nLines++;
                        total++;
                        lineStart = len;
                        if (nLines == batchLines || total >= limitLines) {
                            q.put(new Batch(cur, len, nSamples, nLines));
                            cur = new byte[Math.max(1 << 16, len + len / 4)];
                            len = 0;
                            nLines = 0;
                            nSamples = 0;
                            lineStart = 0;
                            if (total >= limitLines) break outer;
                        }
                    }
                }
            }
            if (nLines > 0) q.put(new Batch(cur, len, nSamples, nLines));
        }
        for (int w = 0; w < workers; w++) q.put(poison);
        for (Thread w : ws) w.join();
        double secs = (System.nanoTime() - t0) / 1e9;
        ObjectNode row = Env.JSON.createObjectNode();
        row.put("exp", a.str("exp", "load"));
        row.put("label", label);
        row.put("ts", Env.now());
        row.put("url", url);
        row.put("file", baseName(file));
        row.put("workers", workers);
        row.put("batch_lines", batchLines);
        row.put("lines", lines.get());
        row.put("samples", samples.get());
        row.put("seconds", Math.round(secs * 100) / 100.0);
        row.put("samples_per_s", Math.round(samples.get() / secs));
        row.put("retries", retries.get());
        row.put("errors", errors.get());
        if (firstError[0] != null) row.put("first_error", firstError[0].length() > 300 ? firstError[0].substring(0, 300) : firstError[0]);
        row.set("batch_latency_ms", Env.hist(lat, 1000.0));
        row.set("machine", Env.machine());
        row.set("load_after", Env.load());
        Env.append(a.str("out", null), row);
    }

    private static String baseName(String f) { return java.nio.file.Path.of(f).getFileName().toString(); }

    /** Number of fields in one line: commas in the field section plus one. */
    static int fields(byte[] b, int s, int e) {
        int p = s;
        // skip measurement and tags (up to the first unescaped space)
        while (p < e && b[p] != ' ') { if (b[p] == '\\') p++; p++; }
        p++;
        int n = 1;
        boolean inStr = false;
        for (; p < e; p++) {
            byte c = b[p];
            if (c == '"') inStr = !inStr;
            else if (!inStr && c == ',') n++;
            else if (!inStr && c == ' ') break;
            else if (c == '\\') p++;
        }
        return p >= s ? n : 0;
    }
}
