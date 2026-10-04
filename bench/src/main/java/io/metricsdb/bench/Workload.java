package io.metricsdb.bench;

import com.fasterxml.jackson.databind.JsonNode;
import io.metricsdb.cluster.HashRing;
import io.metricsdb.model.Labels;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A deterministic write workload for failure experiments: S series, one sample per series per
 * tick, value a pure function of (series, tick). Ticks are written through the router at a fixed
 * rate; a tick is acknowledged when the router answers 204. Failed ticks go to a retry queue (as
 * a real client would buffer them) so the write rate is kept. Every acknowledged sample is later
 * checked, bit for bit, through the router and on each replica.
 */
public final class Workload {
    public final int series;
    public final String run;
    public final long t0;
    public final long stepMs;
    private final String url;
    public final BitSet acked = new BitSet();
    public final AtomicLong firstTryOk = new AtomicLong(), firstTryFail = new AtomicLong(), retries = new AtomicLong();
    public final AtomicLong ackedSamples = new AtomicLong();
    private final ConcurrentLinkedQueue<Integer> retry = new ConcurrentLinkedQueue<>();
    public final List<long[]> perSecond = new ArrayList<>(); // [second, ticks ok first try, ticks failed first try]
    private volatile boolean stop;
    private int ticksSent;

    public Workload(String url, int series, String run, long t0, long stepMs) {
        this.url = url;
        this.series = series;
        this.run = run;
        this.t0 = t0;
        this.stepMs = stepMs;
    }

    public static double value(int s, int tick) {
        return ((s * 7919L + tick * 104729L) % 100_000) / 100.0;
    }

    public Labels labels(int s) {
        return Labels.of("__name__", "exp_value", "run", run, "series", "s" + s);
    }

    private String body(int tick) {
        StringBuilder sb = new StringBuilder(series * 48);
        long t = t0 + tick * stepMs;
        for (int s = 0; s < series; s++) {
            sb.append("exp,run=").append(run).append(",series=s").append(s).append(" value=").append(value(s, tick)).append(' ').append(t).append('\n');
        }
        return sb.toString();
    }

    private boolean send(int tick) {
        try {
            HttpResponse<String> r = Queries.HTTP.send(HttpRequest.newBuilder(URI.create(url + "/write?precision=ms"))
                    .timeout(Duration.ofSeconds(10)).POST(HttpRequest.BodyPublishers.ofString(body(tick))).build(),
                    HttpResponse.BodyHandlers.ofString());
            return r.statusCode() == 204;
        } catch (Exception e) {
            return false;
        }
    }

    private void ack(int tick) {
        synchronized (acked) { acked.set(tick); }
        ackedSamples.addAndGet(series);
    }

    /** Writes ticks at {@code ticksPerSecond} until {@link #stop()}; returns when stopped. */
    public void runLoader(double ticksPerSecond, long startNanos) throws InterruptedException {
        Thread retrier = Thread.ofPlatform().start(() -> {
            while (!stop || !retry.isEmpty()) {
                Integer t = retry.peek();
                if (t == null) { sleep(50); continue; }
                if (send(t)) { retry.poll(); ack(t); } else { retries.incrementAndGet(); sleep(200); }
            }
        });
        long periodNanos = (long) (1e9 / ticksPerSecond);
        int tick = 0;
        while (!stop) {
            long due = startNanos + tick * periodNanos;
            long now = System.nanoTime();
            if (due > now) Thread.sleep((due - now) / 1_000_000, (int) ((due - now) % 1_000_000));
            int sec = (int) ((System.nanoTime() - startNanos) / 1_000_000_000L);
            boolean ok = send(tick);
            synchronized (perSecond) {
                while (perSecond.size() <= sec) perSecond.add(new long[]{perSecond.size(), 0, 0});
                perSecond.get(sec)[ok ? 1 : 2]++;
            }
            if (ok) { firstTryOk.incrementAndGet(); ack(tick); }
            else { firstTryFail.incrementAndGet(); retry.add(tick); }
            tick++;
        }
        ticksSent = tick;
        retrier.join(120_000);
    }

    public void stop() { stop = true; }
    public int ticksSent() { return ticksSent; }
    public int pendingRetries() { return retry.size(); }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { }
    }

    public record Check(long expected, long found, long missing, long wrongValue, long extra) {}

    /** Verifies acknowledged samples through {@code base} (router or one node), optionally only series the node replicates. */
    public Check verify(String base, HashRing ring, int nodeIndex) throws Exception {
        String match = URLEncoder.encode("{__name__=\"exp_value\",run=\"" + run + "\"}", StandardCharsets.UTF_8);
        HttpResponse<String> r = Queries.HTTP.send(HttpRequest.newBuilder(URI.create(base + "/api/v1/export?match%5B%5D=" + match))
                .timeout(Duration.ofMinutes(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) throw new IllegalStateException("export from " + base + " failed: " + r.statusCode() + " " + r.body());
        long[][] found = new long[series][];
        double[][] vals = new double[series][];
        for (String line : r.body().split("\n")) {
            if (line.isBlank()) continue;
            JsonNode n = Env.JSON.readTree(line);
            if (!n.has("metric") || !n.get("metric").has("series")) throw new IllegalStateException("unexpected export line from " + base + ": " + line);
            int s = Integer.parseInt(n.get("metric").get("series").asText().substring(1));
            JsonNode ts = n.get("timestamps"), vs = n.get("values");
            found[s] = new long[ts.size()];
            vals[s] = new double[ts.size()];
            for (int i = 0; i < ts.size(); i++) { found[s][i] = ts.get(i).asLong(); vals[s][i] = vs.get(i).asDouble(); }
        }
        BitSet ack;
        synchronized (acked) { ack = (BitSet) acked.clone(); }
        long expected = 0, have = 0, missing = 0, wrong = 0, extra = 0;
        for (int s = 0; s < series; s++) {
            if (ring != null) {
                boolean mine = false;
                for (int x : ring.replicas(labels(s))) if (x == nodeIndex) mine = true;
                if (!mine) {
                    if (found[s] != null) extra += found[s].length;
                    continue;
                }
            }
            java.util.HashMap<Long, Double> got = new java.util.HashMap<>();
            if (found[s] != null) for (int i = 0; i < found[s].length; i++) got.put(found[s][i], vals[s][i]);
            have += got.size();
            for (int tick = ack.nextSetBit(0); tick >= 0; tick = ack.nextSetBit(tick + 1)) {
                expected++;
                Double v = got.get(t0 + tick * stepMs);
                if (v == null) missing++;
                else if (v != value(s, tick)) wrong++;
            }
        }
        return new Check(expected, have, missing, wrong, extra);
    }
}
