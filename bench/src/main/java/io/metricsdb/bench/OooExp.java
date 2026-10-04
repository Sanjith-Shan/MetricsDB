package io.metricsdb.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Late samples: N series with a sample every 10 s, where a fraction of samples is delivered late
 * by a random delay up to {@code max-delay}. The harness simulates the acceptance rule (accept if
 * within the window behind the series' newest sample) to know exactly which samples must be
 * stored and which refused, then checks the database's counters and its stored data against that.
 */
public final class OooExp {
    public static void run(Args a) throws Exception {
        String url = a.req("url");
        int series = a.i("series", 500);
        int perSeries = a.i("samples", 360);
        double frac = a.d("late-fraction", 0.10);
        long maxDelay = a.l("max-delay-ms", 300_000);
        long window = a.l("window-ms", 600_000);
        long seed = a.l("seed", 1);
        String run = a.str("run", "r" + System.currentTimeMillis());
        long t0 = a.l("t0", 1_790_812_800_000L + 2 * 86_400_000L); // after the benchmark day
        Random r = new Random(seed);
        record S(int s, long t, double v, long arrive) {}
        List<S> all = new ArrayList<>();
        int late = 0;
        for (int s = 0; s < series; s++) {
            for (int k = 0; k < perSeries; k++) {
                long t = t0 + k * 10_000L;
                boolean isLate = r.nextDouble() < frac;
                long arrive = t + (isLate ? 1 + (long) (r.nextDouble() * maxDelay) : 0);
                if (isLate) late++;
                all.add(new S(s, t, Math.rint(r.nextGaussian() * 1000) / 10, arrive));
            }
        }
        all.sort((x, y) -> x.arrive != y.arrive ? Long.compare(x.arrive, y.arrive) : Integer.compare(x.s, y.s));
        // the acceptance rule, simulated in arrival order
        long[] maxT = new long[series];
        java.util.Arrays.fill(maxT, Long.MIN_VALUE);
        Set<Long> mustStore = new HashSet<>();
        long expectRefused = 0;
        for (S x : all) {
            if (x.t > maxT[x.s]) { maxT[x.s] = x.t; mustStore.add(key(x.s, x.t)); }
            else if (x.t >= maxT[x.s] - window) mustStore.add(key(x.s, x.t));
            else expectRefused++;
        }
        JsonNode before = stats(url);
        int batch = a.i("batch", 1000);
        long sentOk = 0, refusedResponses = 0;
        for (int i = 0; i < all.size(); i += batch) {
            StringBuilder sb = new StringBuilder();
            for (int j = i; j < Math.min(all.size(), i + batch); j++) {
                S x = all.get(j);
                sb.append("ooo,run=").append(run).append(",series=s").append(x.s).append(" value=").append(x.v).append(' ').append(x.t * 1_000_000L).append('\n');
            }
            HttpResponse<String> resp = Queries.HTTP.send(HttpRequest.newBuilder(URI.create(url + "/write"))
                    .POST(HttpRequest.BodyPublishers.ofString(sb.toString())).build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 204) sentOk++;
            else if (resp.statusCode() == 400) refusedResponses++;
            else throw new IllegalStateException("write failed: " + resp.statusCode() + " " + resp.body());
        }
        JsonNode after = stats(url);
        // read back everything and check it against the simulation
        String match = URLEncoder.encode("{__name__=\"ooo_value\",run=\"" + run + "\"}", StandardCharsets.UTF_8);
        HttpResponse<String> exp = Queries.HTTP.send(HttpRequest.newBuilder(URI.create(url + "/api/v1/export?match%5B%5D=" + match))
                .timeout(Duration.ofMinutes(2)).GET().build(), HttpResponse.BodyHandlers.ofString());
        Map<Long, Boolean> stored = new HashMap<>();
        for (String line : exp.body().split("\n")) {
            if (line.isBlank()) continue;
            JsonNode n = Env.JSON.readTree(line);
            int s = Integer.parseInt(n.get("metric").get("series").asText().substring(1));
            for (JsonNode t : n.get("timestamps")) stored.put(key(s, t.asLong()), true);
        }
        long lostInsideWindow = 0;
        for (long k : mustStore) if (!stored.containsKey(k)) lostInsideWindow++;
        long unexpected = 0;
        for (long k : stored.keySet()) if (!mustStore.contains(k)) unexpected++;
        ObjectNode row = Env.JSON.createObjectNode();
        row.put("exp", "ooo");
        row.put("ts", Env.now());
        row.put("label", a.str("label", "ooo"));
        row.put("series", series);
        row.put("samples_sent", all.size());
        row.put("late_samples", late);
        row.put("late_fraction", frac);
        row.put("max_delay_ms", maxDelay);
        row.put("window_ms", window);
        row.put("expected_stored", mustStore.size());
        row.put("expected_refused", expectRefused);
        row.put("stored", stored.size());
        row.put("lost_inside_window", lostInsideWindow);
        row.put("stored_but_should_be_refused", unexpected);
        row.put("db_out_of_order_accepted", after.get("samplesOutOfOrder").asLong() - before.get("samplesOutOfOrder").asLong());
        row.put("db_refused_too_old", after.get("samplesTooOld").asLong() - before.get("samplesTooOld").asLong());
        row.put("batches_ok", sentOk);
        row.put("batches_with_refusals", refusedResponses);
        row.set("machine", Env.machine());
        Env.append(a.str("out", null), row);
    }

    private static long key(int s, long t) { return (long) s << 44 ^ t; }

    static JsonNode stats(String url) throws Exception {
        HttpResponse<String> r = Queries.HTTP.send(HttpRequest.newBuilder(URI.create(url + "/admin/stats")).GET().build(), HttpResponse.BodyHandlers.ofString());
        return Env.JSON.readTree(r.body());
    }
}
