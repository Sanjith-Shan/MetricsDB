package io.metricsdb.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.metricsdb.cluster.HashRing;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * exp5: one storage node is killed with SIGKILL mid-write and restarted later. Measures samples
 * lost, write and query availability during the outage, and whether the replicas converge again
 * (hint replay, then a Merkle-style anti-entropy pass), at the configured replication factor.
 */
public final class Exp5 {
    public static void run(Args a) throws Exception {
        String jar = a.req("jar");
        int rf = a.i("rf", 2);
        int nodes = a.i("nodes", 3);
        int series = a.i("series", 1000);
        double rate = a.d("ticks-per-s", 10);
        int killAt = a.i("kill-at", 20), restartAt = a.i("restart-at", 40), endAt = a.i("end-at", 60);
        int victim = a.i("victim", 1);
        boolean wipe = a.b("wipe");
        String run = a.str("run", "e" + System.currentTimeMillis() % 1_000_000);
        Path work = Path.of(a.str("work", System.getProperty("java.io.tmpdir") + "/metricsdb-exp5"));
        List<String> routerExtra = new ArrayList<>();
        if (a.b("no-hints")) routerExtra.add("--metricsdb.cluster.hinted-handoff=false");
        try (Cluster c = new Cluster(jar, work, nodes, a.i("port", 9400), rf, routerExtra)) {
            c.startAll();
            long t0 = 1_790_812_800_000L + 3 * 86_400_000L;
            Workload w = new Workload(c.routerUrl(), series, run, t0, 1000);
            long start = System.nanoTime();
            Thread loader = Thread.ofPlatform().start(() -> {
                try { w.runLoader(rate, start); } catch (InterruptedException ignored) { }
            });
            // probe queries: complete, partial, or failed, and how many series are visible
            ArrayNode probes = Env.JSON.createArrayNode();
            Thread prober = Thread.ofPlatform().start(() -> probe(c, w, start, endAt, probes));
            sleepUntil(start, killAt);
            c.kill(victim);
            long killedAt = System.nanoTime();
            sleepUntil(start, restartAt);
            c.startNode(victim, wipe);
            c.waitHealthy(c.nodeUrl(victim));
            long restartedAt = System.nanoTime();
            sleepUntil(start, endAt);
            w.stop();
            loader.join();
            prober.join();
            // let hints drain
            long hintWaitStart = System.nanoTime();
            JsonNode rs = null;
            for (int i = 0; i < 240; i++) {
                rs = Env.JSON.readTree(c.get(c.routerUrl() + "/admin/stats"));
                long pending = 0;
                for (JsonNode n : rs.get("nodes")) pending += n.get("hintBytes").asLong();
                if (pending == 0) break;
                Thread.sleep(250);
            }
            double hintDrainS = (System.nanoTime() - hintWaitStart) / 1e9;
            HashRing ring = new HashRing(c.nodeNames(), 64, rf);
            // replica convergence before and after anti-entropy
            ArrayNode before = perNode(c, w, ring);
            JsonNode repair1 = Env.JSON.readTree(c.post(c.routerUrl() + "/admin/repair", ""));
            JsonNode repair2 = Env.JSON.readTree(c.post(c.routerUrl() + "/admin/repair", ""));
            ArrayNode after = perNode(c, w, ring);
            Workload.Check viaRouter = w.verify(c.routerUrl(), null, -1);
            ObjectNode row = Env.JSON.createObjectNode();
            row.put("exp", "exp5");
            row.put("ts", Env.now());
            row.put("label", a.str("label", "rf" + rf + (wipe ? "-wipe" : "")));
            row.put("rf", rf);
            row.put("nodes", nodes);
            row.put("series", series);
            row.put("ticks_per_s", rate);
            row.put("samples_per_s_offered", Math.round(series * rate));
            row.put("kill_at_s", killAt);
            row.put("restart_at_s", restartAt);
            row.put("end_at_s", endAt);
            row.put("wipe_on_restart", wipe);
            row.put("outage_s", Math.round((restartedAt - killedAt) / 1e8) / 10.0);
            row.put("ticks_sent", w.ticksSent());
            row.put("ticks_ok_first_try", w.firstTryOk.get());
            row.put("ticks_failed_first_try", w.firstTryFail.get());
            row.put("retries", w.retries.get());
            row.put("ticks_never_acked", w.pendingRetries());
            row.put("samples_acked", w.ackedSamples.get());
            row.put("samples_lost", viaRouter.missing() + viaRouter.wrongValue());
            row.put("samples_missing_via_router", viaRouter.missing());
            row.put("samples_wrong_value", viaRouter.wrongValue());
            row.put("hint_drain_s", Math.round(hintDrainS * 10) / 10.0);
            row.set("router_stats", rs);
            row.set("replicas_before_repair", before);
            row.set("repair_pass_1", repair1);
            row.set("repair_pass_2", repair2);
            row.set("replicas_after_repair", after);
            row.set("probe_summary", summarize(probes, start, killedAt, restartedAt));
            row.set("machine", Env.machine());
            row.set("load_after", Env.load());
            Env.append(a.str("out", null), row);
            // timeline for the chart: per second, write ticks ok/failed and probe outcomes
            String tl = a.str("timeline", null);
            if (tl != null) {
                ObjectNode t = Env.JSON.createObjectNode();
                t.put("exp", "exp5_timeline");
                t.put("label", row.get("label").asText());
                t.put("ts", row.get("ts").asText());
                ArrayNode secs = Env.JSON.createArrayNode();
                synchronized (w.perSecond) {
                    for (long[] s : w.perSecond) secs.add(Env.JSON.createArrayNode().add(s[0]).add(s[1]).add(s[2]));
                }
                t.set("writes_per_second", secs);
                t.set("probes", probes);
                t.put("kill_at_s", Math.round((killedAt - start) / 1e8) / 10.0);
                t.put("restart_at_s", Math.round((restartedAt - start) / 1e8) / 10.0);
                Env.append(tl, t);
            }
        }
    }

    private static ArrayNode perNode(Cluster c, Workload w, HashRing ring) throws Exception {
        ArrayNode out = Env.JSON.createArrayNode();
        for (int i = 0; i < c.n; i++) {
            Workload.Check ck = w.verify(c.nodeUrl(i), ring, i);
            ObjectNode o = Env.JSON.createObjectNode();
            o.put("node", "n" + i);
            o.put("expected", ck.expected());
            o.put("stored", ck.found());
            o.put("missing", ck.missing());
            o.put("wrong", ck.wrongValue());
            o.put("not_owned", ck.extra());
            out.add(o);
        }
        return out;
    }

    private static void probe(Cluster c, Workload w, long start, int endAt, ArrayNode out) {
        String q = URLEncoder.encode("count(last_over_time({__name__=\"exp_value\",run=\"" + w.run + "\"}[1h]))", StandardCharsets.UTF_8);
        while ((System.nanoTime() - start) / 1e9 < endAt) {
            long s = System.nanoTime();
            long evalAt = w.t0 + 3_600_000L; // a fixed time after all data: count the series that are visible
            String status;
            int visible = -1;
            try {
                HttpResponse<String> r = Queries.HTTP.send(HttpRequest.newBuilder(URI.create(c.routerUrl() + "/api/v1/query?query=" + q + "&time=" + evalAt / 1000))
                        .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() != 200) status = "error";
                else {
                    JsonNode n = Env.JSON.readTree(r.body());
                    status = n.path("isPartial").asBoolean() ? "partial" : "complete";
                    JsonNode res = n.path("data").path("result");
                    visible = res.size() == 0 ? 0 : (int) Double.parseDouble(res.get(0).get("value").get(1).asText());
                }
            } catch (Exception e) {
                status = "error";
            }
            ObjectNode p = Env.JSON.createObjectNode();
            p.put("t", Math.round((s - start) / 1e8) / 10.0);
            p.put("status", status);
            p.put("visible_series", visible);
            p.put("ms", (System.nanoTime() - s) / 1_000_000);
            synchronized (out) { out.add(p); }
            try { Thread.sleep(250); } catch (InterruptedException e) { return; }
        }
    }

    private static ObjectNode summarize(ArrayNode probes, long start, long killed, long restarted) {
        double k = (killed - start) / 1e9, r = (restarted - start) / 1e9;
        int during = 0, complete = 0, partial = 0, error = 0, minVisible = Integer.MAX_VALUE;
        int all = 0, allComplete = 0;
        for (JsonNode p : probes) {
            all++;
            if (p.get("status").asText().equals("complete")) allComplete++;
            double t = p.get("t").asDouble();
            if (t < k || t > r) continue;
            during++;
            switch (p.get("status").asText()) {
                case "complete" -> complete++;
                case "partial" -> partial++;
                default -> error++;
            }
            if (p.get("visible_series").asInt() >= 0) minVisible = Math.min(minVisible, p.get("visible_series").asInt());
        }
        ObjectNode o = Env.JSON.createObjectNode();
        o.put("probes_total", all);
        o.put("probes_complete_total", allComplete);
        o.put("probes_during_outage", during);
        o.put("complete_during_outage", complete);
        o.put("partial_during_outage", partial);
        o.put("error_during_outage", error);
        o.put("min_visible_series_during_outage", minVisible == Integer.MAX_VALUE ? -1 : minVisible);
        return o;
    }

    static void sleepUntil(long start, int second) throws InterruptedException {
        long due = start + second * 1_000_000_000L;
        long now = System.nanoTime();
        if (due > now) Thread.sleep((due - now) / 1_000_000);
    }
}
