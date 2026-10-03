package io.metricsdb.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.metricsdb.cluster.HashRing;

import java.nio.file.Path;
import java.util.List;
import java.util.Random;

/**
 * A small Jepsen-style fault run: a 3-node cluster at RF=2 takes a steady write load while a
 * nemesis, one node at a time, kills a node with SIGKILL and restarts it, freezes it with SIGSTOP,
 * or cuts its link to the router. Afterwards every fault is healed, hints drain, anti-entropy
 * runs, and the checker requires that every acknowledged sample is readable through the router
 * and present on both of its replicas.
 */
public final class Faults {
    public static void run(Args a) throws Exception {
        String jar = a.req("jar");
        int runs = a.i("runs", 1);
        int duration = a.i("duration", 60);
        int series = a.i("series", 300);
        double rate = a.d("ticks-per-s", 10);
        long seed = a.l("seed", 42);
        List<String> kinds = List.of(a.str("nemesis", "kill,pause,partition").split(","));
        boolean allOk = true;
        for (int runNo = 0; runNo < runs; runNo++) {
            Random r = new Random(seed + runNo);
            String run = "f" + (seed + runNo);
            Path work = Path.of(a.str("work", System.getProperty("java.io.tmpdir") + "/metricsdb-faults"));
            ArrayNode events = Env.JSON.createArrayNode();
            try (Cluster c = new Cluster(jar, work, 3, a.i("port", 9500), 2, List.of())) {
                c.startAll();
                Workload w = new Workload(c.routerUrl(), series, run, 1_790_812_800_000L + 4 * 86_400_000L, 1000);
                long start = System.nanoTime();
                Thread loader = Thread.ofPlatform().start(() -> {
                    try { w.runLoader(rate, start); } catch (InterruptedException ignored) { }
                });
                Thread.sleep(3000);
                while ((System.nanoTime() - start) / 1e9 < duration - 8) {
                    int node = r.nextInt(3);
                    String kind = kinds.get(r.nextInt(kinds.size()));
                    long holdMs = 1500 + r.nextInt(4000);
                    ObjectNode ev = Env.JSON.createObjectNode();
                    ev.put("t", Math.round((System.nanoTime() - start) / 1e8) / 10.0);
                    ev.put("fault", kind);
                    ev.put("node", "n" + node);
                    ev.put("hold_ms", holdMs);
                    events.add(ev);
                    switch (kind) {
                        case "kill" -> {
                            c.kill(node);
                            Thread.sleep(holdMs);
                            c.startNode(node, false);
                            c.waitHealthy(c.nodeUrl(node));
                        }
                        case "pause" -> {
                            c.pause(node);
                            Thread.sleep(holdMs);
                            c.resume(node);
                        }
                        case "partition" -> {
                            c.proxies[node].drop();
                            Thread.sleep(holdMs);
                            c.proxies[node].heal();
                        }
                        default -> throw new IllegalArgumentException(kind);
                    }
                    Thread.sleep(500 + r.nextInt(2000));
                }
                Thread.sleep(Math.max(0, duration * 1000L - (System.nanoTime() - start) / 1_000_000));
                w.stop();
                loader.join();
                for (int i = 0; i < 300; i++) {
                    JsonNode rs = Env.JSON.readTree(c.get(c.routerUrl() + "/admin/stats"));
                    long pending = 0;
                    for (JsonNode n : rs.get("nodes")) pending += n.get("hintBytes").asLong();
                    if (pending == 0) break;
                    Thread.sleep(200);
                }
                JsonNode repair = Env.JSON.readTree(c.post(c.routerUrl() + "/admin/repair", ""));
                Workload.Check viaRouter = w.verify(c.routerUrl(), null, -1);
                HashRing ring = new HashRing(c.nodeNames(), 64, 2);
                long replicaMissing = 0;
                ArrayNode replicas = Env.JSON.createArrayNode();
                for (int i = 0; i < 3; i++) {
                    Workload.Check ck = w.verify(c.nodeUrl(i), ring, i);
                    replicaMissing += ck.missing() + ck.wrongValue();
                    replicas.add(Env.JSON.createObjectNode().put("node", "n" + i).put("expected", ck.expected()).put("missing", ck.missing()).put("wrong", ck.wrongValue()));
                }
                boolean ok = viaRouter.missing() == 0 && viaRouter.wrongValue() == 0 && replicaMissing == 0 && w.pendingRetries() == 0;
                allOk &= ok;
                ObjectNode row = Env.JSON.createObjectNode();
                row.put("exp", "faults");
                row.put("ts", Env.now());
                row.put("run", run);
                row.put("seed", seed + runNo);
                row.put("duration_s", duration);
                row.put("series", series);
                row.put("ticks_per_s", rate);
                row.set("events", events);
                row.put("faults", events.size());
                row.put("samples_acked", w.ackedSamples.get());
                row.put("ticks_failed_first_try", w.firstTryFail.get());
                row.put("ticks_never_acked", w.pendingRetries());
                row.put("lost_via_router", viaRouter.missing() + viaRouter.wrongValue());
                row.put("replica_samples_missing_after_repair", replicaMissing);
                row.set("replicas", replicas);
                row.set("repair", repair);
                row.put("ok", ok);
                row.set("machine", Env.machine());
                Env.append(a.str("out", null), row);
            }
        }
        if (!allOk) {
            System.err.println("FAULT CHECK FAILED");
            System.exit(1);
        }
    }
}
