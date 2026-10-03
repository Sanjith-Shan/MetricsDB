package io.metricsdb.server.cluster;

import io.metricsdb.cluster.Wire;
import io.metricsdb.index.Matcher;
import io.metricsdb.model.Labels;
import io.metricsdb.server.NodeMetrics;
import io.metricsdb.storage.Chunk;
import io.metricsdb.storage.QueryContext;
import io.metricsdb.storage.Queryable;
import io.metricsdb.storage.RollupSeries;
import io.metricsdb.storage.SampleArray;
import io.metricsdb.storage.SeriesChunks;
import io.metricsdb.storage.WriteBatch;
import io.metricsdb.util.ByteOut;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Query side of the router: sends the selector to every storage node at once (one virtual
 * thread each), merges what comes back by series, and de-duplicates the replicas' samples.
 *
 * <p>Partial-result policy: if a node fails, the answer is still complete as long as every ring
 * range has at least one replica among the nodes that answered. Otherwise the result is marked
 * partial with a warning naming the unreachable nodes; it is never silently short.
 *
 * <p>Read repair: when the replicas of a series disagree (one is missing it, or holds fewer
 * samples), the union is written back to the lagging replicas in the background.
 */
public final class FanoutQueryable implements Queryable {
    private final ClusterState cluster;
    private final NodeMetrics metrics;
    private final boolean readRepair;
    private final List<Long> replicaSets = new ArrayList<>(); // bitmask of nodes per ring range
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    public FanoutQueryable(ClusterState cluster, NodeMetrics metrics, boolean readRepair) {
        this.cluster = cluster;
        this.metrics = metrics;
        this.readRepair = readRepair;
        Set<Long> sets = new HashSet<>();
        for (long[] r : cluster.ring.ranges()) {
            long mask = 0;
            for (int n : cluster.ring.replicasForHash(r[1])) mask |= 1L << n;
            sets.add(mask);
        }
        replicaSets.addAll(sets);
    }

    private record NodeAnswer(int node, byte[] body, String error) {}

    private List<NodeAnswer> fanout(String path, byte[] req) {
        List<Future<NodeAnswer>> fs = new ArrayList<>();
        for (int n = 0; n < cluster.size(); n++) {
            final int node = n;
            if (!cluster.isUp(n)) {
                fs.add(java.util.concurrent.CompletableFuture.completedFuture(new NodeAnswer(n, null, cluster.nodes.get(n).name + " is down")));
                continue;
            }
            fs.add(pool.submit(() -> {
                long t0 = System.nanoTime();
                try {
                    byte[] body = cluster.nodes.get(node).post(path, req);
                    metrics.recordNanos("metricsdb.router.node.select", System.nanoTime() - t0, "node", cluster.nodes.get(node).name);
                    return new NodeAnswer(node, body, null);
                } catch (NodeClient.NodeException e) {
                    if (e.status / 100 == 4) throw new QueryContext.LimitExceeded("node", e.getMessage());
                    cluster.markDown(node);
                    return new NodeAnswer(node, null, e.getMessage());
                } catch (Exception e) {
                    cluster.markDown(node);
                    return new NodeAnswer(node, null, cluster.nodes.get(node).name + ": " + e);
                }
            }));
        }
        List<NodeAnswer> out = new ArrayList<>();
        for (Future<NodeAnswer> f : fs) {
            try {
                out.add(f.get());
            } catch (java.util.concurrent.ExecutionException e) {
                if (e.getCause() instanceof RuntimeException re) throw re;
                throw new RuntimeException(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return out;
    }

    private void checkCoverage(List<NodeAnswer> answers, QueryContext ctx) {
        long failed = 0;
        List<String> why = new ArrayList<>();
        for (NodeAnswer a : answers) {
            if (a.error != null) {
                failed |= 1L << a.node;
                why.add(a.error);
            }
        }
        if (failed == 0) return;
        for (long set : replicaSets) {
            if ((set & ~failed) == 0) {
                ctx.markPartial("partial result: every replica of some series is unreachable (" + String.join("; ", why) + ")");
                metrics.counter("metricsdb.router.queries.partial").increment();
                return;
            }
        }
        metrics.counter("metricsdb.router.queries.degraded").increment();
    }

    private static byte[] request(List<Matcher> matchers, long mint, long maxt, QueryContext ctx) {
        ByteOut o = new ByteOut(256);
        o.varint(mint).varint(maxt);
        o.uvarint(ctx == null ? 0 : Math.max(0, ctx.maxSeries() == Long.MAX_VALUE ? 0 : ctx.maxSeries()));
        o.str(Wire.encodeMatchers(matchers));
        return o.toByteArray();
    }

    @Override
    public List<SeriesChunks> select(List<Matcher> matchers, long mint, long maxt, QueryContext ctx) {
        List<NodeAnswer> answers = fanout("/internal/select", request(matchers, mint, maxt, ctx));
        if (ctx != null) checkCoverage(answers, ctx);
        Map<Labels, List<Chunk>> merged = new LinkedHashMap<>();
        Map<Labels, Map<Integer, List<Chunk>>> perNode = readRepair ? new HashMap<>() : null;
        for (NodeAnswer a : answers) {
            if (a.body == null) continue;
            for (SeriesChunks sc : Wire.decodeSeries(a.body)) {
                merged.computeIfAbsent(sc.labels(), k -> new ArrayList<>()).addAll(sc.chunks());
                if (perNode != null) perNode.computeIfAbsent(sc.labels(), k -> new HashMap<>()).put(a.node, sc.chunks());
            }
        }
        List<SeriesChunks> out = new ArrayList<>(merged.size());
        for (var e : merged.entrySet()) {
            if (ctx != null) ctx.addSeries(1);
            out.add(new SeriesChunks(e.getKey(), e.getValue()));
        }
        if (perNode != null) scheduleReadRepair(perNode, answers, mint, maxt);
        return out;
    }

    private void scheduleReadRepair(Map<Labels, Map<Integer, List<Chunk>>> perNode, List<NodeAnswer> answers, long mint, long maxt) {
        boolean[] answered = new boolean[cluster.size()];
        for (NodeAnswer a : answers) answered[a.node] = a.body != null;
        List<Runnable> repairs = new ArrayList<>();
        for (var e : perNode.entrySet()) {
            Labels l = e.getKey();
            Map<Integer, List<Chunk>> byNode = e.getValue();
            int[] reps = cluster.ring.replicas(l);
            boolean disagree = false;
            long expect = -1;
            for (int r : reps) {
                if (!answered[r]) continue;
                List<Chunk> cs = byNode.get(r);
                long cnt = 0;
                if (cs != null) for (Chunk c : cs) cnt += c.count();
                if (expect < 0) expect = cnt; else if (cnt != expect) disagree = true;
            }
            if (!disagree) continue;
            repairs.add(() -> repairSeries(l, byNode, reps, answered, mint, maxt));
        }
        if (repairs.isEmpty()) return;
        metrics.counter("metricsdb.router.read.repairs").increment(repairs.size());
        pool.submit(() -> repairs.forEach(Runnable::run));
    }

    private void repairSeries(Labels l, Map<Integer, List<Chunk>> byNode, int[] reps, boolean[] answered, long mint, long maxt) {
        List<Chunk> all = new ArrayList<>();
        for (List<Chunk> cs : byNode.values()) all.addAll(cs);
        SampleArray union = SampleArray.decode(all, mint, maxt, null);
        for (int r : reps) {
            if (!answered[r]) continue;
            List<Chunk> mine = byNode.get(r);
            SampleArray have = mine == null ? new SampleArray(0) : SampleArray.decode(mine, mint, maxt, null);
            if (have.n >= union.n) continue;
            WriteBatch b = new WriteBatch(union.n);
            for (int i = 0; i < union.n; i++) b.add(l, union.t[i], union.v[i]);
            try {
                cluster.nodes.get(r).post("/internal/write", Wire.encodeBatch(b, null, b.n));
                metrics.counter("metricsdb.router.read.repair.samples").increment(union.n - have.n);
            } catch (Exception ignored) {
                // the node is struggling; anti-entropy will catch it later
            }
        }
    }

    @Override
    public List<RollupSeries> selectRollup(List<Matcher> matchers, long mint, long maxt, long res, QueryContext ctx) {
        ByteOut o = new ByteOut(256);
        o.varint(res);
        o.bytes(request(matchers, mint, maxt, ctx));
        List<NodeAnswer> answers = fanout("/internal/select_rollup", o.toByteArray());
        for (NodeAnswer a : answers) {
            if (a.body != null && a.body.length == 0) return null; // a node cannot serve this resolution
        }
        if (ctx != null) checkCoverage(answers, ctx);
        Map<Labels, RollupSeries> merged = new LinkedHashMap<>();
        for (NodeAnswer a : answers) {
            if (a.body == null) continue;
            for (RollupSeries rs : Wire.decodeRollups(a.body)) merged.putIfAbsent(rs.labels(), rs); // replicas hold the same buckets
        }
        if (ctx != null) ctx.addSeries(merged.size());
        return new ArrayList<>(merged.values());
    }
}
