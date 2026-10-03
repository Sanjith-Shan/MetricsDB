package io.metricsdb.server.cluster;

import io.metricsdb.cluster.Digests;
import io.metricsdb.cluster.Wire;
import io.metricsdb.model.Labels;
import io.metricsdb.server.NodeMetrics;
import io.metricsdb.storage.Chunk;
import io.metricsdb.storage.SampleArray;
import io.metricsdb.storage.SeriesChunks;
import io.metricsdb.storage.WriteBatch;
import io.metricsdb.util.ByteOut;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Anti-entropy between replicas. The router asks every node for one hash per ring range; only
 * ranges whose replicas disagree are expanded into per-series, per-bucket leaves; only the
 * series whose leaves disagree are fetched, merged and written back to the replicas missing
 * samples. Two replicas that agree cost one hash per range to confirm.
 */
public final class AntiEntropy {
    private final ClusterState cluster;
    private final NodeMetrics metrics;

    public AntiEntropy(ClusterState cluster, NodeMetrics metrics) {
        this.cluster = cluster;
        this.metrics = metrics;
    }

    public record Report(int ranges, int rangesMismatched, int seriesCompared, int seriesRepaired,
                         long samplesWritten, long millis, boolean complete, String note) {}

    public synchronized Report run(long mint, long maxt, long bucketMs) {
        long t0 = System.currentTimeMillis();
        long[] tokens = cluster.ring.tokens();
        int nr = tokens.length;
        int nodes = cluster.size();
        int[][] replicas = new int[nr][];
        for (int i = 0; i < nr; i++) replicas[i] = cluster.ring.replicasForHash(tokens[i]);
        boolean complete = true;
        // level 0: one hash per range per node
        long[][][] level0 = new long[nodes][][];
        for (int n = 0; n < nodes; n++) {
            if (!cluster.isUp(n)) { complete = false; continue; }
            try {
                byte[] body = cluster.nodes.get(n).post("/internal/digest", new Digests.Request(mint, maxt, bucketMs, tokens, 0, null).encode());
                level0[n] = Digests.decodeRanges(body);
            } catch (Exception e) {
                complete = false;
            }
        }
        boolean[][] mismatchedFor = new boolean[nodes][nr];
        int mismatched = 0;
        for (int r = 0; r < nr; r++) {
            Long h = null;
            boolean differ = false;
            for (int n : replicas[r]) {
                if (level0[n] == null) continue;
                long[] d = level0[n][r];
                long key = d[0] * 31 + d[1];
                if (h == null) h = key; else if (h != key) differ = true;
            }
            if (differ) {
                mismatched++;
                for (int n : replicas[r]) mismatchedFor[n][r] = true;
            }
        }
        int seriesCompared = 0, seriesRepaired = 0;
        long written = 0;
        if (mismatched > 0) {
            // level 1: leaves for the mismatched ranges only
            Map<Labels, Map<Integer, Digests.Leaf>> leaves = new LinkedHashMap<>();
            for (int n = 0; n < nodes; n++) {
                if (level0[n] == null) continue;
                boolean any = false;
                for (boolean b : mismatchedFor[n]) any |= b;
                if (!any) continue;
                try {
                    byte[] body = cluster.nodes.get(n).post("/internal/digest", new Digests.Request(mint, maxt, bucketMs, tokens, 1, mismatchedFor[n]).encode());
                    for (Digests.Leaf l : Digests.decodeLeaves(body)) leaves.computeIfAbsent(l.labels(), k -> new HashMap<>()).put(n, l);
                } catch (Exception e) {
                    complete = false;
                }
            }
            List<Labels> toRepair = new ArrayList<>();
            for (var e : leaves.entrySet()) {
                seriesCompared++;
                int[] reps = cluster.ring.replicas(e.getKey());
                Digests.Leaf first = null;
                boolean differ = false;
                for (int n : reps) {
                    if (level0[n] == null) continue;
                    Digests.Leaf l = e.getValue().get(n);
                    if (first == null && l != null) first = l;
                    if (l == null || !sameLeaf(first, l)) differ = true;
                }
                if (differ) toRepair.add(e.getKey());
            }
            // fetch the disagreeing series from every replica, merge, write the union back
            for (Labels l : toRepair) {
                int[] reps = cluster.ring.replicas(l);
                Map<Integer, List<Chunk>> got = new HashMap<>();
                for (int n : reps) {
                    if (level0[n] == null) continue;
                    try {
                        got.put(n, fetch(n, l, mint, maxt));
                    } catch (Exception e) {
                        complete = false;
                    }
                }
                List<Chunk> all = new ArrayList<>();
                got.values().forEach(all::addAll);
                SampleArray union = SampleArray.decode(all, mint, maxt, null);
                boolean repaired = false;
                for (var g : got.entrySet()) {
                    SampleArray have = SampleArray.decode(g.getValue(), mint, maxt, null);
                    if (have.n >= union.n) continue;
                    WriteBatch b = new WriteBatch(union.n);
                    for (int i = 0; i < union.n; i++) b.add(l, union.t[i], union.v[i]);
                    try {
                        cluster.nodes.get(g.getKey()).post("/internal/write", Wire.encodeBatch(b, null, b.n));
                        written += union.n - have.n;
                        repaired = true;
                    } catch (Exception e) {
                        complete = false;
                    }
                }
                if (repaired) seriesRepaired++;
            }
        }
        metrics.counter("metricsdb.router.antientropy.runs").increment();
        metrics.counter("metricsdb.router.antientropy.samples.written").increment(written);
        return new Report(nr, mismatched, seriesCompared, seriesRepaired, written, System.currentTimeMillis() - t0, complete,
                complete ? "" : "some nodes were unreachable; their replicas were not compared");
    }

    private static boolean sameLeaf(Digests.Leaf a, Digests.Leaf b) {
        return a == b || (java.util.Arrays.equals(a.bucket(), b.bucket()) && java.util.Arrays.equals(a.count(), b.count())
                && java.util.Arrays.equals(a.hash(), b.hash()));
    }

    private List<Chunk> fetch(int node, Labels l, long mint, long maxt) throws Exception {
        ByteOut o = new ByteOut(256);
        o.varint(mint).varint(maxt).uvarint(0);
        List<io.metricsdb.index.Matcher> ms = new ArrayList<>();
        for (int i = 0; i < l.size(); i++) ms.add(io.metricsdb.index.Matcher.eq(l.name(i), l.value(i)));
        o.str(Wire.encodeMatchers(ms));
        byte[] body = cluster.nodes.get(node).post("/internal/select", o.toByteArray());
        for (SeriesChunks sc : Wire.decodeSeries(body)) if (Objects.equals(sc.labels(), l)) return sc.chunks();
        return List.of();
    }
}
