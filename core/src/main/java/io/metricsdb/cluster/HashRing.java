package io.metricsdb.cluster;

import io.metricsdb.model.Labels;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Consistent hashing of series onto storage nodes. Each node owns {@code vnodes} points on a
 * 64-bit ring; a series is stored on the first {@code rf} distinct nodes clockwise from the hash
 * of its labels. Adding or removing a node moves only the series in the ranges it gains or loses.
 */
public final class HashRing {
    private final List<String> nodes;
    private final long[] tokens;
    private final int[] owner;
    private final int rf;

    public HashRing(List<String> nodes, int vnodes, int rf) {
        if (nodes.isEmpty()) throw new IllegalArgumentException("no nodes");
        this.nodes = List.copyOf(nodes);
        this.rf = Math.min(rf, nodes.size());
        long[][] pts = new long[nodes.size() * vnodes][];
        int k = 0;
        for (int n = 0; n < nodes.size(); n++) {
            for (int v = 0; v < vnodes; v++) pts[k++] = new long[]{hash(nodes.get(n) + "#" + v), n};
        }
        Arrays.sort(pts, (a, b) -> Long.compareUnsigned(a[0], b[0]));
        tokens = new long[pts.length];
        owner = new int[pts.length];
        for (int i = 0; i < pts.length; i++) { tokens[i] = pts[i][0]; owner[i] = (int) pts[i][1]; }
    }

    public int replicationFactor() { return rf; }
    public long[] tokens() { return tokens.clone(); }

    /** Index of the ring range (previous token, token] that holds the hash. */
    public static int rangeOf(long[] tokens, long h) {
        int lo = 0, hi = tokens.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (Long.compareUnsigned(tokens[mid], h) < 0) lo = mid + 1; else hi = mid;
        }
        return lo == tokens.length ? 0 : lo;
    }
    public List<String> nodes() { return nodes; }

    /** Indexes (into {@link #nodes()}) of the replicas for a series, primary first. */
    public int[] replicas(Labels l) { return replicasForHash(l.stableHash()); }

    public int[] replicasForHash(long h) {
        int i = firstAtOrAfter(h);
        int[] out = new int[rf];
        int found = 0;
        for (int step = 0; step < tokens.length && found < rf; step++) {
            int n = owner[(i + step) % tokens.length];
            boolean dup = false;
            for (int j = 0; j < found; j++) if (out[j] == n) { dup = true; break; }
            if (!dup) out[found++] = n;
        }
        return out;
    }

    private int firstAtOrAfter(long h) {
        int lo = 0, hi = tokens.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (Long.compareUnsigned(tokens[mid], h) < 0) lo = mid + 1; else hi = mid;
        }
        return lo == tokens.length ? 0 : lo;
    }

    /** Ring token ranges (start exclusive, end inclusive) with their replica sets, for anti-entropy. */
    public List<long[]> ranges() {
        List<long[]> out = new ArrayList<>();
        for (int i = 0; i < tokens.length; i++) {
            long start = tokens[(i - 1 + tokens.length) % tokens.length];
            out.add(new long[]{start, tokens[i]});
        }
        return out;
    }

    static long hash(String s) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < s.length(); i++) { h ^= s.charAt(i); h *= 0x100000001b3L; }
        h ^= h >>> 33; h *= 0xff51afd7ed558ccdL; h ^= h >>> 33; h *= 0xc4ceb9fe1a85ec53L; h ^= h >>> 33;
        return h;
    }
}
