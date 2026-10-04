package io.metricsdb.storage;

import io.metricsdb.encoding.XorChunk;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Decoded samples of one series, sorted by time with duplicates removed. */
public final class SampleArray {
    public long[] t;
    public double[] v;
    public int n;

    public SampleArray(int cap) {
        t = new long[Math.max(4, cap)];
        v = new double[Math.max(4, cap)];
    }

    public void add(long ts, double val) {
        if (n == t.length) {
            t = Arrays.copyOf(t, n * 2);
            v = Arrays.copyOf(v, n * 2);
        }
        t[n] = ts;
        v[n] = val;
        n++;
    }

    /**
     * Decodes the chunks restricted to [mint, maxt]. Chunks from different sources (blocks, head,
     * out-of-order buffer, another replica) may overlap; the result is merged and de-duplicated by
     * timestamp, keeping the first value seen in chunk order.
     */
    public static SampleArray decode(List<Chunk> chunks, long mint, long maxt, QueryContext ctx) {
        int est = 0;
        boolean sorted = true;
        Chunk[] cs = chunks.toArray(new Chunk[0]);
        Arrays.sort(cs, Comparator.comparingLong(Chunk::minT));
        for (int i = 0; i < cs.length; i++) {
            if (!cs[i].overlaps(mint, maxt)) continue;
            est += cs[i].count();
            if (i > 0 && cs[i].minT() <= cs[i - 1].maxT()) sorted = false;
        }
        if (ctx != null) ctx.addSamples(est);
        SampleArray out = new SampleArray(est);
        int[] runs = new int[cs.length + 1];
        int nr = 0;
        for (Chunk c : cs) {
            if (!c.overlaps(mint, maxt)) continue;
            runs[nr++] = out.n;
            XorChunk.Iterator it = c.iterator();
            while (it.next()) {
                long ts = it.t();
                if (ts < mint) continue;
                if (ts > maxt) break;
                out.add(ts, it.v());
            }
        }
        runs[nr] = out.n;
        if (!sorted) out.mergeRuns(runs, nr);
        else out.dedupAdjacent();
        return out;
    }

    private void dedupAdjacent() {
        int w = 0;
        for (int i = 0; i < n; i++) {
            if (w > 0 && t[w - 1] == t[i]) continue;
            t[w] = t[i];
            v[w] = v[i];
            w++;
        }
        n = w;
    }

    /**
     * Merges sorted runs (one per chunk, in chunk order) bottom-up. On equal timestamps the
     * earlier run wins, so the result is the same as a stable sort followed by keep-first.
     */
    void mergeRuns(int[] runs, int nr) {
        long[] st = t, dt = new long[n];
        double[] sv = v, dv = new double[n];
        int[] bounds = java.util.Arrays.copyOf(runs, nr + 1);
        int count = nr;
        while (count > 1) {
            int[] nb = new int[(count + 1) / 2 + 1];
            int w = 0, k = 0;
            for (int r = 0; r < count; r += 2) {
                nb[k++] = w;
                int i = bounds[r], ie = bounds[r + 1];
                if (r + 1 == count) {
                    while (i < ie) { dt[w] = st[i]; dv[w] = sv[i]; w++; i++; }
                    continue;
                }
                int j = bounds[r + 1], je = bounds[r + 2];
                while (i < ie || j < je) {
                    long x;
                    double y;
                    if (j >= je || (i < ie && st[i] <= st[j])) {
                        x = st[i]; y = sv[i];
                        if (j < je && st[j] == x) j++; // duplicate in the later run loses
                        i++;
                    } else {
                        x = st[j]; y = sv[j]; j++;
                    }
                    if (w > 0 && dt[w - 1] == x && w > nb[k - 1]) continue;
                    dt[w] = x; dv[w] = y; w++;
                }
            }
            nb[k] = w;
            bounds = nb;
            count = k;
            long[] tt = st; st = dt; dt = tt;
            double[] tv = sv; sv = dv; dv = tv;
            n = w;
        }
        t = st;
        v = sv;
        dedupAdjacent();
    }
}
