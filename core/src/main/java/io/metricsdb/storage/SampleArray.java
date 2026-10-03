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
        for (Chunk c : cs) {
            if (!c.overlaps(mint, maxt)) continue;
            XorChunk.Iterator it = c.iterator();
            while (it.next()) {
                long ts = it.t();
                if (ts < mint) continue;
                if (ts > maxt) break;
                out.add(ts, it.v());
            }
        }
        if (!sorted) out.sortDedup();
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

    /** Stable sort by timestamp, then keep the first of each run of equal timestamps. */
    public void sortDedup() {
        Integer[] idx = new Integer[n];
        for (int i = 0; i < n; i++) idx[i] = i;
        Arrays.sort(idx, (a, b) -> Long.compare(t[a], t[b]));
        long[] nt = new long[n];
        double[] nv = new double[n];
        int w = 0;
        for (int k = 0; k < n; k++) {
            int i = idx[k];
            if (w > 0 && nt[w - 1] == t[i]) continue;
            nt[w] = t[i];
            nv[w] = v[i];
            w++;
        }
        t = nt;
        v = nv;
        n = w;
    }
}
