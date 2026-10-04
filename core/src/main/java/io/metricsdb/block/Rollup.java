package io.metricsdb.block;

import io.metricsdb.encoding.XorChunk;
import io.metricsdb.storage.Chunk;
import io.metricsdb.storage.SampleArray;

/**
 * Downsampled companion of raw data: per series and per bucket of {@code res} milliseconds, the
 * min, max, sum, count and last sample. A bucket ending at {@code e} covers {@code (e - res, e]},
 * the same left-open convention as PromQL range windows, so {@code max_over_time(x[1h])}
 * evaluated at a bucket-aligned time is answered exactly from 12 five-minute buckets.
 */
public final class Rollup {
    public static final int MIN = 0, MAX = 1, SUM = 2, COUNT = 3, LAST = 4, AGGS = 5;
    public static final String[] NAMES = {"min", "max", "sum", "count", "last"};

    private Rollup() {}

    public static long bucketEnd(long t, long res) {
        long q = Math.floorDiv(t, res) * res;
        return q == t ? t : q + res;
    }

    /** Aggregates samples (sorted) with timestamps in (fromExcl, toIncl] into one chunk per aggregate. */
    public static Chunk[] build(SampleArray s, long fromExcl, long toIncl, long res) {
        SampleArray[] a = new SampleArray[AGGS];
        for (int k = 0; k < AGGS; k++) a[k] = new SampleArray(16);
        long cur = Long.MIN_VALUE;
        double mn = 0, mx = 0, sum = 0, last = 0;
        long cnt = 0;
        for (int i = 0; i < s.n; i++) {
            long t = s.t[i];
            if (t <= fromExcl || t > toIncl) continue;
            long e = bucketEnd(t, res);
            if (e != cur) {
                if (cnt > 0) emit(a, cur, mn, mx, sum, cnt, last);
                cur = e;
                mn = Double.POSITIVE_INFINITY;
                mx = Double.NEGATIVE_INFINITY;
                sum = 0;
                cnt = 0;
            }
            double v = s.v[i];
            mn = Math.min(mn, v);
            mx = Math.max(mx, v);
            sum += v;
            cnt++;
            last = v;
        }
        if (cnt > 0) emit(a, cur, mn, mx, sum, cnt, last);
        if (a[0].n == 0) return null;
        Chunk[] out = new Chunk[AGGS];
        for (int k = 0; k < AGGS; k++) out[k] = encode(a[k]);
        return out;
    }

    /** One chunk for a whole aggregate series, integer-encoded when it can be. */
    public static Chunk encode(SampleArray s) {
        return Chunk.of(s.t[0], s.t[s.n - 1], XorChunk.encodeBest(s.t, s.v, 0, s.n));
    }

    private static void emit(SampleArray[] a, long e, double mn, double mx, double sum, long cnt, double last) {
        a[MIN].add(e, mn);
        a[MAX].add(e, mx);
        a[SUM].add(e, sum);
        a[COUNT].add(e, cnt);
        a[LAST].add(e, last);
    }
}
