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
        XorChunk.Appender[] a = new XorChunk.Appender[AGGS];
        for (int k = 0; k < AGGS; k++) a[k] = new XorChunk.Appender();
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
        if (a[0].count() == 0) return null;
        Chunk[] out = new Chunk[AGGS];
        for (int k = 0; k < AGGS; k++) out[k] = Chunk.of(a[k].minT(), a[k].maxT(), a[k].toBytes());
        return out;
    }

    private static void emit(XorChunk.Appender[] a, long e, double mn, double mx, double sum, long cnt, double last) {
        a[MIN].append(e, mn);
        a[MAX].append(e, mx);
        a[SUM].append(e, sum);
        a[COUNT].append(e, cnt);
        a[LAST].append(e, last);
    }
}
