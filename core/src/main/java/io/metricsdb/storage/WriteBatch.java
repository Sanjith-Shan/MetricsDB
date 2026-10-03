package io.metricsdb.storage;

import io.metricsdb.model.Labels;

import java.util.Arrays;

/** A batch of samples to append: parallel arrays of series labels, timestamps (ms) and values. */
public final class WriteBatch {
    public Labels[] labels;
    public long[] t;
    public double[] v;
    public int n;

    public WriteBatch() { this(64); }

    public WriteBatch(int cap) {
        cap = Math.max(4, cap);
        labels = new Labels[cap];
        t = new long[cap];
        v = new double[cap];
    }

    public void add(Labels l, long ts, double val) {
        if (n == t.length) {
            int cap = n * 2;
            labels = Arrays.copyOf(labels, cap);
            t = Arrays.copyOf(t, cap);
            v = Arrays.copyOf(v, cap);
        }
        labels[n] = l;
        t[n] = ts;
        v[n] = val;
        n++;
    }

    public int size() { return n; }
}
