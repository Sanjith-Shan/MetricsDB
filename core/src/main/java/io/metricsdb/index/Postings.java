package io.metricsdb.index;

import java.util.Arrays;
import java.util.List;

/**
 * Operations on sorted, duplicate-free posting lists of series ids. Intersection walks the
 * shorter list and gallops (exponential then binary search) through the longer one, so a
 * selective matcher costs O(small * log(large / small)) rather than O(small + large).
 */
public final class Postings {
    private Postings() {}

    public static final int[] EMPTY = new int[0];

    public static int[] intersect(int[] a, int na, int[] b, int nb) {
        if (na > nb) { int[] t = a; a = b; b = t; int tn = na; na = nb; nb = tn; }
        int[] out = new int[na];
        int n = 0;
        int j = 0;
        // when sizes are close a linear merge is cheaper than galloping
        if (nb < na * 8) {
            int i = 0;
            while (i < na && j < nb) {
                int x = a[i], y = b[j];
                if (x == y) { out[n++] = x; i++; j++; }
                else if (x < y) i++;
                else j++;
            }
        } else {
            for (int i = 0; i < na && j < nb; i++) {
                int x = a[i];
                j = gallop(b, j, nb, x);
                if (j < nb && b[j] == x) { out[n++] = x; j++; }
            }
        }
        return n == out.length ? out : Arrays.copyOf(out, n);
    }

    public static int[] intersect(int[] a, int[] b) { return intersect(a, a.length, b, b.length); }

    /** First index in [from, n) with b[index] >= x. */
    static int gallop(int[] b, int from, int n, int x) {
        int step = 1;
        int lo = from, hi = from;
        while (hi < n && b[hi] < x) {
            lo = hi + 1;
            hi = from + step;
            step <<= 1;
        }
        if (hi > n) hi = n;
        int idx = Arrays.binarySearch(b, lo, hi, x);
        return idx >= 0 ? idx : -idx - 1;
    }

    public static int[] union(List<int[]> lists) {
        if (lists.isEmpty()) return EMPTY;
        if (lists.size() == 1) return lists.get(0);
        if (lists.size() == 2) return union2(lists.get(0), lists.get(1));
        int total = 0;
        for (int[] l : lists) total += l.length;
        int[] all = new int[total];
        int p = 0;
        for (int[] l : lists) { System.arraycopy(l, 0, all, p, l.length); p += l.length; }
        Arrays.sort(all);
        int n = 0;
        for (int i = 0; i < all.length; i++) if (i == 0 || all[i] != all[i - 1]) all[n++] = all[i];
        return n == all.length ? all : Arrays.copyOf(all, n);
    }

    static int[] union2(int[] a, int[] b) {
        int[] out = new int[a.length + b.length];
        int i = 0, j = 0, n = 0;
        while (i < a.length && j < b.length) {
            int x = a[i], y = b[j];
            if (x == y) { out[n++] = x; i++; j++; }
            else if (x < y) { out[n++] = x; i++; }
            else { out[n++] = y; j++; }
        }
        while (i < a.length) out[n++] = a[i++];
        while (j < b.length) out[n++] = b[j++];
        return n == out.length ? out : Arrays.copyOf(out, n);
    }

    /** a minus b. */
    public static int[] subtract(int[] a, int[] b) {
        int[] out = new int[a.length];
        int n = 0, j = 0;
        for (int x : a) {
            while (j < b.length && b[j] < x) j++;
            if (j < b.length && b[j] == x) continue;
            out[n++] = x;
        }
        return n == out.length ? out : Arrays.copyOf(out, n);
    }
}
