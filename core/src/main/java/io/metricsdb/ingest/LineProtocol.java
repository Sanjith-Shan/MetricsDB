package io.metricsdb.ingest;

import io.metricsdb.model.Labels;
import io.metricsdb.storage.WriteBatch;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * InfluxDB line protocol: {@code measurement,tag=v,... field=1.5,other=3i 1700000000000000000}.
 * Each numeric field becomes one sample of series {@code measurement_field} with the tags as
 * labels, the naming VictoriaMetrics uses, so the standard benchmark's loaders and queries work
 * unchanged. String fields are skipped. Timestamps default to nanoseconds.
 *
 * <p>Metrics repeat the same series line after line, so the parser caches, across requests and
 * threads, the raw bytes of each {@code measurement,tags} prefix together with the label set it
 * produces for every field. A repeated series costs one hash of its prefix bytes: no strings,
 * no sorting, and the cached {@link Labels} instance makes the series lookup an identity hit.
 */
public final class LineProtocol {

    public static final class Stats {
        public long lines, samples, skippedFields, errors;
        public String firstError;
    }

    private static final int MAX_CACHED = 1 << 20;
    private static final ConcurrentHashMap<Key, String> STRINGS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Key, Prefix> PREFIXES = new ConcurrentHashMap<>();

    private final Key probe = new Key();

    /** A byte range used as a hash key; probes point into the request body, stored keys own a copy. */
    private static final class Key {
        byte[] a;
        int off, len, hash;

        Key set(byte[] a, int off, int len) {
            this.a = a;
            this.off = off;
            this.len = len;
            int h = 1;
            for (int i = off; i < off + len; i++) h = 31 * h + a[i];
            this.hash = h;
            return this;
        }

        Key copy() { return new Key().set(Arrays.copyOfRange(a, off, off + len), 0, len); }

        @Override public int hashCode() { return hash; }

        @Override public boolean equals(Object o) {
            return o instanceof Key k && k.len == len && Arrays.equals(a, off, off + len, k.a, k.off, k.off + k.len);
        }
    }

    /** One {@code measurement,tags} prefix: its sorted tags and the label set of each field seen. */
    private static final class Prefix {
        final String measurement;
        final String[] tagKv; // sorted by name, no __name__
        final int at;         // where __name__ goes
        final ConcurrentHashMap<String, Labels> byField = new ConcurrentHashMap<>();

        Prefix(String measurement, String[] tagKv) {
            this.measurement = measurement;
            this.tagKv = tagKv;
            int a = 0;
            while (a < tagKv.length && tagKv[a].compareTo(Labels.NAME) < 0) a += 2;
            this.at = a;
        }

        Labels labels(String field) {
            Labels l = byField.get(field);
            if (l != null) return l;
            String[] kv = new String[tagKv.length + 2];
            System.arraycopy(tagKv, 0, kv, 0, at);
            kv[at] = Labels.NAME;
            kv[at + 1] = intern(measurement.isEmpty() ? field : measurement + "_" + field);
            System.arraycopy(tagKv, at, kv, at + 2, tagKv.length - at);
            l = Labels.fromSorted(kv);
            Labels prev = byField.putIfAbsent(field, l);
            return prev != null ? prev : l;
        }
    }

    private static String intern(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        String prev = STRINGS.putIfAbsent(new Key().set(b, 0, b.length), s);
        return prev != null ? prev : s;
    }

    private String str(byte[] b, int off, int len, boolean escaped) {
        if (escaped) return intern(unescape(b, off, len));
        String s = STRINGS.get(probe.set(b, off, len));
        if (s != null) return s;
        if (STRINGS.size() > MAX_CACHED) STRINGS.clear();
        s = new String(b, off, len, StandardCharsets.UTF_8);
        String prev = STRINGS.putIfAbsent(probe.copy(), s);
        return prev != null ? prev : s;
    }

    private static String unescape(byte[] b, int off, int len) {
        byte[] out = new byte[len];
        int n = 0;
        for (int i = off; i < off + len; i++) {
            if (b[i] == '\\' && i + 1 < off + len) i++;
            out[n++] = b[i];
        }
        return new String(out, 0, n, StandardCharsets.UTF_8);
    }

    /** Parses {@code measurement,tag=v,...} in [s, e) into a prefix entry. */
    private Prefix parsePrefix(byte[] b, int s, int e) {
        int p = s;
        boolean esc = false;
        while (p < e && b[p] != ',') { if (b[p] == '\\') { esc = true; p++; } p++; }
        String measurement = str(b, s, p - s, esc);
        String[] tagKv = new String[16];
        int nt = 0;
        while (p < e && b[p] == ',') {
            p++;
            int ks = p;
            esc = false;
            while (p < e && b[p] != '=') { if (b[p] == '\\') { esc = true; p++; } p++; }
            if (p >= e) throw new IllegalArgumentException("tag without value");
            String k = str(b, ks, p - ks, esc);
            p++;
            int vs = p;
            esc = false;
            while (p < e && b[p] != ',') { if (b[p] == '\\') { esc = true; p++; } p++; }
            String v = str(b, vs, p - vs, esc);
            if (v.isEmpty() || k.equals(Labels.NAME)) continue;
            if (nt + 2 > tagKv.length) tagKv = Arrays.copyOf(tagKv, tagKv.length * 2);
            tagKv[nt++] = k;
            tagKv[nt++] = v;
        }
        // sort by key (insertion sort: usually already sorted, and short); duplicate keys: last wins
        for (int a = 2; a < nt; a += 2) {
            String k = tagKv[a], v = tagKv[a + 1];
            int c = a - 2;
            while (c >= 0 && tagKv[c].compareTo(k) > 0) {
                tagKv[c + 2] = tagKv[c];
                tagKv[c + 3] = tagKv[c + 1];
                c -= 2;
            }
            tagKv[c + 2] = k;
            tagKv[c + 3] = v;
        }
        int w = 0;
        for (int a = 0; a < nt; a += 2) {
            if (w >= 2 && tagKv[w - 2].equals(tagKv[a])) { tagKv[w - 1] = tagKv[a + 1]; continue; }
            tagKv[w] = tagKv[a];
            tagKv[w + 1] = tagKv[a + 1];
            w += 2;
        }
        return new Prefix(measurement, Arrays.copyOf(tagKv, w));
    }

    /**
     * Parses a body into {@code out}. {@code precisionDivisor} converts the timestamp unit to
     * milliseconds as a divisor (1_000_000 for ns, 1_000 for us, 1 for ms) or, when negative, a
     * multiplier (-1000 for s).
     */
    public Stats parse(byte[] b, int len, long precisionDivisor, WriteBatch out) {
        Stats st = new Stats();
        int i = 0;
        String[] fieldNames = new String[64];
        double[] fieldVals = new double[64];
        long now = System.currentTimeMillis();
        while (i < len) {
            int lineStart = i;
            int lineEnd = i;
            while (lineEnd < len && b[lineEnd] != '\n') lineEnd++;
            i = lineEnd + 1;
            int e = lineEnd;
            if (e > lineStart && b[e - 1] == '\r') e--;
            int p = lineStart;
            while (p < e && (b[p] == ' ' || b[p] == '\t')) p++;
            if (p >= e || b[p] == '#') continue;
            st.lines++;
            try {
                int ps = p;
                while (p < e && b[p] != ' ') { if (b[p] == '\\') p++; p++; }
                if (p >= e) throw new IllegalArgumentException("missing fields");
                Prefix pre = PREFIXES.get(probe.set(b, ps, p - ps));
                if (pre == null) {
                    pre = parsePrefix(b, ps, p);
                    if (PREFIXES.size() > MAX_CACHED) PREFIXES.clear();
                    Prefix prev = PREFIXES.putIfAbsent(new Key().set(b, ps, p - ps).copy(), pre);
                    if (prev != null) pre = prev;
                }
                p++;
                int nf = 0;
                while (p < e && b[p] != ' ') {
                    int ks = p;
                    boolean esc = false;
                    while (p < e && b[p] != '=') { if (b[p] == '\\') { esc = true; p++; } p++; }
                    if (p >= e) throw new IllegalArgumentException("field without value");
                    String k = str(b, ks, p - ks, esc);
                    p++;
                    if (p < e && b[p] == '"') {
                        p++;
                        while (p < e && b[p] != '"') { if (b[p] == '\\') p++; p++; }
                        p++;
                        st.skippedFields++;
                    } else {
                        int vs = p;
                        while (p < e && b[p] != ',' && b[p] != ' ') p++;
                        double v = parseFieldValue(b, vs, p);
                        if (nf == fieldNames.length) {
                            fieldNames = Arrays.copyOf(fieldNames, nf * 2);
                            fieldVals = Arrays.copyOf(fieldVals, nf * 2);
                        }
                        fieldNames[nf] = k;
                        fieldVals[nf] = v;
                        nf++;
                    }
                    if (p < e && b[p] == ',') p++;
                }
                long ts;
                while (p < e && b[p] == ' ') p++;
                if (p < e) {
                    long raw = parseLong(b, p, e);
                    ts = precisionDivisor > 0 ? Math.floorDiv(raw, precisionDivisor) : raw * -precisionDivisor;
                } else {
                    ts = now;
                }
                for (int f = 0; f < nf; f++) out.add(pre.labels(fieldNames[f]), ts, fieldVals[f]);
                st.samples += nf;
            } catch (RuntimeException ex) {
                st.errors++;
                if (st.firstError == null) {
                    st.firstError = ex.getMessage() + " in line: " + new String(b, lineStart, Math.min(200, e - lineStart), StandardCharsets.UTF_8);
                }
            }
        }
        return st;
    }

    private static double parseFieldValue(byte[] b, int s, int e) {
        if (s >= e) throw new IllegalArgumentException("empty field value");
        byte last = b[e - 1];
        if (last == 'i' || last == 'u') return parseLong(b, s, e - 1);
        if (b[s] == 't' || b[s] == 'T') return 1;
        if (b[s] == 'f' || b[s] == 'F') return 0;
        // fast path for plain decimals; anything else goes through Double.parseDouble
        long mant = 0;
        int p = s;
        boolean neg = false;
        if (b[p] == '-') { neg = true; p++; } else if (b[p] == '+') p++;
        int digits = 0, scale = 0;
        boolean dot = false;
        for (; p < e; p++) {
            byte c = b[p];
            if (c >= '0' && c <= '9') {
                if (digits >= 15) return Double.parseDouble(new String(b, s, e - s, StandardCharsets.US_ASCII));
                mant = mant * 10 + (c - '0');
                if (mant != 0) digits++;
                if (dot && ++scale > 22) return Double.parseDouble(new String(b, s, e - s, StandardCharsets.US_ASCII));
            } else if (c == '.' && !dot) {
                dot = true;
            } else {
                return Double.parseDouble(new String(b, s, e - s, StandardCharsets.US_ASCII));
            }
        }
        double v = scale == 0 ? mant : mant / POW10[scale];
        return neg ? -v : v;
    }

    private static final double[] POW10 = new double[23];
    static { POW10[0] = 1; for (int i = 1; i < POW10.length; i++) POW10[i] = POW10[i - 1] * 10; }

    private static long parseLong(byte[] b, int s, int e) {
        boolean neg = false;
        if (b[s] == '-') { neg = true; s++; }
        long v = 0;
        if (s >= e) throw new IllegalArgumentException("bad integer");
        for (int p = s; p < e; p++) {
            int d = b[p] - '0';
            if (d < 0 || d > 9) throw new IllegalArgumentException("bad integer");
            v = v * 10 + d;
        }
        return neg ? -v : v;
    }
}
