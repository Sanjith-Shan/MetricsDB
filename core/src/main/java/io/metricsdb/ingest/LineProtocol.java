package io.metricsdb.ingest;

import io.metricsdb.model.Labels;
import io.metricsdb.storage.WriteBatch;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;

/**
 * InfluxDB line protocol: {@code measurement,tag=v,... field=1.5,other=3i 1700000000000000000}.
 * Each numeric field becomes one sample of series {@code measurement_field} with the tags as
 * labels, the naming VictoriaMetrics uses, so the standard benchmark's loaders and queries work
 * unchanged. String fields are skipped. Timestamps default to nanoseconds.
 */
public final class LineProtocol {

    public static final class Stats {
        public long lines, samples, skippedFields, errors;
        public String firstError;
    }

    private final HashMap<Key, String> strings = new HashMap<>();
    private final Key probe = new Key();

    /** Small per-parser string cache: tag keys and values repeat across lines. */
    private static final class Key {
        byte[] a;
        int off, len, hash;

        void set(byte[] a, int off, int len) {
            this.a = a;
            this.off = off;
            this.len = len;
            int h = 1;
            for (int i = off; i < off + len; i++) h = 31 * h + a[i];
            this.hash = h;
        }

        @Override public int hashCode() { return hash; }

        @Override public boolean equals(Object o) {
            return o instanceof Key k && k.len == len && Arrays.equals(a, off, off + len, k.a, k.off, k.off + k.len);
        }
    }

    private String str(byte[] b, int off, int len, boolean escaped) {
        if (escaped) return unescape(b, off, len);
        probe.set(b, off, len);
        String s = strings.get(probe);
        if (s != null) return s;
        s = new String(b, off, len, StandardCharsets.UTF_8);
        if (strings.size() > 200_000) strings.clear();
        Key k = new Key();
        k.set(Arrays.copyOfRange(b, off, off + len), 0, len);
        strings.put(k, s);
        return s;
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

    /**
     * Parses a body into {@code out}. {@code precisionMs} converts the timestamp unit to
     * milliseconds as a divisor (1_000_000 for ns, 1_000 for us, 1 for ms) or multiplier (negative: -1000 for s).
     */
    public Stats parse(byte[] b, int len, long precisionDivisor, WriteBatch out) {
        Stats st = new Stats();
        int i = 0;
        String[] tagKv = new String[32];
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
                // measurement
                int ms = p;
                boolean esc = false;
                while (p < e && b[p] != ',' && b[p] != ' ') { if (b[p] == '\\') { esc = true; p++; } p++; }
                String measurement = str(b, ms, p - ms, esc);
                // tags
                int nt = 0;
                while (p < e && b[p] == ',') {
                    p++;
                    int ks = p;
                    esc = false;
                    while (p < e && b[p] != '=') { if (b[p] == '\\') { esc = true; p++; } p++; }
                    String k = str(b, ks, p - ks, esc);
                    p++;
                    int vs = p;
                    esc = false;
                    while (p < e && b[p] != ',' && b[p] != ' ') { if (b[p] == '\\') { esc = true; p++; } p++; }
                    String v = str(b, vs, p - vs, esc);
                    if (v.isEmpty()) continue;
                    if (nt + 2 > tagKv.length) tagKv = Arrays.copyOf(tagKv, tagKv.length * 2);
                    tagKv[nt++] = k;
                    tagKv[nt++] = v;
                }
                if (p >= e || b[p] != ' ') throw new IllegalArgumentException("missing fields");
                p++;
                // fields
                int nf = 0;
                while (p < e && b[p] != ' ') {
                    int ks = p;
                    esc = false;
                    while (p < e && b[p] != '=') { if (b[p] == '\\') { esc = true; p++; } p++; }
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
                emit(measurement, tagKv, nt, fieldNames, fieldVals, nf, ts, out);
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

    private void emit(String measurement, String[] tagKv, int nt, String[] fieldNames, double[] fieldVals, int nf,
                      long ts, WriteBatch out) {
        // sort tags by key (insertion sort: usually already sorted, and short)
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
        // drop duplicate keys (last wins) and find where __name__ goes
        int w = 0;
        for (int a = 0; a < nt; a += 2) {
            if (tagKv[a].equals(Labels.NAME)) continue;
            if (w >= 2 && tagKv[w - 2].equals(tagKv[a])) { tagKv[w - 1] = tagKv[a + 1]; continue; }
            tagKv[w] = tagKv[a];
            tagKv[w + 1] = tagKv[a + 1];
            w += 2;
        }
        int at = 0;
        while (at < w && tagKv[at].compareTo(Labels.NAME) < 0) at += 2;
        for (int f = 0; f < nf; f++) {
            String[] kv = new String[w + 2];
            System.arraycopy(tagKv, 0, kv, 0, at);
            kv[at] = Labels.NAME;
            kv[at + 1] = metricName(measurement, fieldNames[f]);
            System.arraycopy(tagKv, at, kv, at + 2, w - at);
            out.add(Labels.fromSorted(kv), ts, fieldVals[f]);
        }
    }

    private final HashMap<String, HashMap<String, String>> names = new HashMap<>();

    private String metricName(String measurement, String field) {
        return names.computeIfAbsent(measurement, k -> new HashMap<>())
                .computeIfAbsent(field, f -> measurement.isEmpty() ? f : measurement + "_" + f);
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
