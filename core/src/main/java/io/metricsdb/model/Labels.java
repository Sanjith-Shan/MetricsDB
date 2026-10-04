package io.metricsdb.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * An immutable, sorted set of label pairs identifying one series. The metric name is the
 * {@code __name__} label. Stored as one flat array {name0, value0, name1, value1, ...}
 * sorted by name, so equality and hashing are cheap and deterministic across nodes.
 */
public final class Labels implements Comparable<Labels> {
    public static final String NAME = "__name__";
    public static final Labels EMPTY = new Labels(new String[0]);

    private final String[] kv;
    private final int hash;
    /** Set by the storage engine: this label set's series in one database, to skip the hash lookup. */
    public volatile Object seriesRef;

    private Labels(String[] sortedKv) {
        this.kv = sortedKv;
        this.hash = Arrays.hashCode(sortedKv);
    }

    public static Labels of(String... kv) {
        if (kv.length % 2 != 0) throw new IllegalArgumentException("odd label list");
        TreeMap<String, String> m = new TreeMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return fromMap(m);
    }

    public static Labels fromMap(Map<String, String> m) {
        TreeMap<String, String> sorted = m instanceof TreeMap<String, String> t ? t : new TreeMap<>(m);
        String[] arr = new String[sorted.size() * 2];
        int i = 0;
        for (var e : sorted.entrySet()) {
            if (e.getValue() == null || e.getValue().isEmpty()) continue;
            arr[i++] = e.getKey();
            arr[i++] = e.getValue();
        }
        return new Labels(i == arr.length ? arr : Arrays.copyOf(arr, i));
    }

    /** Builds from pairs that the caller guarantees are sorted by name with no duplicates and no empty values. */
    public static Labels fromSorted(String[] kv) {
        return new Labels(kv);
    }

    public int size() { return kv.length / 2; }
    public String name(int i) { return kv[2 * i]; }
    public String value(int i) { return kv[2 * i + 1]; }

    public String get(String name) {
        // label sets are small; linear scan beats binary search below ~16 pairs
        for (int i = 0; i < kv.length; i += 2) if (kv[i].equals(name)) return kv[i + 1];
        return null;
    }

    public String metricName() {
        String n = get(NAME);
        return n == null ? "" : n;
    }

    public Labels without(Collection<String> names) {
        List<String> out = new ArrayList<>(kv.length);
        for (int i = 0; i < kv.length; i += 2) {
            if (!names.contains(kv[i])) { out.add(kv[i]); out.add(kv[i + 1]); }
        }
        return out.size() == kv.length ? this : new Labels(out.toArray(new String[0]));
    }

    public Labels only(Collection<String> names) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < kv.length; i += 2) {
            if (names.contains(kv[i])) { out.add(kv[i]); out.add(kv[i + 1]); }
        }
        return out.size() == kv.length ? this : new Labels(out.toArray(new String[0]));
    }

    public Labels withoutName() {
        if (get(NAME) == null) return this;
        return without(List.of(NAME));
    }

    public Labels with(String name, String value) {
        TreeMap<String, String> m = toMap();
        m.put(name, value);
        return fromMap(m);
    }

    public TreeMap<String, String> toMap() {
        TreeMap<String, String> m = new TreeMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    /** Stable 64-bit hash used for consistent hashing; identical on every node and JVM. */
    public long stableHash() {
        long h = 0xcbf29ce484222325L;
        for (String s : kv) {
            for (int i = 0; i < s.length(); i++) {
                h ^= s.charAt(i);
                h *= 0x100000001b3L;
            }
            h ^= 0xff;
            h *= 0x100000001b3L;
        }
        // final avalanche (murmur3 fmix64) so nearby label sets spread over the ring
        h ^= h >>> 33; h *= 0xff51afd7ed558ccdL; h ^= h >>> 33; h *= 0xc4ceb9fe1a85ec53L; h ^= h >>> 33;
        return h;
    }

    String[] raw() { return kv; }

    @Override public boolean equals(Object o) {
        if (o == this) return true;
        return o instanceof Labels l && l.hash == hash && Arrays.equals(l.kv, kv);
    }

    @Override public int hashCode() { return hash; }

    @Override public int compareTo(Labels o) {
        int n = Math.min(kv.length, o.kv.length);
        for (int i = 0; i < n; i++) {
            int c = kv[i].compareTo(o.kv[i]);
            if (c != 0) return c;
        }
        return Integer.compare(kv.length, o.kv.length);
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        String name = get(NAME);
        if (name != null) sb.append(name);
        sb.append('{');
        boolean first = true;
        for (int i = 0; i < kv.length; i += 2) {
            if (kv[i].equals(NAME)) continue;
            if (!first) sb.append(", ");
            first = false;
            sb.append(kv[i]).append("=\"").append(kv[i + 1]).append('"');
        }
        return sb.append('}').toString();
    }
}
