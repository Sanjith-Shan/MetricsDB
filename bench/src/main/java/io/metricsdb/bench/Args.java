package io.metricsdb.bench;

import java.util.HashMap;
import java.util.Map;

/** {@code --name value} flags. */
public final class Args {
    private final Map<String, String> m = new HashMap<>();

    public Args(String[] a) {
        for (int i = 0; i < a.length; i++) {
            if (!a[i].startsWith("--")) throw new IllegalArgumentException("expected --flag, got " + a[i]);
            String k = a[i].substring(2);
            if (i + 1 < a.length && !a[i + 1].startsWith("--")) m.put(k, a[++i]);
            else m.put(k, "true");
        }
    }

    public String str(String k, String def) { return m.getOrDefault(k, def); }

    public String req(String k) {
        String v = m.get(k);
        if (v == null) throw new IllegalArgumentException("missing --" + k);
        return v;
    }

    public int i(String k, int def) { return m.containsKey(k) ? Integer.parseInt(m.get(k)) : def; }
    public long l(String k, long def) { return m.containsKey(k) ? Long.parseLong(m.get(k)) : def; }
    public double d(String k, double def) { return m.containsKey(k) ? Double.parseDouble(m.get(k)) : def; }
    public boolean b(String k) { return Boolean.parseBoolean(m.getOrDefault(k, "false")); }
}
