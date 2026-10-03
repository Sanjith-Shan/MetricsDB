package io.metricsdb.ingest;

import io.metricsdb.model.Labels;
import io.metricsdb.storage.WriteBatch;

import java.util.TreeMap;

/** Parser for the Prometheus text exposition format, used when a node scrapes its own /metrics. */
public final class Exposition {
    private Exposition() {}

    public static int parse(String text, long defaultTs, Labels extra, WriteBatch out) {
        int samples = 0;
        for (String line : text.split("\n")) {
            line = line.strip();
            if (line.isEmpty() || line.startsWith("#")) continue;
            TreeMap<String, String> m = new TreeMap<>();
            int i = 0;
            while (i < line.length() && line.charAt(i) != '{' && line.charAt(i) != ' ') i++;
            m.put(Labels.NAME, line.substring(0, i));
            if (i < line.length() && line.charAt(i) == '{') {
                i++;
                while (i < line.length() && line.charAt(i) != '}') {
                    int eq = line.indexOf('=', i);
                    String k = line.substring(i, eq).strip();
                    int q = eq + 2; // skip ="
                    StringBuilder v = new StringBuilder();
                    while (q < line.length() && line.charAt(q) != '"') {
                        char c = line.charAt(q);
                        if (c == '\\' && q + 1 < line.length()) {
                            char d = line.charAt(++q);
                            v.append(d == 'n' ? '\n' : d);
                        } else {
                            v.append(c);
                        }
                        q++;
                    }
                    m.put(k, v.toString());
                    i = q + 1;
                    while (i < line.length() && (line.charAt(i) == ',' || line.charAt(i) == ' ')) i++;
                }
                i++;
            }
            String[] rest = line.substring(Math.min(i, line.length())).strip().split("\\s+");
            if (rest.length == 0 || rest[0].isEmpty()) continue;
            double v = switch (rest[0]) {
                case "+Inf", "Inf" -> Double.POSITIVE_INFINITY;
                case "-Inf" -> Double.NEGATIVE_INFINITY;
                case "NaN" -> Double.NaN;
                default -> Double.parseDouble(rest[0]);
            };
            long ts = rest.length > 1 ? Long.parseLong(rest[1]) : defaultTs;
            if (extra != null) for (int k = 0; k < extra.size(); k++) m.putIfAbsent(extra.name(k), extra.value(k));
            out.add(Labels.fromMap(m), ts, v);
            samples++;
        }
        return samples;
    }
}
