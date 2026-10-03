package io.metricsdb.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Sends every benchmark query to two databases and compares the answers: the same series (by
 * label set), the same timestamps, and values equal within a relative tolerance of 1e-9 (floating
 * point sums may be added in a different order). Reports mismatches per query type.
 */
public final class Diff {
    public static void run(Args a) throws Exception {
        String urlA = a.req("a"), urlB = a.req("b");
        String suffixB = a.str("b-suffix", "");
        double tol = a.d("tolerance", 1e-9);
        List<Queries.Q> qs = Queries.load(a.req("queries"), a.str("types", null));
        Map<String, int[]> perType = new LinkedHashMap<>(); // queries, mismatched, points compared, series compared
        ArrayNode examples = Env.JSON.createArrayNode();
        for (Queries.Q q : qs) {
            Queries.Resp ra = Queries.get(urlA, q.path(), Duration.ofSeconds(120));
            Queries.Resp rb = Queries.get(urlB, q.path() + suffixB, Duration.ofSeconds(120));
            int[] c = perType.computeIfAbsent(q.type(), k -> new int[4]);
            c[0]++;
            String why;
            if (ra.status() != 200 || rb.status() != 200) {
                why = "status " + ra.status() + " vs " + rb.status() + ": " + trim(ra.body()) + " | " + trim(rb.body());
            } else {
                why = compare(Env.JSON.readTree(ra.body()), Env.JSON.readTree(rb.body()), tol, c);
            }
            if (why != null) {
                c[1]++;
                if (examples.size() < 20) {
                    ObjectNode ex = Env.JSON.createObjectNode();
                    ex.put("type", q.type());
                    ex.put("path", q.path());
                    ex.put("why", why);
                    examples.add(ex);
                }
            }
        }
        int totalQ = 0, totalMis = 0, totalPts = 0, totalSeries = 0;
        for (var e : perType.entrySet()) {
            int[] c = e.getValue();
            totalQ += c[0]; totalMis += c[1]; totalPts += c[2]; totalSeries += c[3];
            ObjectNode row = Env.JSON.createObjectNode();
            row.put("exp", "exp4");
            row.put("ts", Env.now());
            row.put("a", a.str("a-name", "metricsdb"));
            row.put("b", a.str("b-name", "victoriametrics"));
            row.put("query_type", e.getKey());
            row.put("queries", c[0]);
            row.put("mismatched_queries", c[1]);
            row.put("series_compared", c[3]);
            row.put("points_compared", c[2]);
            row.put("tolerance", tol);
            Env.append(a.str("out", null), row);
        }
        ObjectNode total = Env.JSON.createObjectNode();
        total.put("exp", "exp4");
        total.put("ts", Env.now());
        total.put("query_type", "ALL");
        total.put("queries", totalQ);
        total.put("mismatched_queries", totalMis);
        total.put("series_compared", totalSeries);
        total.put("points_compared", totalPts);
        total.put("tolerance", tol);
        total.put("b_suffix", suffixB);
        total.set("examples", examples);
        total.set("machine", Env.machine());
        Env.append(a.str("out", null), total);
    }

    private static String trim(String s) { return s.length() > 200 ? s.substring(0, 200) : s; }

    static String compare(JsonNode a, JsonNode b, double tol, int[] c) {
        Map<String, JsonNode> sa = bySeries(a), sb = bySeries(b);
        if (!sa.keySet().equals(sb.keySet())) {
            List<String> onlyA = new ArrayList<>(sa.keySet()); onlyA.removeAll(sb.keySet());
            List<String> onlyB = new ArrayList<>(sb.keySet()); onlyB.removeAll(sa.keySet());
            return "series differ: only in a " + head(onlyA) + ", only in b " + head(onlyB);
        }
        for (var e : sa.entrySet()) {
            c[3]++;
            JsonNode va = e.getValue(), vb = sb.get(e.getKey());
            if (va.size() != vb.size()) return e.getKey() + ": " + va.size() + " points vs " + vb.size();
            for (int i = 0; i < va.size(); i++) {
                c[2]++;
                double ta = va.get(i).get(0).asDouble(), tb = vb.get(i).get(0).asDouble();
                if (ta != tb) return e.getKey() + ": timestamp " + ta + " vs " + tb + " at point " + i;
                double x = Double.parseDouble(va.get(i).get(1).asText()), y = Double.parseDouble(vb.get(i).get(1).asText());
                if (!(x == y || (Double.isNaN(x) && Double.isNaN(y)) || Math.abs(x - y) <= tol * Math.max(1, Math.max(Math.abs(x), Math.abs(y))))) {
                    return e.getKey() + ": value " + x + " vs " + y + " at t=" + ta;
                }
            }
        }
        return null;
    }

    private static String head(List<String> xs) {
        return xs.size() <= 3 ? xs.toString() : xs.subList(0, 3) + " and " + (xs.size() - 3) + " more";
    }

    private static Map<String, JsonNode> bySeries(JsonNode resp) {
        Map<String, JsonNode> out = new TreeMap<>();
        for (JsonNode s : resp.path("data").path("result")) {
            TreeMap<String, String> m = new TreeMap<>();
            s.get("metric").fields().forEachRemaining(f -> m.put(f.getKey(), f.getValue().asText()));
            out.put(m.toString(), s.has("values") ? s.get("values") : Env.JSON.createArrayNode().add(s.get("value")));
        }
        return out;
    }
}
