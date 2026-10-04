package io.metricsdb.server;

import io.metricsdb.model.Labels;
import io.metricsdb.promql.Ast;
import io.metricsdb.promql.Engine;
import io.metricsdb.promql.ParseException;
import io.metricsdb.promql.Parser;
import io.metricsdb.storage.QueryContext;
import io.metricsdb.storage.SampleArray;
import io.metricsdb.storage.SeriesChunks;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** The Prometheus HTTP query API, enough for Grafana's Prometheus data source and the benchmark's query runner. */
@RestController
public class QueryController {
    private final NodeConfig.Node node;
    private final NodeMetrics metrics;

    public QueryController(NodeConfig.Node node, NodeMetrics metrics) {
        this.node = node;
        this.metrics = metrics;
    }

    private QueryContext ctx(String timeout) {
        MetricsDbProperties.Query q = node.props().getQuery();
        long t = q.getTimeout().toMillis();
        if (timeout != null && !timeout.isBlank()) t = Math.min(t, parseDuration(timeout));
        return new QueryContext(q.getMaxSeries(), q.getMaxSamples(), t);
    }

    private Engine.RollupMode rollupMode(String p) {
        String m = p != null ? p : node.props().getQuery().getRollups();
        return switch (m.toLowerCase()) {
            case "off", "0", "false" -> Engine.RollupMode.OFF;
            case "force", "on", "1", "true" -> Engine.RollupMode.FORCE;
            default -> Engine.RollupMode.AUTO;
        };
    }

    @RequestMapping(value = "/api/v1/query_range", method = {RequestMethod.GET, RequestMethod.POST}, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> queryRange(@RequestParam String query, @RequestParam String start, @RequestParam String end,
                                             @RequestParam String step, @RequestParam(required = false) String timeout,
                                             @RequestParam(required = false) String rollup) {
        long t0 = System.nanoTime();
        QueryContext c = ctx(timeout);
        Engine.Result r = node.engine().rangeQuery(query, parseTime(start), parseTime(end), parseDuration(step), c, rollupMode(rollup));
        metrics.recordNanos("metricsdb.query", System.nanoTime() - t0, "type", "range", "path", r.usedRollup ? "rollup" : "raw");
        return ResponseEntity.ok(Json.result(r, c.warnings(), c.isPartial()));
    }

    @RequestMapping(value = "/api/v1/query", method = {RequestMethod.GET, RequestMethod.POST}, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> query(@RequestParam String query, @RequestParam(required = false) String time,
                                        @RequestParam(required = false) String timeout, @RequestParam(required = false) String rollup) {
        long t0 = System.nanoTime();
        QueryContext c = ctx(timeout);
        long ts = time == null || time.isBlank() ? System.currentTimeMillis() : parseTime(time);
        Engine.Result r = node.engine().instantQuery(query, ts, c, rollupMode(rollup));
        metrics.recordNanos("metricsdb.query", System.nanoTime() - t0, "type", "instant", "path", r.usedRollup ? "rollup" : "raw");
        return ResponseEntity.ok(Json.result(r, c.warnings(), c.isPartial()));
    }

    /**
     * Raw {@code match[]} values. Binding them to a List would let Spring split a single value on
     * commas, which cuts a selector like {@code {a="x",b="y"}} in half.
     */
    private static List<String> matches(jakarta.servlet.http.HttpServletRequest req, boolean required) {
        String[] v = req.getParameterValues("match[]");
        if (v == null || v.length == 0) {
            if (required) throw new ParseException("match[] is required", -1);
            return null;
        }
        return List.of(v);
    }

    private List<SeriesChunks> selectAll(List<String> match, String start, String end, QueryContext c) {
        long mint = start == null ? Long.MIN_VALUE : parseTime(start);
        long maxt = end == null ? Long.MAX_VALUE : parseTime(end);
        List<SeriesChunks> out = new ArrayList<>();
        for (String m : match) {
            Ast.Expr e = Parser.parse(m);
            if (!(e instanceof Ast.VectorSel vs)) throw new ParseException("match[] must be a series selector: " + m, -1);
            out.addAll(node.queryable().select(vs.matchers(), mint, maxt, c));
        }
        return out;
    }

    @RequestMapping(value = "/api/v1/series", method = {RequestMethod.GET, RequestMethod.POST}, produces = MediaType.APPLICATION_JSON_VALUE)
    public String series(jakarta.servlet.http.HttpServletRequest req, @RequestParam(required = false) String start,
                         @RequestParam(required = false) String end) {
        List<String> match = matches(req, true);
        QueryContext c = ctx(null);
        TreeSet<Labels> seen = new TreeSet<>();
        for (SeriesChunks sc : selectAll(match, start, end, c)) seen.add(sc.labels());
        StringBuilder sb = new StringBuilder("{\"status\":\"success\",\"data\":[");
        boolean first = true;
        for (Labels l : seen) {
            if (!first) sb.append(',');
            first = false;
            Json.labels(sb, l);
        }
        return sb.append("]}").toString();
    }

    @RequestMapping(value = "/api/v1/labels", method = {RequestMethod.GET, RequestMethod.POST}, produces = MediaType.APPLICATION_JSON_VALUE)
    public String labels(jakarta.servlet.http.HttpServletRequest req,
                         @RequestParam(required = false) String start, @RequestParam(required = false) String end) {
        List<String> match = matches(req, false);
        if (match != null && !match.isEmpty() || node.tsdb() == null) {
            TreeSet<String> names = new TreeSet<>();
            List<String> ms = match == null || match.isEmpty() ? List.of("{__name__=~\".+\"}") : match;
            for (SeriesChunks sc : selectAll(ms, start, end, ctx(null))) for (int i = 0; i < sc.labels().size(); i++) names.add(sc.labels().name(i));
            return Json.stringList(new ArrayList<>(names));
        }
        return Json.stringList(node.tsdb().labelNames());
    }

    @GetMapping(value = "/api/v1/label/{name}/values", produces = MediaType.APPLICATION_JSON_VALUE)
    public String labelValues(@PathVariable String name, jakarta.servlet.http.HttpServletRequest req,
                              @RequestParam(required = false) String start, @RequestParam(required = false) String end) {
        List<String> match = matches(req, false);
        if (match != null && !match.isEmpty() || node.tsdb() == null) {
            TreeSet<String> vals = new TreeSet<>();
            List<String> ms = match == null || match.isEmpty() ? List.of("{" + name + "=~\".+\"}") : match;
            for (SeriesChunks sc : selectAll(ms, start, end, ctx(null))) {
                String v = sc.labels().get(name);
                if (v != null) vals.add(v);
            }
            return Json.stringList(new ArrayList<>(vals));
        }
        return Json.stringList(node.tsdb().labelValues(name));
    }

    /** VictoriaMetrics-compatible export: one JSON object per series with raw samples. */
    @RequestMapping(value = "/api/v1/export", method = {RequestMethod.GET, RequestMethod.POST}, produces = "application/stream+json")
    public String export(jakarta.servlet.http.HttpServletRequest req, @RequestParam(required = false) String start,
                         @RequestParam(required = false) String end) {
        List<String> match = matches(req, true);
        QueryContext c = ctx(null);
        StringBuilder sb = new StringBuilder();
        long mint = start == null ? Long.MIN_VALUE : parseTime(start);
        long maxt = end == null ? Long.MAX_VALUE : parseTime(end);
        java.util.Map<Labels, List<io.metricsdb.storage.Chunk>> merged = new java.util.TreeMap<>();
        for (SeriesChunks sc : selectAll(match, start, end, c)) merged.computeIfAbsent(sc.labels(), k -> new ArrayList<>()).addAll(sc.chunks());
        for (var e : merged.entrySet()) {
            SampleArray sa = SampleArray.decode(e.getValue(), mint, maxt, c);
            sb.append("{\"metric\":");
            Json.labels(sb, e.getKey());
            sb.append(",\"values\":[");
            for (int i = 0; i < sa.n; i++) { if (i > 0) sb.append(','); sb.append(Json.value(sa.v[i])); }
            sb.append("],\"timestamps\":[");
            for (int i = 0; i < sa.n; i++) { if (i > 0) sb.append(','); sb.append(sa.t[i]); }
            sb.append("]}\n");
        }
        return sb.toString();
    }

    @GetMapping(value = "/api/v1/status/buildinfo", produces = MediaType.APPLICATION_JSON_VALUE)
    public String buildinfo() {
        return "{\"status\":\"success\",\"data\":{\"version\":\"2.45.0\",\"revision\":\"metricsdb\",\"branch\":\"main\",\"goVersion\":\"java21\"}}";
    }

    @GetMapping(value = "/api/v1/metadata", produces = MediaType.APPLICATION_JSON_VALUE)
    public String metadata() { return "{\"status\":\"success\",\"data\":{}}"; }

    @GetMapping(value = "/api/v1/status/tsdb", produces = MediaType.APPLICATION_JSON_VALUE)
    public String tsdbStatus() {
        if (node.tsdb() == null) return Json.error("bad_data", "cardinality status is per storage node; ask a storage node");
        var db = node.tsdb();
        StringBuilder sb = new StringBuilder("{\"status\":\"success\",\"data\":{\"headStats\":{\"numSeries\":").append(db.seriesCount())
                .append(",\"seriesLimit\":").append(db.options().maxSeries).append("},\"labelValueCountByLabelName\":[");
        List<String> names = db.labelNames();
        List<Map.Entry<String, Integer>> counts = new ArrayList<>();
        for (String n : names) counts.add(Map.entry(n, db.labelValueCount(n)));
        counts.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));
        for (int i = 0; i < Math.min(10, counts.size()); i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"name\":");
            Json.str(sb, counts.get(i).getKey());
            sb.append(",\"value\":").append(counts.get(i).getValue()).append('}');
        }
        return sb.append("]}}").toString();
    }

    // ------------------------------------------------------------------ errors

    @ExceptionHandler(ParseException.class)
    public ResponseEntity<String> bad(ParseException e) {
        metrics.counter("metricsdb.query.errors", "reason", "bad_data").increment();
        return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_JSON).body(Json.error("bad_data", e.getMessage()));
    }

    @ExceptionHandler(QueryContext.LimitExceeded.class)
    public ResponseEntity<String> limit(QueryContext.LimitExceeded e) {
        metrics.counter("metricsdb.query.errors", "reason", e.kind).increment();
        HttpStatus s = e.kind.equals("timeout") ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.UNPROCESSABLE_ENTITY;
        return ResponseEntity.status(s).contentType(MediaType.APPLICATION_JSON)
                .body(Json.error(e.kind.equals("timeout") ? "timeout" : "execution", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> badArg(IllegalArgumentException e) {
        return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_JSON).body(Json.error("bad_data", String.valueOf(e.getMessage())));
    }

    // ------------------------------------------------------------------ parameters

    static long parseTime(String s) {
        s = s.trim();
        try {
            return Math.round(Double.parseDouble(s) * 1000);
        } catch (NumberFormatException e) {
            return Instant.parse(s).toEpochMilli();
        }
    }

    static long parseDuration(String s) {
        s = s.trim();
        try {
            return Math.round(Double.parseDouble(s) * 1000);
        } catch (NumberFormatException e) {
            long total = 0;
            int i = 0;
            while (i < s.length()) {
                int j = i;
                while (j < s.length() && (Character.isDigit(s.charAt(j)) || s.charAt(j) == '.')) j++;
                double num = Double.parseDouble(s.substring(i, j));
                int k = j;
                while (k < s.length() && Character.isLetter(s.charAt(k))) k++;
                long unit = switch (s.substring(j, k)) {
                    case "ms" -> 1;
                    case "s" -> 1000;
                    case "m" -> 60_000;
                    case "h" -> 3_600_000;
                    case "d" -> 86_400_000;
                    case "w" -> 604_800_000;
                    default -> throw new IllegalArgumentException("bad duration " + s);
                };
                total += Math.round(num * unit);
                i = k;
            }
            return total;
        }
    }
}
