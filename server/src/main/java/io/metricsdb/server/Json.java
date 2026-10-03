package io.metricsdb.server;

import io.metricsdb.model.Labels;
import io.metricsdb.promql.Engine;

import java.math.BigDecimal;
import java.util.List;

/** Hand-written JSON for the Prometheus HTTP API: query responses can be large, so no reflection. */
public final class Json {
    private Json() {}

    public static String value(double v) {
        if (Double.isNaN(v)) return "NaN";
        if (v == Double.POSITIVE_INFINITY) return "+Inf";
        if (v == Double.NEGATIVE_INFINITY) return "-Inf";
        if (v == Math.rint(v) && Math.abs(v) < 1e15) return Long.toString((long) v);
        String s = Double.toString(v);
        return s.indexOf('E') >= 0 ? new BigDecimal(s).toPlainString() : s;
    }

    public static void time(StringBuilder sb, long ms) {
        if (ms % 1000 == 0) {
            sb.append(ms / 1000);
            return;
        }
        String frac = String.format("%03d", Math.floorMod(ms, 1000L));
        while (frac.endsWith("0")) frac = frac.substring(0, frac.length() - 1);
        sb.append(Math.floorDiv(ms, 1000L)).append('.').append(frac);
    }

    public static void str(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }

    public static void labels(StringBuilder sb, Labels l) {
        sb.append('{');
        for (int i = 0; i < l.size(); i++) {
            if (i > 0) sb.append(',');
            str(sb, l.name(i));
            sb.append(':');
            str(sb, l.value(i));
        }
        sb.append('}');
    }

    public static String error(String type, String msg) {
        StringBuilder sb = new StringBuilder("{\"status\":\"error\",\"errorType\":");
        str(sb, type);
        sb.append(",\"error\":");
        str(sb, msg);
        return sb.append('}').toString();
    }

    public static String stringList(List<String> xs) {
        StringBuilder sb = new StringBuilder("{\"status\":\"success\",\"data\":[");
        for (int i = 0; i < xs.size(); i++) {
            if (i > 0) sb.append(',');
            str(sb, xs.get(i));
        }
        return sb.append("]}").toString();
    }

    public static String result(Engine.Result r, List<String> warnings, boolean partial) {
        StringBuilder sb = new StringBuilder(1024 + r.series.size() * 64);
        sb.append("{\"status\":\"success\",");
        if (!warnings.isEmpty()) {
            sb.append("\"warnings\":[");
            for (int i = 0; i < warnings.size(); i++) {
                if (i > 0) sb.append(',');
                str(sb, warnings.get(i));
            }
            sb.append("],");
        }
        sb.append("\"isPartial\":").append(partial).append(',');
        sb.append("\"stats\":{\"seriesFetched\":").append(r.seriesSelected).append(",\"samplesLoaded\":").append(r.samplesLoaded)
                .append(",\"rollup\":").append(r.usedRollup).append("},");
        sb.append("\"data\":{\"resultType\":\"");
        switch (r.type) {
            case SCALAR -> {
                sb.append("scalar\",\"result\":[");
                time(sb, r.time(r.steps - 1));
                sb.append(",\"").append(value(r.scalar[r.steps - 1])).append("\"]");
            }
            case VECTOR -> {
                sb.append("vector\",\"result\":[");
                boolean first = true;
                for (Engine.StepSeries s : r.series) {
                    if (!s.has[0]) continue;
                    if (!first) sb.append(',');
                    first = false;
                    sb.append("{\"metric\":");
                    labels(sb, s.labels);
                    sb.append(",\"value\":[");
                    time(sb, r.time(0));
                    sb.append(",\"").append(value(s.v[0])).append("\"]}");
                }
                sb.append(']');
            }
            case MATRIX -> {
                sb.append("matrix\",\"result\":[");
                for (int k = 0; k < r.series.size(); k++) {
                    Engine.StepSeries s = r.series.get(k);
                    if (k > 0) sb.append(',');
                    sb.append("{\"metric\":");
                    labels(sb, s.labels);
                    sb.append(",\"values\":[");
                    boolean first = true;
                    for (int i = 0; i < r.steps; i++) {
                        if (!s.has[i]) continue;
                        if (!first) sb.append(',');
                        first = false;
                        sb.append('[');
                        time(sb, r.time(i));
                        sb.append(",\"").append(value(s.v[i])).append("\"]");
                    }
                    sb.append("]}");
                }
                sb.append(']');
            }
        }
        return sb.append("}}").toString();
    }
}
