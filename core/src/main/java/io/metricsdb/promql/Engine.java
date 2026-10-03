package io.metricsdb.promql;

import io.metricsdb.block.Rollup;
import io.metricsdb.model.Labels;
import io.metricsdb.storage.QueryContext;
import io.metricsdb.storage.Queryable;
import io.metricsdb.storage.RollupSeries;
import io.metricsdb.storage.SampleArray;
import io.metricsdb.storage.SeriesChunks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Evaluates a parsed query over a grid of steps. Every node produces, per output series, one
 * value per step (or none). Selectors decode each series once for the whole query range and
 * then slide two pointers along the steps, so a range query costs one pass over the samples
 * instead of one lookup per step.
 */
public final class Engine {

    public enum RollupMode { OFF, AUTO, FORCE }

    public static final class Options {
        public long lookbackMs = 300_000;
        /** Align start and end to the step like VictoriaMetrics does for ranges of 50+ points. */
        public boolean alignLikeVictoriaMetrics = true;
        public long rollupResMs = 300_000;
        /** In AUTO mode rollups are used when the step is at least this long. */
        public long rollupMinStepMs = 3_600_000;
        public int maxSteps = 11_000;
    }

    /** One output series: a value per step, {@code has[i]} false where there is none. */
    public static final class StepSeries {
        public final Labels labels;
        public final double[] v;
        public final boolean[] has;

        StepSeries(Labels labels, int n) {
            this.labels = labels;
            this.v = new double[n];
            this.has = new boolean[n];
        }

        boolean empty() {
            for (boolean b : has) if (b) return false;
            return true;
        }
    }

    public enum ResultType { MATRIX, VECTOR, SCALAR }

    public static final class Result {
        public ResultType type;
        public long start, end, step;
        public int steps;
        public List<StepSeries> series = List.of();
        public double[] scalar;
        public boolean usedRollup;
        public long seriesSelected, samplesLoaded;

        public long time(int i) { return start + i * step; }
    }

    private sealed interface Value permits Scalar, Vector {}
    private record Scalar(double[] v) implements Value {}
    private record Vector(List<StepSeries> series) implements Value {}

    private final class Ctx {
        final long start, step;
        final int n;
        final QueryContext qctx;
        final RollupMode rollup;
        boolean usedRollup;

        Ctx(long start, long step, int n, QueryContext qctx, RollupMode rollup) {
            this.start = start;
            this.step = step;
            this.n = n;
            this.qctx = qctx;
            this.rollup = rollup;
        }

        long end() { return start + (n - 1) * step; }
        long ts(int i) { return start + i * step; }
    }

    private final Queryable storage;
    private final Options opt;

    public Engine(Queryable storage, Options opt) {
        this.storage = storage;
        this.opt = opt;
    }

    public Options options() { return opt; }

    public Result rangeQuery(String q, long startMs, long endMs, long stepMs, QueryContext qctx, RollupMode mode) {
        if (stepMs <= 0) throw new ParseException("step must be positive", -1);
        if (endMs < startMs) throw new ParseException("end is before start", -1);
        if (opt.alignLikeVictoriaMetrics && (endMs - startMs) >= 50 * stepMs) {
            long pointsBefore = (endMs - startMs) / stepMs + 1;
            startMs -= Math.floorMod(startMs, stepMs);
            long adj = Math.floorMod(endMs, stepMs);
            if (adj > 0) endMs += stepMs - adj;
            // keep the number of points unchanged by the rounding, as VictoriaMetrics does
            if ((endMs - startMs) / stepMs + 1 != pointsBefore) endMs -= stepMs;
        }
        long steps = (endMs - startMs) / stepMs + 1;
        if (steps > opt.maxSteps) {
            throw new QueryContext.LimitExceeded("steps", "query has " + steps + " steps, over the limit of " + opt.maxSteps + "; increase the step");
        }
        Ast.Expr e = Parser.parse(q);
        Ctx c = new Ctx(startMs, stepMs, (int) steps, qctx, mode);
        Value v = eval(e, c);
        Result r = new Result();
        r.start = startMs;
        r.step = stepMs;
        r.steps = c.n;
        r.end = c.end();
        r.usedRollup = c.usedRollup;
        r.seriesSelected = qctx.seriesSelected();
        r.samplesLoaded = qctx.samplesLoaded();
        if (v instanceof Scalar s) {
            r.type = ResultType.SCALAR;
            r.scalar = s.v;
        } else {
            r.type = ResultType.MATRIX;
            List<StepSeries> out = new ArrayList<>();
            for (StepSeries s : ((Vector) v).series) if (!s.empty()) out.add(s);
            out.sort((a, b) -> a.labels.compareTo(b.labels));
            r.series = out;
        }
        return r;
    }

    public Result instantQuery(String q, long timeMs, QueryContext qctx, RollupMode mode) {
        Result r = rangeQuery(q, timeMs, timeMs, 1, qctx, mode);
        r.type = r.type == ResultType.SCALAR ? ResultType.SCALAR : ResultType.VECTOR;
        return r;
    }

    // ------------------------------------------------------------------ evaluation

    private Value eval(Ast.Expr e, Ctx c) {
        c.qctx.checkDeadline();
        return switch (e) {
            case Ast.Num num -> {
                double[] v = new double[c.n];
                Arrays.fill(v, num.value());
                yield new Scalar(v);
            }
            case Ast.Str s -> throw new ParseException("string literal is not a valid expression here", -1);
            case Ast.VectorSel vs -> selectInstant(vs, c);
            case Ast.MatrixSel ms -> throw new ParseException("a range vector cannot be a query result here; wrap it in a function such as rate()", -1);
            case Ast.Neg neg -> negate(eval(neg.expr(), c));
            case Ast.Call call -> call(call, c);
            case Ast.Agg agg -> aggregate(agg, c);
            case Ast.Binary bin -> binary(bin, c);
        };
    }

    private Value negate(Value v) {
        if (v instanceof Scalar s) {
            double[] o = new double[s.v.length];
            for (int i = 0; i < o.length; i++) o[i] = -s.v[i];
            return new Scalar(o);
        }
        List<StepSeries> out = new ArrayList<>();
        for (StepSeries s : ((Vector) v).series) {
            StepSeries o = new StepSeries(s.labels.withoutName(), s.v.length);
            for (int i = 0; i < s.v.length; i++) { o.v[i] = -s.v[i]; o.has[i] = s.has[i]; }
            out.add(o);
        }
        return new Vector(out);
    }

    private Vector selectInstant(Ast.VectorSel vs, Ctx c) {
        long mint = c.start - vs.offsetMs() - opt.lookbackMs + 1;
        long maxt = c.end() - vs.offsetMs();
        List<SeriesChunks> sel = storage.select(vs.matchers(), mint, maxt, c.qctx);
        List<StepSeries> out = new ArrayList<>(sel.size());
        for (SeriesChunks sc : sel) {
            SampleArray sa = SampleArray.decode(sc.chunks(), mint, maxt, c.qctx);
            StepSeries s = new StepSeries(sc.labels(), c.n);
            int j = 0;
            for (int i = 0; i < c.n; i++) {
                long ts = c.ts(i) - vs.offsetMs();
                while (j < sa.n && sa.t[j] <= ts) j++;
                if (j > 0 && sa.t[j - 1] > ts - opt.lookbackMs) {
                    s.v[i] = sa.v[j - 1];
                    s.has[i] = true;
                }
            }
            out.add(s);
        }
        return new Vector(out);
    }

    // ------------------------------------------------------------------ functions

    private Value call(Ast.Call call, Ctx c) {
        String f = call.func();
        List<Ast.Expr> args = call.args();
        if (Functions.RANGE.contains(f)) {
            if (args.size() != 1 || !(args.get(0) instanceof Ast.MatrixSel ms)) {
                throw new ParseException(f + " expects one range vector argument, like " + f + "(x[5m])", -1);
            }
            return rangeFunction(f, ms, c);
        }
        switch (f) {
            case "time" -> {
                double[] v = new double[c.n];
                for (int i = 0; i < c.n; i++) v[i] = c.ts(i) / 1000.0;
                return new Scalar(v);
            }
            case "vector" -> {
                Scalar s = asScalar(eval(args.get(0), c), "vector");
                StepSeries o = new StepSeries(Labels.EMPTY, c.n);
                for (int i = 0; i < c.n; i++) { o.v[i] = s.v[i]; o.has[i] = true; }
                return new Vector(List.of(o));
            }
            case "scalar" -> {
                Vector v = asVector(eval(args.get(0), c), "scalar");
                double[] o = new double[c.n];
                for (int i = 0; i < c.n; i++) {
                    int count = 0;
                    double val = Double.NaN;
                    for (StepSeries s : v.series) if (s.has[i]) { count++; val = s.v[i]; }
                    o[i] = count == 1 ? val : Double.NaN;
                }
                return new Scalar(o);
            }
            case "histogram_quantile" -> {
                Scalar phi = asScalar(eval(args.get(0), c), f);
                return histogramQuantile(phi, asVector(eval(args.get(1), c), f), c);
            }
            default -> {
                Vector v = asVector(eval(args.get(0), c), f);
                double[] p1 = args.size() > 1 ? asScalar(eval(args.get(1), c), f).v : null;
                double[] p2 = args.size() > 2 ? asScalar(eval(args.get(2), c), f).v : null;
                List<StepSeries> out = new ArrayList<>();
                for (StepSeries s : v.series) {
                    StepSeries o = new StepSeries(s.labels.withoutName(), c.n);
                    for (int i = 0; i < c.n; i++) {
                        if (!s.has[i]) continue;
                        double x = s.v[i];
                        o.v[i] = switch (f) {
                            case "abs" -> Math.abs(x);
                            case "ceil" -> Math.ceil(x);
                            case "floor" -> Math.floor(x);
                            case "round" -> {
                                double to = p1 == null ? 1 : p1[i];
                                yield Math.floor(x / to + 0.5) * to;
                            }
                            case "sqrt" -> Math.sqrt(x);
                            case "exp" -> Math.exp(x);
                            case "ln" -> Math.log(x);
                            case "log2" -> Math.log(x) / Math.log(2);
                            case "log10" -> Math.log10(x);
                            case "clamp_min" -> Math.max(x, p1[i]);
                            case "clamp_max" -> Math.min(x, p1[i]);
                            case "clamp" -> Math.max(p1[i], Math.min(p2[i], x));
                            default -> throw new ParseException("unsupported function " + f, -1);
                        };
                        o.has[i] = true;
                    }
                    out.add(o);
                }
                return new Vector(out);
            }
        }
    }

    private Value rangeFunction(String f, Ast.MatrixSel ms, Ctx c) {
        long w = ms.rangeMs();
        long off = ms.sel().offsetMs();
        if (useRollup(f, w, off, c)) {
            Vector v = rangeFromRollup(f, ms, c);
            if (v != null) {
                c.usedRollup = true;
                return v;
            }
        }
        long mint = c.start - off - w + 1;
        long maxt = c.end() - off;
        List<SeriesChunks> sel = storage.select(ms.sel().matchers(), mint, maxt, c.qctx);
        boolean keepName = Functions.KEEP_NAME.contains(f);
        List<StepSeries> out = new ArrayList<>(sel.size());
        for (SeriesChunks sc : sel) {
            SampleArray sa = SampleArray.decode(sc.chunks(), mint, maxt, c.qctx);
            StepSeries s = new StepSeries(keepName ? sc.labels() : sc.labels().withoutName(), c.n);
            int lo = 0, hi = 0;
            for (int i = 0; i < c.n; i++) {
                long ts = c.ts(i) - off;
                while (hi < sa.n && sa.t[hi] <= ts) hi++;
                while (lo < hi && sa.t[lo] <= ts - w) lo++;
                if (hi == lo) continue;
                double r = windowValue(f, sa, lo, hi, ts - w, ts, w);
                if (Double.isNaN(r) && (f.equals("rate") || f.equals("increase") || f.equals("delta") || f.equals("irate"))) continue;
                s.v[i] = r;
                s.has[i] = true;
            }
            out.add(s);
        }
        return new Vector(out);
    }

    private static double windowValue(String f, SampleArray sa, int lo, int hi, long rangeStart, long rangeEnd, long w) {
        switch (f) {
            case "max_over_time": {
                double m = sa.v[lo];
                for (int k = lo + 1; k < hi; k++) if (sa.v[k] > m || Double.isNaN(m)) m = sa.v[k];
                return m;
            }
            case "min_over_time": {
                double m = sa.v[lo];
                for (int k = lo + 1; k < hi; k++) if (sa.v[k] < m || Double.isNaN(m)) m = sa.v[k];
                return m;
            }
            case "sum_over_time": {
                double s = 0;
                for (int k = lo; k < hi; k++) s += sa.v[k];
                return s;
            }
            case "avg_over_time": {
                double s = 0;
                for (int k = lo; k < hi; k++) s += sa.v[k];
                return s / (hi - lo);
            }
            case "count_over_time": return hi - lo;
            case "present_over_time": return 1;
            case "last_over_time": return sa.v[hi - 1];
            case "stddev_over_time":
            case "stdvar_over_time": {
                double mean = 0, m2 = 0;
                int cnt = 0;
                for (int k = lo; k < hi; k++) {
                    cnt++;
                    double d = sa.v[k] - mean;
                    mean += d / cnt;
                    m2 += d * (sa.v[k] - mean);
                }
                double var = m2 / cnt;
                return f.equals("stddev_over_time") ? Math.sqrt(var) : var;
            }
            case "irate": {
                if (hi - lo < 2) return Double.NaN;
                double last = sa.v[hi - 1], prev = sa.v[hi - 2];
                double dt = (sa.t[hi - 1] - sa.t[hi - 2]) / 1000.0;
                if (dt == 0) return Double.NaN;
                return (last < prev ? last : last - prev) / dt;
            }
            case "rate": return extrapolatedRate(sa, lo, hi, rangeStart, rangeEnd, true, true, w);
            case "increase": return extrapolatedRate(sa, lo, hi, rangeStart, rangeEnd, true, false, w);
            case "delta": return extrapolatedRate(sa, lo, hi, rangeStart, rangeEnd, false, false, w);
            default: throw new ParseException("unsupported function " + f, -1);
        }
    }

    /** Prometheus' extrapolated rate, increase and delta (promql/functions.go). */
    private static double extrapolatedRate(SampleArray sa, int lo, int hi, long rangeStart, long rangeEnd,
                                           boolean counter, boolean isRate, long w) {
        if (hi - lo < 2) return Double.NaN;
        double first = sa.v[lo], last = sa.v[hi - 1];
        double result = last - first;
        if (counter) {
            for (int k = lo + 1; k < hi; k++) if (sa.v[k] < sa.v[k - 1]) result += sa.v[k - 1];
        }
        double durationToStart = (sa.t[lo] - rangeStart) / 1000.0;
        double durationToEnd = (rangeEnd - sa.t[hi - 1]) / 1000.0;
        double sampled = (sa.t[hi - 1] - sa.t[lo]) / 1000.0;
        double avgBetween = sampled / (hi - lo - 1);
        if (counter && result > 0 && first >= 0) {
            double durationToZero = sampled * (first / result);
            if (durationToZero < durationToStart) durationToStart = durationToZero;
        }
        double threshold = avgBetween * 1.1;
        double extrapolateTo = sampled;
        extrapolateTo += durationToStart < threshold ? durationToStart : avgBetween / 2;
        extrapolateTo += durationToEnd < threshold ? durationToEnd : avgBetween / 2;
        double factor = extrapolateTo / sampled;
        if (isRate) factor /= w / 1000.0;
        return result * factor;
    }

    // ------------------------------------------------------------------ rollups

    private boolean useRollup(String f, long w, long off, Ctx c) {
        if (c.rollup == RollupMode.OFF || !Functions.ROLLUP_ABLE.contains(f)) return false;
        long res = opt.rollupResMs;
        boolean aligned = w % res == 0 && off % res == 0 && Math.floorMod(c.start, res) == 0 && (c.n == 1 || c.step % res == 0);
        if (!aligned) return false;
        return c.rollup == RollupMode.FORCE || c.step >= opt.rollupMinStepMs;
    }

    /**
     * Answers a window function from rollup buckets. A bucket ending at e covers (e - res, e], so
     * with bucket-aligned steps and windows the window (t - w, t] is exactly the buckets ending
     * in that interval, and the result equals the raw evaluation.
     */
    private Vector rangeFromRollup(String f, Ast.MatrixSel ms, Ctx c) {
        long w = ms.rangeMs(), off = ms.sel().offsetMs();
        long mint = c.start - off - w + 1;
        long maxt = c.end() - off;
        List<RollupSeries> sel = storage.selectRollup(ms.sel().matchers(), mint, maxt, opt.rollupResMs, c.qctx);
        if (sel == null) return null;
        int agg = switch (f) {
            case "max_over_time" -> Rollup.MAX;
            case "min_over_time" -> Rollup.MIN;
            case "sum_over_time", "avg_over_time" -> Rollup.SUM;
            case "count_over_time" -> Rollup.COUNT;
            default -> Rollup.LAST;
        };
        boolean keepName = Functions.KEEP_NAME.contains(f);
        List<StepSeries> out = new ArrayList<>(sel.size());
        for (RollupSeries rs : sel) {
            SampleArray a = SampleArray.decode(rs.aggs()[agg], mint, maxt, c.qctx);
            SampleArray cnt = f.equals("avg_over_time") ? SampleArray.decode(rs.aggs()[Rollup.COUNT], mint, maxt, c.qctx) : null;
            StepSeries s = new StepSeries(keepName ? rs.labels() : rs.labels().withoutName(), c.n);
            int lo = 0, hi = 0;
            for (int i = 0; i < c.n; i++) {
                long ts = c.ts(i) - off;
                while (hi < a.n && a.t[hi] <= ts) hi++;
                while (lo < hi && a.t[lo] <= ts - w) lo++;
                if (hi == lo) continue;
                double r;
                switch (f) {
                    case "max_over_time" -> { r = a.v[lo]; for (int k = lo + 1; k < hi; k++) r = Math.max(r, a.v[k]); }
                    case "min_over_time" -> { r = a.v[lo]; for (int k = lo + 1; k < hi; k++) r = Math.min(r, a.v[k]); }
                    case "sum_over_time", "count_over_time" -> { r = 0; for (int k = lo; k < hi; k++) r += a.v[k]; }
                    case "avg_over_time" -> {
                        double sum = 0, n = 0;
                        for (int k = lo; k < hi; k++) sum += a.v[k];
                        // count buckets share the sum buckets' timestamps
                        int clo = lowerBound(cnt, a.t[lo]), chi = lowerBound(cnt, a.t[hi - 1] + 1);
                        for (int k = clo; k < chi; k++) n += cnt.v[k];
                        r = sum / n;
                    }
                    default -> r = a.v[hi - 1];
                }
                s.v[i] = r;
                s.has[i] = true;
            }
            out.add(s);
        }
        return new Vector(out);
    }

    private static int lowerBound(SampleArray a, long t) {
        int idx = Arrays.binarySearch(a.t, 0, a.n, t);
        return idx >= 0 ? idx : -idx - 1;
    }

    // ------------------------------------------------------------------ aggregation

    private Value aggregate(Ast.Agg agg, Ctx c) {
        Vector in = asVector(eval(agg.expr(), c), agg.op());
        double[] param = agg.param() != null ? asScalar(eval(agg.param(), c), agg.op()).v : null;
        String op = agg.op();
        Set<String> grouping = new HashSet<>(agg.grouping());
        Map<Labels, List<StepSeries>> groups = new LinkedHashMap<>();
        for (StepSeries s : in.series) {
            Labels key;
            if (agg.without()) {
                Set<String> drop = new HashSet<>(grouping);
                drop.add(Labels.NAME);
                key = s.labels.without(drop);
            } else {
                key = s.labels.only(grouping);
            }
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(s);
        }
        List<StepSeries> out = new ArrayList<>();
        if (op.equals("topk") || op.equals("bottomk")) {
            for (List<StepSeries> members : groups.values()) {
                List<StepSeries> kept = new ArrayList<>();
                Map<StepSeries, StepSeries> copies = new HashMap<>();
                for (int i = 0; i < c.n; i++) {
                    int k = (int) param[i];
                    final int step = i;
                    List<StepSeries> present = new ArrayList<>();
                    for (StepSeries s : members) if (s.has[step]) present.add(s);
                    present.sort((a, b) -> op.equals("topk") ? Double.compare(b.v[step], a.v[step]) : Double.compare(a.v[step], b.v[step]));
                    for (int r = 0; r < Math.min(k, present.size()); r++) {
                        StepSeries src = present.get(r);
                        StepSeries dst = copies.computeIfAbsent(src, x -> {
                            StepSeries ns = new StepSeries(x.labels, c.n);
                            kept.add(ns);
                            return ns;
                        });
                        dst.v[step] = src.v[step];
                        dst.has[step] = true;
                    }
                }
                out.addAll(kept);
            }
            return new Vector(out);
        }
        for (var e : groups.entrySet()) {
            StepSeries o = new StepSeries(e.getKey(), c.n);
            List<StepSeries> members = e.getValue();
            double[] vals = new double[members.size()];
            for (int i = 0; i < c.n; i++) {
                int m = 0;
                for (StepSeries s : members) if (s.has[i]) vals[m++] = s.v[i];
                if (m == 0) continue;
                o.v[i] = reduce(op, vals, m, param == null ? 0 : param[i]);
                o.has[i] = true;
            }
            out.add(o);
        }
        return new Vector(out);
    }

    private static double reduce(String op, double[] v, int m, double param) {
        switch (op) {
            case "sum": { double s = 0; for (int k = 0; k < m; k++) s += v[k]; return s; }
            case "avg": { double s = 0; for (int k = 0; k < m; k++) s += v[k]; return s / m; }
            case "count": return m;
            case "group": return 1;
            case "max": {
                double r = v[0];
                for (int k = 1; k < m; k++) if (v[k] > r || Double.isNaN(r)) r = v[k];
                return r;
            }
            case "min": {
                double r = v[0];
                for (int k = 1; k < m; k++) if (v[k] < r || Double.isNaN(r)) r = v[k];
                return r;
            }
            case "stddev":
            case "stdvar": {
                double mean = 0, m2 = 0;
                for (int k = 0; k < m; k++) {
                    double d = v[k] - mean;
                    mean += d / (k + 1);
                    m2 += d * (v[k] - mean);
                }
                double var = m2 / m;
                return op.equals("stddev") ? Math.sqrt(var) : var;
            }
            case "quantile": {
                double[] s = Arrays.copyOf(v, m);
                Arrays.sort(s);
                if (param < 0) return Double.NEGATIVE_INFINITY;
                if (param > 1) return Double.POSITIVE_INFINITY;
                double rank = param * (m - 1);
                int lo = (int) Math.floor(rank), hi = Math.min(m - 1, lo + 1);
                double weight = rank - lo;
                return s[lo] * (1 - weight) + s[hi] * weight;
            }
            default: throw new ParseException("unsupported aggregation " + op, -1);
        }
    }

    // ------------------------------------------------------------------ histogram_quantile

    private Value histogramQuantile(Scalar phi, Vector in, Ctx c) {
        Map<Labels, List<StepSeries>> groups = new LinkedHashMap<>();
        Map<StepSeries, Double> le = new HashMap<>();
        for (StepSeries s : in.series) {
            String l = s.labels.get("le");
            if (l == null) continue;
            double bound = l.equals("+Inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(l);
            le.put(s, bound);
            groups.computeIfAbsent(s.labels.without(Set.of("le", Labels.NAME)), k -> new ArrayList<>()).add(s);
        }
        List<StepSeries> out = new ArrayList<>();
        for (var e : groups.entrySet()) {
            List<StepSeries> buckets = e.getValue();
            buckets.sort((a, b) -> Double.compare(le.get(a), le.get(b)));
            StepSeries o = new StepSeries(e.getKey(), c.n);
            for (int i = 0; i < c.n; i++) {
                List<double[]> bs = new ArrayList<>();
                for (StepSeries b : buckets) if (b.has[i]) bs.add(new double[]{le.get(b), b.v[i]});
                if (bs.size() < 2 || bs.get(bs.size() - 1)[0] != Double.POSITIVE_INFINITY) continue;
                o.v[i] = bucketQuantile(phi.v[i], bs);
                o.has[i] = true;
            }
            out.add(o);
        }
        return new Vector(out);
    }

    private static double bucketQuantile(double q, List<double[]> b) {
        if (q < 0) return Double.NEGATIVE_INFINITY;
        if (q > 1) return Double.POSITIVE_INFINITY;
        // make cumulative counts monotonic, as Prometheus does
        for (int k = 1; k < b.size(); k++) if (b.get(k)[1] < b.get(k - 1)[1]) b.get(k)[1] = b.get(k - 1)[1];
        double total = b.get(b.size() - 1)[1];
        if (total == 0) return Double.NaN;
        double rank = q * total;
        int k = 0;
        while (k < b.size() - 1 && b.get(k)[1] < rank) k++;
        if (k == b.size() - 1) return b.get(b.size() - 2)[0];
        if (k == 0 && b.get(0)[0] <= 0) return b.get(0)[0];
        double start = 0, end = b.get(k)[0], count = b.get(k)[1];
        if (k > 0) {
            start = b.get(k - 1)[0];
            count -= b.get(k - 1)[1];
            rank -= b.get(k - 1)[1];
        }
        return start + (end - start) * (rank / count);
    }

    // ------------------------------------------------------------------ binary operators

    private static boolean isComparison(String op) {
        return op.equals("==") || op.equals("!=") || op.equals(">") || op.equals("<") || op.equals(">=") || op.equals("<=");
    }

    private static double arith(String op, double a, double b) {
        return switch (op) {
            case "+" -> a + b;
            case "-" -> a - b;
            case "*" -> a * b;
            case "/" -> a / b;
            case "%" -> a % b;
            case "^" -> Math.pow(a, b);
            case "==" -> a == b ? 1 : 0;
            case "!=" -> a != b ? 1 : 0;
            case ">" -> a > b ? 1 : 0;
            case "<" -> a < b ? 1 : 0;
            case ">=" -> a >= b ? 1 : 0;
            case "<=" -> a <= b ? 1 : 0;
            default -> throw new ParseException("unsupported operator " + op, -1);
        };
    }

    private Value binary(Ast.Binary b, Ctx c) {
        Value l = eval(b.lhs(), c), r = eval(b.rhs(), c);
        String op = b.op();
        boolean cmp = isComparison(op);
        if (l instanceof Scalar ls && r instanceof Scalar rs) {
            if (cmp && !b.returnBool()) throw new ParseException("comparisons between scalars must use the bool modifier", -1);
            double[] o = new double[c.n];
            for (int i = 0; i < c.n; i++) o[i] = arith(op, ls.v[i], rs.v[i]);
            return new Scalar(o);
        }
        if (l instanceof Vector lv && r instanceof Scalar rs) return vectorScalar(lv, rs.v, op, cmp, b.returnBool(), false, c);
        if (l instanceof Scalar ls && r instanceof Vector rv) return vectorScalar(rv, ls.v, op, cmp, b.returnBool(), true, c);
        Vector lv = (Vector) l, rv = (Vector) r;
        Map<Labels, StepSeries> rightBySig = new HashMap<>();
        for (StepSeries s : rv.series) {
            if (rightBySig.put(s.labels.withoutName(), s) != null) {
                throw new ParseException("many-to-many matching is not supported: duplicate series on the right-hand side", -1);
            }
        }
        List<StepSeries> out = new ArrayList<>();
        for (StepSeries ls : lv.series) {
            StepSeries rs = rightBySig.get(ls.labels.withoutName());
            if (rs == null) continue;
            StepSeries o = new StepSeries(cmp && !b.returnBool() ? ls.labels : ls.labels.withoutName(), c.n);
            for (int i = 0; i < c.n; i++) {
                if (!ls.has[i] || !rs.has[i]) continue;
                double v = arith(op, ls.v[i], rs.v[i]);
                if (cmp && !b.returnBool()) {
                    if (v == 0) continue;
                    v = ls.v[i];
                }
                o.v[i] = v;
                o.has[i] = true;
            }
            out.add(o);
        }
        return new Vector(out);
    }

    private Vector vectorScalar(Vector v, double[] s, String op, boolean cmp, boolean bool, boolean scalarLeft, Ctx c) {
        List<StepSeries> out = new ArrayList<>();
        for (StepSeries in : v.series) {
            StepSeries o = new StepSeries(cmp && !bool ? in.labels : in.labels.withoutName(), c.n);
            for (int i = 0; i < c.n; i++) {
                if (!in.has[i]) continue;
                double res = scalarLeft ? arith(op, s[i], in.v[i]) : arith(op, in.v[i], s[i]);
                if (cmp && !bool) {
                    if (res == 0) continue;
                    res = in.v[i];
                }
                o.v[i] = res;
                o.has[i] = true;
            }
            out.add(o);
        }
        return new Vector(out);
    }

    private static Vector asVector(Value v, String where) {
        if (v instanceof Vector vec) return vec;
        throw new ParseException(where + " expects an instant vector", -1);
    }

    private static Scalar asScalar(Value v, String where) {
        if (v instanceof Scalar s) return s;
        throw new ParseException(where + " expects a scalar", -1);
    }
}
