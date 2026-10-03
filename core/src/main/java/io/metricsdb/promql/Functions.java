package io.metricsdb.promql;

import java.util.Set;

/** The functions in the supported subset and how each treats the metric name. */
final class Functions {
    private Functions() {}

    /** Functions over a range vector, evaluated per step on the window (t - range, t]. */
    static final Set<String> RANGE = Set.of("rate", "irate", "increase", "delta", "max_over_time", "min_over_time",
            "avg_over_time", "sum_over_time", "count_over_time", "last_over_time", "stddev_over_time",
            "stdvar_over_time", "present_over_time");

    /**
     * Range functions that keep the metric name, following MetricsQL: they do not change what
     * the series measures. Prometheus drops the name for every function; MetricsQL keeps it for
     * these, and the standard benchmark's PromQL queries ({@code max(max_over_time(..)) by (__name__)})
     * are written for that behaviour.
     */
    static final Set<String> KEEP_NAME = Set.of("max_over_time", "min_over_time", "avg_over_time", "last_over_time");

    /** Range functions that can be answered from rollup buckets, by rollup aggregate. */
    static final Set<String> ROLLUP_ABLE = Set.of("max_over_time", "min_over_time", "avg_over_time", "sum_over_time",
            "count_over_time", "last_over_time");

    static final Set<String> INSTANT = Set.of("abs", "ceil", "floor", "round", "sqrt", "exp", "ln", "log2", "log10",
            "clamp_min", "clamp_max", "clamp", "time", "vector", "scalar", "histogram_quantile");

    static boolean isKnown(String f) { return RANGE.contains(f) || INSTANT.contains(f); }
}
