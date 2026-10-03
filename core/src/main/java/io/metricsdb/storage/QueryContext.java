package io.metricsdb.storage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-query budget: series selected, samples decoded and wall-clock time. Checked inside the
 * selection and decode loops, so a runaway query fails fast with a clear message instead of
 * exhausting the node's memory.
 */
public final class QueryContext {
    public static final class LimitExceeded extends RuntimeException {
        public final String kind;
        public LimitExceeded(String kind, String message) {
            super(message);
            this.kind = kind;
        }
    }

    private final long maxSeries;
    private final long maxSamples;
    private final long deadlineNanos;
    private final long timeoutMs;
    private final AtomicLong series = new AtomicLong();
    private final AtomicLong samples = new AtomicLong();
    private final List<String> warnings = new CopyOnWriteArrayList<>();
    private volatile boolean partial;

    public QueryContext(long maxSeries, long maxSamples, long timeoutMs) {
        this.maxSeries = maxSeries <= 0 ? Long.MAX_VALUE : maxSeries;
        this.maxSamples = maxSamples <= 0 ? Long.MAX_VALUE : maxSamples;
        this.timeoutMs = timeoutMs;
        this.deadlineNanos = timeoutMs <= 0 ? Long.MAX_VALUE : System.nanoTime() + timeoutMs * 1_000_000L;
    }

    public static QueryContext unlimited() { return new QueryContext(0, 0, 0); }

    public void addSeries(long n) {
        long total = series.addAndGet(n);
        if (total > maxSeries) {
            throw new LimitExceeded("series", "query selects " + total + " series, over the limit of " + maxSeries
                    + " (metricsdb.query.max-series); narrow the selector");
        }
    }

    public void addSamples(long n) {
        long total = samples.addAndGet(n);
        if (total > maxSamples) {
            throw new LimitExceeded("samples", "query would load more than " + maxSamples
                    + " samples (metricsdb.query.max-samples); shorten the range or narrow the selector");
        }
        checkDeadline();
    }

    public void checkDeadline() {
        if (deadlineNanos != Long.MAX_VALUE && System.nanoTime() > deadlineNanos) {
            throw new LimitExceeded("timeout", "query timed out after " + timeoutMs + " ms (metricsdb.query.timeout)");
        }
    }

    /** Marks the result incomplete (some data was unreachable) and records why. */
    public void markPartial(String warning) {
        partial = true;
        if (!warnings.contains(warning)) warnings.add(warning);
    }

    public boolean isPartial() { return partial; }
    public List<String> warnings() { return warnings; }

    public long seriesSelected() { return series.get(); }
    public long samplesLoaded() { return samples.get(); }
    public long maxSeries() { return maxSeries; }
    public long maxSamples() { return maxSamples; }
    public long timeoutMs() { return timeoutMs; }
}
