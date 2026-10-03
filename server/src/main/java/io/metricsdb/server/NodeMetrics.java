package io.metricsdb.server;

import io.metricsdb.storage.Tsdb;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** MetricsDB's own metrics, exposed in Prometheus format on {@code /metrics} and optionally stored in itself. */
@Component
public class NodeMetrics {
    private final MeterRegistry reg;
    private final ConcurrentHashMap<String, Timer> timers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
    private volatile Tsdb.Stats cachedStats;
    private volatile long statsAt;

    public NodeMetrics(MeterRegistry reg) {
        this.reg = reg;
    }

    public MeterRegistry registry() { return reg; }

    public Timer timer(String name, String... tags) {
        String key = name + String.join(",", tags);
        return timers.computeIfAbsent(key, k -> Timer.builder(name).tags(tags)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofNanos(50_000))
                .maximumExpectedValue(Duration.ofSeconds(30))
                .register(reg));
    }

    public Counter counter(String name, String... tags) {
        String key = name + String.join(",", tags);
        return counters.computeIfAbsent(key, k -> Counter.builder(name).tags(tags).register(reg));
    }

    public void gauge(String name, Supplier<Number> f, String... tags) {
        Gauge.builder(name, f).tags(tags).register(reg);
    }

    public void recordNanos(String name, long nanos, String... tags) {
        timer(name, tags).record(nanos, TimeUnit.NANOSECONDS);
    }

    /** Wires the storage engine's counters and hooks. */
    public void bind(Tsdb db) {
        db.hooks.walFsyncNanos = n -> recordNanos("metricsdb.wal.fsync", n);
        db.hooks.cutNanos = n -> recordNanos("metricsdb.head.cut", n);
        db.hooks.compactionNanos = n -> recordNanos("metricsdb.compaction", n);
        Supplier<Tsdb.Stats> stats = () -> {
            long now = System.currentTimeMillis();
            if (cachedStats == null || now - statsAt > 5_000) {
                cachedStats = db.stats();
                statsAt = now;
            }
            return cachedStats;
        };
        gauge("metricsdb.head.series", () -> db.seriesCount());
        gauge("metricsdb.head.samples", () -> stats.get().headSamples());
        gauge("metricsdb.head.chunk.bytes", () -> stats.get().headChunkBytes());
        gauge("metricsdb.blocks", () -> stats.get().blocks());
        gauge("metricsdb.block.bytes", () -> stats.get().blockBytesOnDisk());
        gauge("metricsdb.block.samples", () -> stats.get().blockSamples());
        gauge("metricsdb.wal.bytes", () -> stats.get().walBytesOnDisk());
        gauge("metricsdb.series.limit", () -> db.options().maxSeries);
        FunctionCounter.builder("metricsdb.ingest.samples", db.samplesAppended, x -> (double) x.get()).tag("result", "accepted").register(reg);
        FunctionCounter.builder("metricsdb.ingest.samples", db.samplesOutOfOrder, x -> (double) x.get()).tag("result", "out_of_order").register(reg);
        FunctionCounter.builder("metricsdb.ingest.samples", db.samplesDuplicate, x -> (double) x.get()).tag("result", "duplicate").register(reg);
        FunctionCounter.builder("metricsdb.ingest.samples", db.samplesTooOld, x -> (double) x.get()).tag("result", "too_old").register(reg);
        FunctionCounter.builder("metricsdb.ingest.samples", db.samplesOverCardinality, x -> (double) x.get()).tag("result", "over_cardinality").register(reg);
        FunctionCounter.builder("metricsdb.blocks.cut", db.blocksCut, x -> (double) x.get()).register(reg);
        FunctionCounter.builder("metricsdb.compactions", db.compactions, x -> (double) x.get()).register(reg);
    }
}
