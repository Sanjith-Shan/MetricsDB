package io.metricsdb.promql;

import io.metricsdb.model.Labels;
import io.metricsdb.storage.QueryContext;
import io.metricsdb.storage.Tsdb;
import io.metricsdb.storage.WriteBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EngineTest {
    static final long T0 = 1_790_812_800_000L;
    @TempDir Path dir;
    Tsdb db;
    Engine e;

    @BeforeEach
    void setUp() {
        Tsdb.Options o = new Tsdb.Options(dir);
        o.backgroundMaintenance = false;
        db = new Tsdb(o);
        // cpu_usage_user for 3 hosts: value = host*100 + minute index, one sample every 10 s for 2 hours
        WriteBatch b = new WriteBatch();
        for (long t = T0; t < T0 + 7_200_000; t += 10_000) {
            for (int h = 0; h < 3; h++) {
                double v = h * 100 + (t - T0) / 60_000;
                b.add(Labels.of("__name__", "cpu_usage_user", "hostname", "host_" + h, "region", h < 2 ? "us" : "eu"), t, v);
                b.add(Labels.of("__name__", "cpu_usage_system", "hostname", "host_" + h, "region", h < 2 ? "us" : "eu"), t, 1);
                // a counter that increases by 10 per sample
                b.add(Labels.of("__name__", "requests_total", "hostname", "host_" + h), t, (t - T0) / 1000.0);
            }
        }
        db.append(b);
        e = new Engine(db, new Engine.Options());
    }

    @AfterEach
    void tearDown() { db.close(); }

    Engine.Result range(String q, long start, long end, long step) {
        return e.rangeQuery(q, start, end, step, QueryContext.unlimited(), Engine.RollupMode.OFF);
    }

    @Test
    void maxOverTimeGroupedByNameKeepsTheMetricName() {
        Engine.Result r = range("max(max_over_time({__name__=~'cpu_(usage_user|usage_system)', hostname=~'host_0|host_1'}[1m])) by (__name__)",
                T0 + 600_000, T0 + 1_200_000, 60_000);
        assertEquals(2, r.series.size());
        Engine.StepSeries user = r.series.get(1);
        assertEquals("cpu_usage_user", user.labels.metricName());
        // window (t-60s, t] at t = T0+10m holds minutes 9 (t-50s .. t-10s) and 10 (t): max is host_1's 110
        assertEquals(110.0, user.v[0]);
    }

    @Test
    void windowsAreLeftOpen() {
        // at t = T0 + 60s the window (T0, T0+60s] excludes the sample at T0
        Engine.Result r = range("count_over_time(cpu_usage_user{hostname='host_0'}[1m])", T0 + 60_000, T0 + 60_000, 60_000);
        assertEquals(6.0, r.series.get(0).v[0]);
    }

    @Test
    void avgByTwoLabels() {
        Engine.Result r = range("avg(avg_over_time(cpu_usage_user[1h])) by (__name__, region)", T0 + 3_600_000, T0 + 3_600_000, 3_600_000);
        assertEquals(2, r.series.size());
        assertEquals(Labels.of("__name__", "cpu_usage_user", "region", "eu"), r.series.get(0).labels);
    }

    @Test
    void rateFollowsPrometheusExtrapolation() {
        Engine.Result r = range("rate(requests_total{hostname='host_0'}[5m])", T0 + 1_800_000, T0 + 1_800_000, 60_000);
        assertEquals(1.0, r.series.get(0).v[0], 1e-9);
        assertFalse(r.series.get(0).labels.metricName().equals("requests_total"), "rate drops the metric name");
    }

    @Test
    void arithmeticComparisonAndTopk() {
        Engine.Result r = range("cpu_usage_user > 150", T0 + 600_000, T0 + 600_000, 60_000);
        assertEquals(1, r.series.size());
        assertEquals("cpu_usage_user", r.series.get(0).labels.metricName(), "a filter keeps the name");
        r = range("cpu_usage_user * 2 + 1", T0 + 600_000, T0 + 600_000, 60_000);
        assertEquals(21.0, r.series.get(0).v[0]);
        r = range("topk(1, cpu_usage_user)", T0 + 600_000, T0 + 600_000, 60_000);
        assertEquals(1, r.series.size());
        assertEquals("host_2", r.series.get(0).labels.get("hostname"));
        r = range("sum without (hostname) (cpu_usage_system)", T0 + 600_000, T0 + 600_000, 60_000);
        assertEquals(2, r.series.size());
    }

    @Test
    void stepsAlignLikeVictoriaMetricsOnlyForLongRanges() {
        Engine.Result r = range("cpu_usage_user{hostname='host_0'}", T0 + 1_234, T0 + 1_234 + 3_600_000, 60_000);
        assertEquals(T0, r.start);
        Engine.Result s = range("cpu_usage_user{hostname='host_0'}", T0 + 1_234, T0 + 1_234 + 600_000, 60_000);
        assertEquals(T0 + 1_234, s.start);
    }

    @Test
    void queryLimitsFailWithClearMessages() {
        QueryContext series = new QueryContext(2, 0, 0);
        QueryContext.LimitExceeded ex = assertThrows(QueryContext.LimitExceeded.class,
                () -> e.rangeQuery("cpu_usage_user", T0, T0 + 600_000, 60_000, series, Engine.RollupMode.OFF));
        assertTrue(ex.getMessage().contains("over the limit of 2"), ex.getMessage());
        QueryContext samples = new QueryContext(0, 1000, 0);
        ex = assertThrows(QueryContext.LimitExceeded.class,
                () -> e.rangeQuery("max_over_time(cpu_usage_user[1h])", T0, T0 + 7_200_000, 60_000, samples, Engine.RollupMode.OFF));
        assertEquals("samples", ex.kind);
        QueryContext timeout = new QueryContext(0, 0, 1);
        ex = assertThrows(QueryContext.LimitExceeded.class, () -> {
            for (int i = 0; i < 1000; i++) e.rangeQuery("sum(rate(requests_total[5m]))", T0, T0 + 7_200_000, 10_000, timeout, Engine.RollupMode.OFF);
        });
        assertEquals("timeout", ex.kind);
    }

    @Test
    void parseErrorsAreReported() {
        assertThrows(ParseException.class, () -> Parser.parse("sum(rate(x[5m])"));
        assertThrows(ParseException.class, () -> Parser.parse("nosuchfunc(x)"));
        assertThrows(ParseException.class, () -> Parser.parse("{a=\"\"}"));
        assertThrows(ParseException.class, () -> Parser.parse("rate(x)"));
    }
}
