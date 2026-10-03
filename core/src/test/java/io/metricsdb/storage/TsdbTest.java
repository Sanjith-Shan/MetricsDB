package io.metricsdb.storage;

import io.metricsdb.index.Matcher;
import io.metricsdb.model.Labels;
import io.metricsdb.promql.Engine;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TsdbTest {
    static final long H = 3_600_000L;
    static final long T0 = 1_759_276_800_000L; // 2025-10-01T00:00Z

    static Tsdb.Options opts(Path dir) {
        Tsdb.Options o = new Tsdb.Options(dir);
        o.backgroundMaintenance = false;
        return o;
    }

    /** Reference model: labels -> (t -> value bits). */
    static final class Model {
        final Map<Labels, TreeMap<Long, Long>> data = new HashMap<>();
        void put(Labels l, long t, double v) {
            data.computeIfAbsent(l, k -> new TreeMap<>()).putIfAbsent(t, Double.doubleToRawLongBits(v));
        }
    }

    static void assertMatches(Model m, Tsdb db) {
        List<SeriesChunks> all = db.select(List.of(Matcher.re("__name__", ".+")), Long.MIN_VALUE, Long.MAX_VALUE, null);
        Map<Labels, TreeMap<Long, Long>> got = new HashMap<>();
        for (SeriesChunks sc : all) {
            SampleArray sa = SampleArray.decode(sc.chunks(), Long.MIN_VALUE, Long.MAX_VALUE, null);
            TreeMap<Long, Long> s = new TreeMap<>();
            for (int i = 0; i < sa.n; i++) s.put(sa.t[i], Double.doubleToRawLongBits(sa.v[i]));
            got.put(sc.labels(), s);
        }
        assertEquals(m.data.keySet(), got.keySet(), "series sets differ");
        for (var e : m.data.entrySet()) assertEquals(e.getValue(), got.get(e.getKey()), "samples differ for " + e.getKey());
    }

    static Labels series(int i) {
        return Labels.of("__name__", "cpu_usage_" + (i % 3), "hostname", "host_" + i, "region", "r" + (i % 2));
    }

    @Property(tries = 15)
    void everyAcceptedSampleReadsBackThroughCutsCompactionAndRestart(@ForAll @LongRange(min = 0, max = 1_000_000) long seed) throws IOException {
        Path dir = Files.createTempDirectory("tsdb-prop");
        try {
            Random r = new Random(seed);
            Model m = new Model();
            Tsdb.Options o = opts(dir);
            o.oooWindowMs = 10 * 60_000;
            Tsdb db = new Tsdb(o);
            int nSeries = 1 + r.nextInt(20);
            long[] next = new long[nSeries];
            for (int i = 0; i < nSeries; i++) next[i] = T0 + r.nextInt(60_000);
            int rounds = 50 + r.nextInt(200);
            for (int round = 0; round < rounds; round++) {
                WriteBatch b = new WriteBatch();
                for (int k = 0; k < 1 + r.nextInt(40); k++) {
                    int s = r.nextInt(nSeries);
                    long t;
                    if (r.nextInt(10) == 0) t = next[s] - r.nextInt(15 * 60_000); // late, maybe outside the window
                    else { next[s] += 1_000 + r.nextInt(120_000); t = next[s]; }
                    double v = r.nextInt(4) == 0 ? Math.rint(r.nextGaussian() * 10) : r.nextGaussian();
                    b.add(series(s), t, v);
                }
                // replicate the acceptance rules in the model: apply in order
                Map<Labels, Long> maxT = new HashMap<>();
                for (var e : m.data.entrySet()) maxT.put(e.getKey(), e.getValue().lastKey());
                Tsdb.AppendResult res = db.append(b);
                int accepted = 0;
                long mvt = db.stats().minValidT();
                for (int i = 0; i < b.n; i++) {
                    Labels l = b.labels[i];
                    long mx = maxT.getOrDefault(l, Long.MIN_VALUE);
                    long t = b.t[i];
                    boolean ok;
                    if (t < mvt) ok = false;
                    else if (t > mx) { ok = true; maxT.put(l, t); }
                    else ok = t >= mx - o.oooWindowMs && !m.data.getOrDefault(l, new TreeMap<>()).containsKey(t);
                    if (ok && !(m.data.containsKey(l) && m.data.get(l).containsKey(t))) { m.put(l, t, b.v[i]); accepted++; }
                }
                assertEquals(accepted, res.accepted(), "accepted count, round " + round);
                if (r.nextInt(25) == 0) db.maintain();
                if (r.nextInt(40) == 0) {
                    db.close();
                    db = new Tsdb(o);
                    assertMatches(m, db);
                }
            }
            db.maintain();
            assertMatches(m, db);
            db.flushHead();
            assertMatches(m, db);
            db.close();
            db = new Tsdb(o);
            assertMatches(m, db);
            db.close();
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void cutsBlocksCompactsAndKeepsTheIndexRight(@TempDir Path dir) {
        Tsdb db = new Tsdb(opts(dir));
        Model m = new Model();
        // 30 hours of 10-second samples for 6 series: 15 two-hour ranges, compacted to 6h and 24h
        for (long t = T0; t < T0 + 30 * H; t += 10_000) {
            WriteBatch b = new WriteBatch();
            for (int s = 0; s < 6; s++) {
                double v = (t / 10_000 % 100) + s;
                b.add(series(s), t, v);
                m.put(series(s), t, v);
            }
            db.append(b);
            if ((t - T0) % H == 0) db.maintain();
        }
        db.maintain();
        assertTrue(db.blocks().stream().anyMatch(b -> b.meta.level() == 2), "expected a 24h block: " + db.blocks().stream().map(b -> b.dir.getFileName().toString()).toList());
        assertMatches(m, db);
        List<SeriesChunks> host0 = db.select(List.of(Matcher.eq("hostname", "host_0")), T0, T0 + 30 * H, null);
        assertEquals(1, host0.size());
        db.close();
        Tsdb again = new Tsdb(opts(dir));
        assertMatches(m, again);
        again.close();
    }

    @Test
    void outOfOrderWindowAcceptsLateSamplesAndRefusesOlderOnes(@TempDir Path dir) {
        Tsdb.Options o = opts(dir);
        o.oooWindowMs = 60_000;
        Tsdb db = new Tsdb(o);
        Labels l = series(0);
        WriteBatch b = new WriteBatch();
        b.add(l, T0 + 100_000, 1);
        b.add(l, T0 + 50_000, 2);   // 50 s late: inside the window
        b.add(l, T0 + 30_000, 3);   // 70 s late: outside
        b.add(l, T0 + 100_000, 4);  // duplicate timestamp
        Tsdb.AppendResult r = db.append(b);
        assertEquals(2, r.accepted());
        assertEquals(1, r.outOfOrder());
        assertEquals(1, r.tooOld());
        assertEquals(1, r.duplicates());
        db.close();
    }

    @Test
    void cardinalityLimitRefusesNewSeriesButNotExistingOnes(@TempDir Path dir) {
        Tsdb.Options o = opts(dir);
        o.maxSeries = 3;
        Tsdb db = new Tsdb(o);
        WriteBatch b = new WriteBatch();
        for (int i = 0; i < 5; i++) b.add(series(i), T0, i);
        Tsdb.AppendResult r = db.append(b);
        assertEquals(3, r.accepted());
        assertEquals(2, r.overCardinality());
        WriteBatch again = new WriteBatch();
        again.add(series(0), T0 + 1000, 1);
        assertEquals(1, db.append(again).accepted());
        db.close();
    }

    @Test
    void rollupsAnswerLikeRawData(@TempDir Path dir) {
        Tsdb db = new Tsdb(opts(dir));
        Random r = new Random(3);
        for (long t = T0; t < T0 + 26 * H; t += 10_000) {
            WriteBatch b = new WriteBatch();
            for (int s = 0; s < 4; s++) if (r.nextInt(20) != 0) b.add(series(s), t + r.nextInt(3000), r.nextGaussian() * 50);
            db.append(b);
            if ((t - T0) % H == 0) db.maintain();
        }
        db.maintain();
        Engine e = new Engine(db, new Engine.Options());
        for (String f : List.of("max_over_time", "min_over_time", "sum_over_time", "count_over_time", "avg_over_time", "last_over_time")) {
            String q = "sum by (hostname) (" + f + "({__name__=~\"cpu_usage_.\"}[1h]))";
            Engine.Result raw = e.rangeQuery(q, T0 + 2 * H, T0 + 26 * H, H, QueryContext.unlimited(), Engine.RollupMode.OFF);
            Engine.Result rolled = e.rangeQuery(q, T0 + 2 * H, T0 + 26 * H, H, QueryContext.unlimited(), Engine.RollupMode.FORCE);
            assertTrue(rolled.usedRollup, f);
            assertEquals(raw.series.size(), rolled.series.size(), f);
            for (int i = 0; i < raw.series.size(); i++) {
                var a = raw.series.get(i);
                var b = rolled.series.get(i);
                assertEquals(a.labels, b.labels);
                for (int k = 0; k < raw.steps; k++) {
                    assertEquals(a.has[k], b.has[k], f + " step " + k);
                    if (a.has[k]) assertEquals(a.v[k], b.v[k], Math.abs(a.v[k]) * 1e-12, f + " step " + k);
                }
            }
        }
        db.close();
    }

    static void deleteTree(Path p) throws IOException {
        if (!Files.exists(p)) return;
        try (var s = Files.walk(p)) {
            for (Path x : s.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(x);
        }
    }
}
