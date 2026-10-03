package io.metricsdb.storage;

import io.metricsdb.index.Matcher;
import io.metricsdb.model.Labels;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * kill -9 during writes: a child JVM appends from four threads and is SIGKILLed at a random
 * time (possibly mid-fsync, mid-cut or mid-compaction). After each kill the database is reopened
 * and every acknowledged sample must read back bit-exact. The same directory is reused across
 * kills, so recovery of an already-recovered store is exercised too.
 */
class CrashRecoveryTest {

    @Property(tries = 6)
    void acknowledgedSamplesSurviveKillNine(@ForAll @IntRange(min = 1, max = 1_000_000) int seed) throws Exception {
        Path dir = Files.createTempDirectory("crash");
        Random r = new Random(seed);
        Map<Integer, List<Integer>> acked = new HashMap<>();
        try {
            for (int gen = 0; gen < 3; gen++) {
                List<Integer> ids = runAndKill(dir, gen, 300 + r.nextInt(1500));
                acked.put(gen, ids);
                verify(dir, acked);
            }
        } finally {
            TsdbTest.deleteTree(dir);
        }
    }

    static List<Integer> runAndKill(Path dir, int gen, long killAfterMs) throws Exception {
        String cp = System.getProperty("test.classpath", System.getProperty("java.class.path"));
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder pb = new ProcessBuilder(java, "-Xmx256m", "-cp", cp, CrashWriter.class.getName(), dir.toString(), String.valueOf(gen));
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        try {
            return collect(p, killAfterMs);
        } finally {
            p.destroyForcibly();
        }
    }

    private static List<Integer> collect(Process p, long killAfterMs) throws Exception {
        List<Integer> ids = new ArrayList<>();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String first = in.readLine();
        assertEquals("READY", first);
        Thread reader = Thread.ofVirtual().start(() -> {
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] f = line.split(" ");
                    if (f.length == 3 && f[0].equals("ACK")) synchronized (ids) { ids.add(Integer.parseInt(f[2])); }
                }
            } catch (IOException ignored) {
            }
        });
        long waitUntil = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < waitUntil) {
            synchronized (ids) { if (!ids.isEmpty()) break; }
            Thread.sleep(10);
        }
        Thread.sleep(killAfterMs); // counted from the first acknowledgement, so a slow start cannot skip the test
        p.destroyForcibly(); // SIGKILL on Linux and macOS
        p.waitFor(10, TimeUnit.SECONDS);
        reader.join(5000);
        synchronized (ids) {
            assertTrue(ids.size() > 0, "child acknowledged nothing before the kill");
            return new ArrayList<>(ids);
        }
    }

    static void verify(Path dir, Map<Integer, List<Integer>> acked) {
        Tsdb.Options o = new Tsdb.Options(dir);
        o.backgroundMaintenance = false;
        try (Tsdb db = new Tsdb(o)) {
            Map<Labels, Map<Long, Long>> stored = new HashMap<>();
            for (SeriesChunks sc : db.select(List.of(Matcher.re("__name__", "crash_metric_.")), Long.MIN_VALUE, Long.MAX_VALUE, null)) {
                SampleArray sa = SampleArray.decode(sc.chunks(), Long.MIN_VALUE, Long.MAX_VALUE, null);
                Map<Long, Long> m = new HashMap<>();
                for (int i = 0; i < sa.n; i++) m.put(sa.t[i], Double.doubleToRawLongBits(sa.v[i]));
                stored.put(sc.labels(), m);
            }
            long checked = 0;
            for (var e : acked.entrySet()) {
                for (int id : e.getValue()) {
                    WriteBatch b = CrashWriter.batch(e.getKey(), id);
                    java.util.Set<String> seen = new java.util.HashSet<>();
                    for (int i = 0; i < b.n; i++) {
                        Map<Long, Long> s = stored.get(b.labels[i]);
                        assertTrue(s != null && s.containsKey(b.t[i]), "lost acknowledged sample gen " + e.getKey() + " batch " + id);
                        // within a batch the first write of a (series, timestamp) wins
                        if (seen.add(b.labels[i] + "@" + b.t[i])) {
                            assertEquals(Double.doubleToRawLongBits(b.v[i]), s.get(b.t[i]), "value changed for gen " + e.getKey() + " batch " + id);
                        }
                        checked++;
                    }
                }
            }
            assertTrue(checked > 0);
        }
    }
}
