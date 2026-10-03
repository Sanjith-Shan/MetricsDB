package io.metricsdb.storage;

import io.metricsdb.model.Labels;

import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Child process for the crash test: writes deterministic batches from several threads and prints
 * {@code ACK <generation> <batch>} after each append returns. The parent kills it with SIGKILL at
 * a random moment and then checks that every acknowledged batch survived.
 */
public final class CrashWriter {
    public static final int SERIES = 40;

    /** Batch contents are a pure function of (generation, batch), so the parent can regenerate them. */
    public static WriteBatch batch(int generation, int batch) {
        Random r = new Random(generation * 1_000_003L + batch);
        WriteBatch b = new WriteBatch(64);
        long base = TsdbTest.T0 + (long) generation * 50_000_000L + (long) batch * 1_000L;
        for (int k = 0; k < 50; k++) {
            int s = r.nextInt(SERIES);
            Labels l = Labels.of("__name__", "crash_metric_" + (s % 4), "instance", "i" + s);
            b.add(l, base + r.nextInt(1_000), r.nextInt(3) == 0 ? Math.rint(r.nextGaussian() * 100) : r.nextGaussian());
        }
        return b;
    }

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        int generation = Integer.parseInt(args[1]);
        Tsdb.Options o = new Tsdb.Options(dir);
        o.blockRangeMs = 2 * 3_600_000L;
        o.maintenanceIntervalMs = 50; // cut and compact often so kills land inside them too
        Tsdb db = new Tsdb(o);
        System.out.println("READY");
        System.out.flush();
        AtomicInteger next = new AtomicInteger();
        Thread[] ts = new Thread[4];
        for (int w = 0; w < ts.length; w++) {
            ts[w] = new Thread(() -> {
                while (true) {
                    int id = next.getAndIncrement();
                    db.append(batch(generation, id));
                    synchronized (System.out) {
                        System.out.println("ACK " + generation + " " + id);
                        System.out.flush();
                    }
                }
            });
            ts[w].start();
        }
        for (Thread t : ts) t.join();
    }
}
