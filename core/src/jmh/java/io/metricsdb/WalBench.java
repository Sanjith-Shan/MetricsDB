package io.metricsdb;

import io.metricsdb.wal.Records;
import io.metricsdb.wal.Wal;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * WAL append of a 1,000-sample record, waiting for durability. With fsync on, concurrent writers
 * share one fsync per group (group commit), so per-append cost falls as threads are added.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(java.util.concurrent.TimeUnit.MICROSECONDS)
public class WalBench {
    @Param({"ALWAYS", "NONE"})
    public String sync;

    Path dir;
    Wal wal;
    byte[] record;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dir = Files.createTempDirectory(Path.of(System.getProperty("wal.bench.dir", System.getProperty("java.io.tmpdir"))), "walbench");
        wal = new Wal(dir, Wal.Sync.valueOf(sync), 64L << 20);
        Records.SamplesBuilder b = new Records.SamplesBuilder(1000);
        for (int i = 0; i < 1000; i++) b.add(i, 1_790_812_800_000L + i * 10L, i * 0.5);
        record = b.build();
    }

    /** Drops sealed segments after every iteration so the benchmark does not fill the disk. */
    @TearDown(Level.Iteration)
    public void trim() throws IOException {
        wal.writeCheckpoint(wal.rotate(), java.util.List.of());
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        wal.close();
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path p : s.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
        }
    }

    @Benchmark
    @Threads(1)
    public void append1Thread() { wal.append(record).join(); }

    @Benchmark
    @Threads(4)
    public void append4Threads() { wal.append(record).join(); }
}
