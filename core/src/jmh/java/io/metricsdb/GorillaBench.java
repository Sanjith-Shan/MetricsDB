package io.metricsdb;

import io.metricsdb.encoding.XorChunk;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/** Gorilla chunk encode and decode, per sample, on three value shapes. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class GorillaBench {
    static final int N = XorChunk.MAX_SAMPLES;

    @Param({"integer_gauge", "random_walk", "constant"})
    public String shape;

    long[] t = new long[N];
    double[] v = new double[N];
    byte[] encoded;

    @Setup
    public void setup() {
        Random r = new Random(1);
        long ts = 1_790_812_800_000L;
        double x = 50;
        for (int i = 0; i < N; i++) {
            ts += 10_000;
            t[i] = ts;
            x = switch (shape) {
                case "integer_gauge" -> Math.max(0, Math.min(100, Math.rint(x + r.nextInt(5) - 2))); // like the benchmark's cpu fields
                case "random_walk" -> x + r.nextGaussian();
                default -> 1;
            };
            v[i] = x;
        }
        encoded = XorChunk.encode(t, v, 0, N);
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public byte[] encode() {
        XorChunk.Appender a = new XorChunk.Appender();
        for (int i = 0; i < N; i++) a.append(t[i], v[i]);
        return a.toBytes();
    }

    @Benchmark
    @OperationsPerInvocation(N)
    public void decode(Blackhole bh) {
        XorChunk.Iterator it = new XorChunk.Iterator(encoded);
        while (it.next()) {
            bh.consume(it.t());
            bh.consume(it.vBits());
        }
    }
}
