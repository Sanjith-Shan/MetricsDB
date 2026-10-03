package io.metricsdb;

import io.metricsdb.index.Postings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Posting-list intersection: a selective list (one host's series) against a large one (every
 * series of one metric family), the shape of {@code cpu_usage_user{hostname="host_7"}}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PostingsBench {
    @Param({"100", "10000"})
    public int small;

    @Param({"1000000"})
    public int large;

    int[] a, b;

    @Setup
    public void setup() {
        Random r = new Random(2);
        TreeSet<Integer> s = new TreeSet<>();
        while (s.size() < small) s.add(r.nextInt(large * 4));
        a = s.stream().mapToInt(Integer::intValue).toArray();
        TreeSet<Integer> l = new TreeSet<>();
        while (l.size() < large) l.add(r.nextInt(large * 4));
        b = l.stream().mapToInt(Integer::intValue).toArray();
    }

    @Benchmark
    public int[] intersect() { return Postings.intersect(a, b); }
}
