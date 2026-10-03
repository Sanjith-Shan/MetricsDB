package io.metricsdb.index;

import io.metricsdb.model.Labels;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class PostingsTest {

    private static int[] sorted(Set<Integer> s) {
        return new TreeSet<>(s).stream().mapToInt(Integer::intValue).toArray();
    }

    @Property(tries = 500)
    void setOperationsMatchJavaSets(@ForAll @Size(max = 300) Set<@IntRange(min = 0, max = 2000) Integer> a,
                                    @ForAll @Size(max = 3000) Set<@IntRange(min = 0, max = 4000) Integer> b) {
        int[] x = sorted(a), y = sorted(b);
        Set<Integer> inter = new TreeSet<>(a);
        inter.retainAll(b);
        Set<Integer> uni = new TreeSet<>(a);
        uni.addAll(b);
        Set<Integer> diff = new TreeSet<>(a);
        diff.removeAll(b);
        assertArrayEquals(sorted(inter), Postings.intersect(x, y));
        assertArrayEquals(sorted(inter), Postings.intersect(y, x));
        assertArrayEquals(sorted(uni), Postings.union(List.of(x, y)));
        assertArrayEquals(sorted(uni), Postings.union(List.of(x, y, x)));
        assertArrayEquals(sorted(diff), Postings.subtract(x, y));
    }

    @Test
    void selectorsFollowPrometheusSemantics() {
        MemIndex idx = new MemIndex();
        List<Labels> all = new ArrayList<>();
        all.add(Labels.of("__name__", "cpu_usage_user", "hostname", "host_0", "region", "us-east-1"));
        all.add(Labels.of("__name__", "cpu_usage_user", "hostname", "host_1", "region", "eu-west-1"));
        all.add(Labels.of("__name__", "cpu_usage_system", "hostname", "host_0", "region", "us-east-1"));
        all.add(Labels.of("__name__", "mem_used", "hostname", "host_2"));
        for (int i = 0; i < all.size(); i++) idx.add(i, all.get(i));

        assertArrayEquals(new int[]{0, 1}, idx.select(List.of(Matcher.eq("__name__", "cpu_usage_user"))));
        assertArrayEquals(new int[]{0, 2}, idx.select(List.of(Matcher.re("__name__", "cpu_(usage_user|usage_system)"),
                Matcher.eq("hostname", "host_0"))));
        assertArrayEquals(new int[]{1}, idx.select(List.of(Matcher.re("hostname", "host_1|host_9"))));
        // a missing label counts as "", so region!="us-east-1" also selects mem_used
        assertArrayEquals(new int[]{1, 3}, idx.select(List.of(new Matcher(Matcher.Type.NEQ, "region", "us-east-1"))));
        // region="" selects only series without the label
        assertArrayEquals(new int[]{3}, idx.select(List.of(Matcher.eq("__name__", "mem_used"), Matcher.eq("region", ""))));
        assertArrayEquals(new int[]{0, 1, 2}, idx.select(List.of(Matcher.re("region", ".+"))));
        assertArrayEquals(new int[]{0, 2, 3}, idx.select(List.of(new Matcher(Matcher.Type.NRE, "hostname", "host_1"))));
    }
}
