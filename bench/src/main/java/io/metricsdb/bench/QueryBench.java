package io.metricsdb.bench;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.HdrHistogram.Histogram;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs the benchmark's queries against one database and records latency per query type. Each
 * type first runs its first 20 queries as a warm-up (JIT, page cache), then the remaining queries
 * are measured; they differ from the warm-up ones, so a result cache cannot answer them.
 */
public final class QueryBench {
    public static void run(Args a) throws Exception {
        String base = a.req("url");
        List<Queries.Q> qs = Queries.load(a.req("queries"), a.str("types", null));
        int workers = a.i("workers", 1);
        int warmup = a.i("warmup", 20);
        String suffix = a.str("suffix", "");
        Map<String, List<Queries.Q>> byType = new LinkedHashMap<>();
        for (Queries.Q q : qs) byType.computeIfAbsent(q.type(), k -> new java.util.ArrayList<>()).add(q);
        for (var e : byType.entrySet()) {
            // warm up on the first queries of the type, measure the rest: different hosts and windows,
            // so no database can answer the measured queries from a result cache
            List<Queries.Q> all = e.getValue();
            int w0 = Math.min(warmup, all.size() / 2);
            for (Queries.Q q : all.subList(0, w0)) Queries.get(base, q.path() + suffix, Duration.ofSeconds(120));
            List<Queries.Q> list = all.subList(w0, all.size());
            Histogram h = new Histogram(600_000_000L, 3);
            AtomicInteger next = new AtomicInteger(), errors = new AtomicInteger();
            String[] firstError = new String[1];
            long t0 = System.nanoTime();
            Thread[] ts = new Thread[workers];
            for (int w = 0; w < workers; w++) {
                ts[w] = Thread.ofPlatform().start(() -> {
                    int i;
                    while ((i = next.getAndIncrement()) < list.size()) {
                        Queries.Resp r = Queries.get(base, list.get(i).path() + suffix, Duration.ofSeconds(120));
                        if (r.status() / 100 != 2) {
                            errors.incrementAndGet();
                            synchronized (firstError) { if (firstError[0] == null) firstError[0] = r.status() + " " + r.body(); }
                            continue;
                        }
                        synchronized (h) { h.recordValue(r.micros()); }
                    }
                });
            }
            for (Thread t : ts) t.join();
            double secs = (System.nanoTime() - t0) / 1e9;
            ObjectNode row = Env.JSON.createObjectNode();
            row.put("exp", a.str("exp", "exp3"));
            row.put("db", a.req("db"));
            row.put("label", a.str("label", a.req("db")));
            row.put("ts", Env.now());
            row.put("query_type", e.getKey());
            row.put("queries", list.size());
            row.put("workers", workers);
            row.put("errors", errors.get());
            if (firstError[0] != null) row.put("first_error", firstError[0].length() > 300 ? firstError[0].substring(0, 300) : firstError[0]);
            row.put("queries_per_s", Math.round(list.size() / secs * 10) / 10.0);
            row.set("latency_ms", Env.hist(h, 1000.0));
            row.set("machine", Env.machine());
            row.set("load_after", Env.load());
            Env.append(a.str("out", null), row);
        }
    }
}
