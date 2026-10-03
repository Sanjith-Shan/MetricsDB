package io.metricsdb.index;

import io.metricsdb.model.Labels;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Inverted index from label pairs to series ids. Series ids are assigned in increasing order,
 * so appending an id keeps every posting list sorted without a sort. Reads take a shared lock;
 * adding a series takes the exclusive lock briefly.
 */
public final class MemIndex {
    private static final class IntList {
        int[] a = new int[4];
        int n;
        void add(int v) {
            if (n == a.length) a = Arrays.copyOf(a, n * 2);
            a[n++] = v;
        }
        int[] copy() { return Arrays.copyOf(a, n); }
    }

    private final Map<String, Map<String, IntList>> postings = new HashMap<>();
    private final IntList all = new IntList();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void add(int id, Labels labels) {
        lock.writeLock().lock();
        try {
            if (all.n > 0 && all.a[all.n - 1] >= id) throw new IllegalStateException("ids must increase");
            all.add(id);
            for (int i = 0; i < labels.size(); i++) {
                postings.computeIfAbsent(labels.name(i), k -> new HashMap<>())
                        .computeIfAbsent(labels.value(i), k -> new IntList())
                        .add(id);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int seriesCount() {
        lock.readLock().lock();
        try { return all.n; } finally { lock.readLock().unlock(); }
    }

    /**
     * Resolves a selector to series ids. Positive matchers that cannot match the empty string
     * are intersected smallest first; every other matcher is applied by subtracting the ids of
     * values it rejects, which also gives Prometheus semantics for a missing label ("").
     */
    public int[] select(List<Matcher> matchers) {
        lock.readLock().lock();
        try {
            List<int[]> positive = new ArrayList<>();
            List<Matcher> subtractive = new ArrayList<>();
            for (Matcher m : matchers) {
                if (m.isPositive() && !m.matchesEmpty()) positive.add(resolve(m));
                else subtractive.add(m);
            }
            int[] result;
            if (positive.isEmpty()) {
                result = all.copy();
            } else {
                positive.sort((x, y) -> Integer.compare(x.length, y.length));
                result = positive.get(0);
                for (int i = 1; i < positive.size() && result.length > 0; i++) {
                    result = Postings.intersect(result, positive.get(i));
                }
            }
            for (Matcher m : subtractive) {
                if (result.length == 0) break;
                Map<String, IntList> values = postings.get(m.name);
                if (values == null) {
                    if (!m.matchesEmpty()) result = Postings.EMPTY;
                    continue;
                }
                List<int[]> reject = new ArrayList<>();
                for (var e : values.entrySet()) if (!m.matches(e.getKey())) reject.add(e.getValue().copy());
                if (!m.matchesEmpty()) {
                    // series without the label have value "" and are rejected too
                    List<int[]> has = new ArrayList<>();
                    for (IntList l : values.values()) has.add(l.copy());
                    result = Postings.intersect(result, Postings.union(has));
                }
                if (!reject.isEmpty()) result = Postings.subtract(result, Postings.union(reject));
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    private int[] resolve(Matcher m) {
        Map<String, IntList> values = postings.get(m.name);
        if (values == null) return Postings.EMPTY;
        if (m.type == Matcher.Type.EQ) {
            IntList l = values.get(m.value);
            return l == null ? Postings.EMPTY : l.copy();
        }
        List<int[]> hits = new ArrayList<>();
        for (var e : values.entrySet()) if (m.matches(e.getKey())) hits.add(e.getValue().copy());
        return Postings.union(hits);
    }

    public List<String> labelNames() {
        lock.readLock().lock();
        try { return new ArrayList<>(new TreeSet<>(postings.keySet())); } finally { lock.readLock().unlock(); }
    }

    public List<String> labelValues(String name) {
        lock.readLock().lock();
        try {
            Map<String, IntList> v = postings.get(name);
            return v == null ? List.of() : new ArrayList<>(new TreeSet<>(v.keySet()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public int labelValueCount(String name) {
        lock.readLock().lock();
        try {
            Map<String, IntList> v = postings.get(name);
            return v == null ? 0 : v.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
