package io.metricsdb.storage;

import io.metricsdb.block.Block;
import io.metricsdb.block.BlockWriter;
import io.metricsdb.block.Rollup;
import io.metricsdb.encoding.XorChunk;
import io.metricsdb.head.MemSeries;
import io.metricsdb.index.Matcher;
import io.metricsdb.index.MemIndex;
import io.metricsdb.model.Labels;
import io.metricsdb.wal.Records;
import io.metricsdb.wal.Wal;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

/**
 * One storage node's database: a write-ahead log, an in-memory head of recent data, immutable
 * on-disk blocks of older data, and a label index over every series.
 *
 * <p>Write path: resolve each sample's series (creating it, subject to the cardinality limit),
 * append to the head, enqueue one WAL record for the batch and wait for its group fsync. Only
 * then is the batch acknowledged.
 *
 * <p>Once the head spans 1.5 block ranges, its oldest full range is cut into a block (with a
 * rollup), dropped from memory, and the WAL is compacted into a checkpoint. Small blocks are
 * merged into larger ones; blocks past retention are deleted.
 */
public final class Tsdb implements Queryable, Closeable {

    public static final class Options {
        public Path dir;
        public long blockRangeMs = TimeUnit.HOURS.toMillis(2);
        public long oooWindowMs = TimeUnit.MINUTES.toMillis(10);
        public int maxSeries = 2_000_000;
        public Wal.Sync walSync = Wal.Sync.ALWAYS;
        public long walSegmentBytes = 64L << 20;
        public long retentionRawMs = 0;      // 0 keeps raw data forever
        public long retentionRollupMs = 0;   // 0 keeps rollups as long as raw (or forever)
        public long rollupResMs = TimeUnit.MINUTES.toMillis(5);
        public int[] compactionFactors = {3, 4}; // 2h -> 6h -> 24h
        public boolean backgroundMaintenance = true;
        public long maintenanceIntervalMs = 5_000;

        public Options(Path dir) { this.dir = dir; }
    }

    public record AppendResult(int accepted, int outOfOrder, int duplicates, int tooOld, int overCardinality) {
        public int rejected() { return tooOld + overCardinality; }
    }

    /** Lightweight hooks for self-monitoring, set by the server. */
    public static final class Hooks {
        public volatile LongConsumer walFsyncNanos = n -> {};
        public volatile LongConsumer cutNanos = n -> {};
        public volatile LongConsumer compactionNanos = n -> {};
    }

    private final Options opt;
    private final Path blocksDir;
    private final ConcurrentHashMap<Labels, MemSeries> byLabels = new ConcurrentHashMap<>();
    private volatile MemSeries[] byId = new MemSeries[1024];
    private int nextId;
    private final Object createLock = new Object();
    private final MemIndex index = new MemIndex();
    private volatile List<Block> blocks = List.of();
    private volatile long minValidT = Long.MIN_VALUE;
    private final AtomicLong headMaxT = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong headMinSeen = new AtomicLong(Long.MAX_VALUE);
    private final ReentrantReadWriteLock appendLock = new ReentrantReadWriteLock();
    private final Object maintLock = new Object();
    private final Wal wal;
    private final ScheduledExecutorService maint;
    private final List<Path> pendingDeletes = new ArrayList<>();
    public final Hooks hooks = new Hooks();

    // counters
    public final AtomicLong samplesAppended = new AtomicLong();
    public final AtomicLong samplesOutOfOrder = new AtomicLong();
    public final AtomicLong samplesDuplicate = new AtomicLong();
    public final AtomicLong samplesTooOld = new AtomicLong();
    public final AtomicLong samplesOverCardinality = new AtomicLong();
    public final AtomicLong blocksCut = new AtomicLong();
    public final AtomicLong compactions = new AtomicLong();
    private Wal.ReplayStats replayStats;
    private long replayMillis;

    public Tsdb(Options opt) {
        this.opt = opt;
        if (opt.oooWindowMs > opt.blockRangeMs / 2) throw new IllegalArgumentException("out-of-order window must be at most half a block range");
        this.blocksDir = opt.dir.resolve("blocks");
        try {
            Files.createDirectories(blocksDir);
            loadBlocks();
            long t0 = System.nanoTime();
            Map<Integer, MemSeries> walIds = new HashMap<>();
            replayStats = Wal.replay(opt.dir.resolve("wal"), payload -> Records.decode(payload, new Records.Visitor() {
                @Override public void series(int id, Labels labels) {
                    walIds.put(id, getOrCreate(labels, false, false));
                }
                @Override public void sample(int id, long t, double v) {
                    MemSeries s = walIds.get(id);
                    if (s == null) return;
                    // replay accepts any order: WAL order across concurrent writers need not be time order
                    if (s.append(t, v, minValidT, -1, opt.blockRangeMs) != MemSeries.Result.TOO_OLD) noteTime(t);
                }
            }));
            replayMillis = (System.nanoTime() - t0) / 1_000_000;
            wal = new Wal(opt.dir.resolve("wal"), opt.walSync, opt.walSegmentBytes);
            wal.onFsync(n -> hooks.walFsyncNanos.accept(n));
            // rewrite the replayed head under this process's series ids; older segments go away
            wal.writeCheckpoint(wal.currentSegment() - 1, checkpointRecords());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (opt.backgroundMaintenance) {
            maint = Executors.newSingleThreadScheduledExecutor(r -> Thread.ofPlatform().name("tsdb-maintenance").daemon(true).unstarted(r));
            maint.scheduleWithFixedDelay(() -> {
                try { maintain(); } catch (Throwable t) { System.err.println("maintenance failed: " + t); }
            }, opt.maintenanceIntervalMs, opt.maintenanceIntervalMs, TimeUnit.MILLISECONDS);
        } else {
            maint = null;
        }
    }

    private void noteTime(long t) {
        headMaxT.accumulateAndGet(t, Math::max);
        headMinSeen.accumulateAndGet(t, Math::min);
    }

    private void loadBlocks() throws IOException {
        List<Block> loaded = new ArrayList<>();
        try (Stream<Path> s = Files.list(blocksDir)) {
            for (Path p : s.sorted().toList()) {
                String name = p.getFileName().toString();
                if (name.endsWith(".tmp")) { Block.deleteDir(p); continue; }
                if (!name.startsWith("b-")) continue;
                loaded.add(Block.open(p));
            }
        }
        // a crash during compaction can leave a merged block next to its sources: keep the widest
        loaded.sort(Comparator.comparingLong((Block b) -> b.meta.rangeStart()).thenComparingInt(b -> -b.meta.level()));
        List<Block> kept = new ArrayList<>();
        long coveredTo = Long.MIN_VALUE;
        for (Block b : loaded) {
            if (b.meta.rangeEnd() <= coveredTo) { Block.deleteDir(b.dir); continue; }
            kept.add(b);
            coveredTo = Math.max(coveredTo, b.meta.rangeEnd());
        }
        for (Block b : kept) {
            int[] g = new int[b.seriesCount()];
            for (int i = 0; i < g.length; i++) g[i] = getOrCreate(b.labels(i), false, false).id;
            b.bindGlobalIds(g);
            minValidT = Math.max(minValidT, b.meta.rangeEnd());
        }
        blocks = List.copyOf(kept);
    }

    /** Returns the series, creating it if needed; null when the cardinality limit refuses it. */
    private MemSeries getOrCreate(Labels l, boolean enforceLimit, boolean log) {
        MemSeries s = byLabels.get(l);
        if (s != null) return s;
        synchronized (createLock) {
            s = byLabels.get(l);
            if (s != null) return s;
            if (enforceLimit && nextId >= opt.maxSeries) return null;
            int id = nextId++;
            s = new MemSeries(id, l);
            MemSeries[] arr = byId;
            if (id >= arr.length) {
                arr = Arrays.copyOf(arr, arr.length * 2);
            }
            arr[id] = s;
            byId = arr;
            index.add(id, l);
            // the series record is queued before anyone can reference the id
            if (log && wal != null) wal.append(Records.series(id, l));
            byLabels.put(l, s);
            return s;
        }
    }

    /** Appends a batch and returns once it is durable in the WAL (with {@code Sync.ALWAYS}). */
    public AppendResult append(WriteBatch b) {
        int ok = 0, ooo = 0, dup = 0, old = 0, card = 0;
        CompletableFuture<Void> durable;
        appendLock.readLock().lock();
        try {
            Records.SamplesBuilder rec = new Records.SamplesBuilder(b.n);
            long mvt = minValidT;
            long lo = Long.MAX_VALUE, hi = Long.MIN_VALUE;
            for (int i = 0; i < b.n; i++) {
                MemSeries s = getOrCreate(b.labels[i], true, true);
                if (s == null) { card++; continue; }
                long t = b.t[i];
                switch (s.append(t, b.v[i], mvt, opt.oooWindowMs, opt.blockRangeMs)) {
                    case OK -> { ok++; rec.add(s.id, t, b.v[i]); }
                    case OOO -> { ooo++; rec.add(s.id, t, b.v[i]); }
                    case DUPLICATE -> { dup++; continue; }
                    case TOO_OLD, OOO_FULL -> { old++; continue; }
                }
                if (t < lo) lo = t;
                if (t > hi) hi = t;
            }
            if (hi != Long.MIN_VALUE) { noteTime(lo); noteTime(hi); }
            durable = rec.count() > 0 ? wal.append(rec.build()) : CompletableFuture.completedFuture(null);
        } finally {
            appendLock.readLock().unlock();
        }
        durable.join();
        samplesAppended.addAndGet(ok + ooo);
        samplesOutOfOrder.addAndGet(ooo);
        samplesDuplicate.addAndGet(dup);
        samplesTooOld.addAndGet(old);
        samplesOverCardinality.addAndGet(card);
        return new AppendResult(ok + ooo, ooo, dup, old, card);
    }

    // ---------------------------------------------------------------- reads

    @Override
    public List<SeriesChunks> select(List<Matcher> matchers, long mint, long maxt, QueryContext ctx) {
        int[] ids = index.select(matchers);
        List<Block> bs = overlapping(mint, maxt);
        MemSeries[] arr = byId;
        List<SeriesChunks> out = new ArrayList<>();
        for (int k = 0; k < ids.length; k++) {
            MemSeries s = arr[ids[k]];
            List<Chunk> cs = new ArrayList<>(4);
            for (Block b : bs) b.chunks(s.id, mint, maxt, cs);
            s.chunks(mint, maxt, cs);
            if (cs.isEmpty()) continue;
            if (ctx != null) {
                ctx.addSeries(1);
                if ((k & 255) == 0) ctx.checkDeadline();
            }
            out.add(new SeriesChunks(s.labels, cs));
        }
        return out;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RollupSeries> selectRollup(List<Matcher> matchers, long mint, long maxt, long res, QueryContext ctx) {
        if (res != opt.rollupResMs) return null;
        int[] ids = index.select(matchers);
        List<Block> bs = overlapping(mint, maxt);
        long headFrom = minValidT; // blocks hold buckets ending at or before this; the head the rest
        MemSeries[] arr = byId;
        List<RollupSeries> out = new ArrayList<>();
        for (int k = 0; k < ids.length; k++) {
            MemSeries s = arr[ids[k]];
            List<Chunk>[] aggs = new List[Rollup.AGGS];
            for (int a = 0; a < Rollup.AGGS; a++) aggs[a] = new ArrayList<>(2);
            boolean any = false;
            for (Block b : bs) any |= b.rollups(s.id, mint, maxt, aggs);
            if (maxt > headFrom) {
                List<Chunk> raw = new ArrayList<>();
                long from = Math.max(headFrom, Rollup.bucketEnd(mint, res) - res);
                s.chunks(from + 1, maxt, raw);
                if (!raw.isEmpty()) {
                    SampleArray sa = SampleArray.decode(raw, from + 1, maxt, ctx);
                    Chunk[] r = Rollup.build(sa, from, maxt, res);
                    if (r != null) {
                        for (int a = 0; a < Rollup.AGGS; a++) aggs[a].add(r[a]);
                        any = true;
                    }
                }
            }
            if (!any) continue;
            if (ctx != null) {
                ctx.addSeries(1);
                if ((k & 255) == 0) ctx.checkDeadline();
            }
            out.add(new RollupSeries(s.labels, aggs));
        }
        return out;
    }

    private List<Block> overlapping(long mint, long maxt) {
        List<Block> out = new ArrayList<>();
        for (Block b : blocks) if (b.overlaps(mint, maxt)) out.add(b);
        return out;
    }

    public List<String> labelNames() { return index.labelNames(); }
    public List<String> labelValues(String name) { return index.labelValues(name); }
    public int seriesCount() { return index.seriesCount(); }
    public int labelValueCount(String name) { return index.labelValueCount(name); }

    /** Visits every series' labels (for anti-entropy digests and admin listings). */
    public void forEachSeries(Consumer<MemSeries> c) {
        MemSeries[] arr = byId;
        int n;
        synchronized (createLock) { n = nextId; }
        for (int i = 0; i < n; i++) c.accept(arr[i]);
    }

    /** All chunks of one series in [mint, maxt]. */
    public List<Chunk> seriesChunks(MemSeries s, long mint, long maxt) {
        List<Chunk> cs = new ArrayList<>();
        for (Block b : overlapping(mint, maxt)) b.chunks(s.id, mint, maxt, cs);
        s.chunks(mint, maxt, cs);
        return cs;
    }

    // ---------------------------------------------------------------- maintenance

    /** Cuts full head ranges into blocks, compacts, applies retention. Safe to call any time. */
    public void maintain() {
        synchronized (maintLock) {
            try {
                while (cutHeadOnce(false)) { /* keep cutting while the head is long */ }
                compact();
                retention();
                retryDeletes();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** Persists the whole head into blocks (used before measuring on-disk size). */
    public void flushHead() {
        synchronized (maintLock) {
            try {
                while (cutHeadOnce(true)) { /* until the head is empty */ }
                compact();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private boolean cutHeadOnce(boolean force) throws IOException {
        long hmax = headMaxT.get();
        if (hmax == Long.MIN_VALUE) return false;
        long start = minValidT != Long.MIN_VALUE ? minValidT
                : Math.floorDiv(headMinSeen.get(), opt.blockRangeMs) * opt.blockRangeMs;
        long end = start + opt.blockRangeMs;
        if (force) {
            if (start > hmax) return false;
        } else if (hmax < end + opt.blockRangeMs / 2) {
            return false;
        }
        cut(start, end);
        return true;
    }

    private void cut(long a, long b) throws IOException {
        long t0 = System.nanoTime();
        appendLock.writeLock().lock();
        try {
            minValidT = b; // late samples for [a, b) are refused from now on
        } finally {
            appendLock.writeLock().unlock();
        }
        BlockWriter w = new BlockWriter(blocksDir, a, b, 0, opt.rollupResMs);
        List<Integer> ids = new ArrayList<>();
        try {
            MemSeries[] arr = byId;
            int n;
            synchronized (createLock) { n = nextId; }
            for (int i = 0; i < n; i++) {
                MemSeries s = arr[i];
                List<Chunk> cs = s.chunksBefore(b);
                if (cs.isEmpty()) continue;
                cs.sort(Comparator.comparingLong(Chunk::minT));
                List<Chunk> forRollup = new ArrayList<>(cs);
                s.chunks(b, b, forRollup); // the sample at exactly b closes the last bucket
                SampleArray sa = SampleArray.decode(forRollup, a + 1, b, null);
                w.addSeries(s.labels, cs, Rollup.build(sa, a, b, opt.rollupResMs));
                ids.add(s.id);
            }
            if (ids.isEmpty()) {
                w.abort();
            } else {
                Block blk = Block.open(w.finish());
                int[] g = new int[blk.seriesCount()];
                for (int i = 0; i < g.length; i++) g[i] = byLabels.get(blk.labels(i)).id;
                blk.bindGlobalIds(g);
                List<Block> nb = new ArrayList<>(blocks);
                nb.add(blk);
                nb.sort(Comparator.comparingLong(x -> x.meta.rangeStart()));
                blocks = List.copyOf(nb);
            }
        } catch (IOException | RuntimeException e) {
            w.abort();
            throw e;
        }
        MemSeries[] arr = byId;
        for (int id : ids) arr[id].truncateBefore(b);
        checkpoint();
        blocksCut.incrementAndGet();
        hooks.cutNanos.accept(System.nanoTime() - t0);
    }

    /** Seals the WAL and replaces everything up to the seal with an image of the current head. */
    private void checkpoint() throws IOException {
        int sealed = wal.rotate();
        wal.writeCheckpoint(sealed, checkpointRecords());
    }

    private Iterable<byte[]> checkpointRecords() {
        return () -> new Iterator<>() {
            final MemSeries[] arr = byId;
            final int n;
            { synchronized (createLock) { n = nextId; } }
            int seriesPos = 0, samplePos = 0;
            byte[] nextRec;

            private byte[] produce() {
                if (seriesPos < n) {
                    MemSeries s = arr[seriesPos++];
                    return Records.series(s.id, s.labels);
                }
                Records.SamplesBuilder rb = null;
                while (samplePos < n) {
                    MemSeries s = arr[samplePos++];
                    List<Chunk> cs = new ArrayList<>();
                    s.chunks(Long.MIN_VALUE, Long.MAX_VALUE, cs);
                    if (cs.isEmpty()) continue;
                    SampleArray sa = SampleArray.decode(cs, Long.MIN_VALUE, Long.MAX_VALUE, null);
                    if (rb == null) rb = new Records.SamplesBuilder(sa.n);
                    for (int i = 0; i < sa.n; i++) rb.add(s.id, sa.t[i], sa.v[i]);
                    if (rb.count() >= 50_000) break;
                }
                return rb == null ? null : rb.build();
            }

            @Override public boolean hasNext() {
                if (nextRec == null) nextRec = produce();
                return nextRec != null;
            }

            @Override public byte[] next() {
                if (!hasNext()) throw new NoSuchElementException();
                byte[] r = nextRec;
                nextRec = null;
                return r;
            }
        };
    }

    private void compact() throws IOException {
        long parentRange = opt.blockRangeMs;
        for (int level = 0; level < opt.compactionFactors.length; level++) {
            parentRange *= opt.compactionFactors[level];
            TreeMap<Long, List<Block>> groups = new TreeMap<>();
            for (Block b : blocks) {
                if (b.meta.level() != level) continue;
                groups.computeIfAbsent(Math.floorDiv(b.meta.rangeStart(), parentRange), k -> new ArrayList<>()).add(b);
            }
            for (var e : groups.entrySet()) {
                long ps = e.getKey() * parentRange, pe = ps + parentRange;
                if (pe > minValidT || e.getValue().size() < 2) continue;
                merge(e.getValue(), ps, pe, level + 1);
            }
        }
    }

    private void merge(List<Block> group, long ps, long pe, int level) throws IOException {
        long t0 = System.nanoTime();
        group.sort(Comparator.comparingLong(b -> b.meta.rangeStart()));
        TreeMap<Labels, int[]> union = new TreeMap<>(); // labels -> local index per block (-1 absent)
        for (int bi = 0; bi < group.size(); bi++) {
            Block b = group.get(bi);
            for (int i = 0; i < b.seriesCount(); i++) {
                int[] locs = union.computeIfAbsent(b.labels(i), k -> {
                    int[] x = new int[group.size()];
                    Arrays.fill(x, -1);
                    return x;
                });
                locs[bi] = i;
            }
        }
        BlockWriter w = new BlockWriter(blocksDir, ps, pe, level, opt.rollupResMs);
        try {
            for (var e : union.entrySet()) {
                List<Chunk> raw = new ArrayList<>();
                XorChunk.Appender[] ra = null;
                for (int bi = 0; bi < group.size(); bi++) {
                    int loc = e.getValue()[bi];
                    if (loc < 0) continue;
                    Block b = group.get(bi);
                    b.chunksByLocal(loc, raw);
                    Chunk[] r = b.rollupByLocal(loc);
                    if (r != null) {
                        if (ra == null) {
                            ra = new XorChunk.Appender[Rollup.AGGS];
                            for (int k = 0; k < Rollup.AGGS; k++) ra[k] = new XorChunk.Appender();
                        }
                        for (int k = 0; k < Rollup.AGGS; k++) {
                            XorChunk.Iterator it = r[k].iterator();
                            while (it.next()) {
                                if (ra[k].count() > 0 && it.t() <= ra[k].lastT()) continue;
                                ra[k].append(it.t(), it.v());
                            }
                        }
                    }
                }
                Chunk[] rollup = null;
                if (ra != null && ra[0].count() > 0) {
                    rollup = new Chunk[Rollup.AGGS];
                    for (int k = 0; k < Rollup.AGGS; k++) rollup[k] = Chunk.of(ra[k].minT(), ra[k].maxT(), ra[k].toBytes());
                }
                w.addSeries(e.getKey(), raw, rollup);
            }
            Block merged = Block.open(w.finish());
            int[] g = new int[merged.seriesCount()];
            for (int i = 0; i < g.length; i++) g[i] = byLabels.get(merged.labels(i)).id;
            merged.bindGlobalIds(g);
            List<Block> nb = new ArrayList<>(blocks);
            nb.removeAll(group);
            nb.add(merged);
            nb.sort(Comparator.comparingLong(x -> x.meta.rangeStart()));
            blocks = List.copyOf(nb);
        } catch (IOException | RuntimeException ex) {
            w.abort();
            throw ex;
        }
        for (Block b : group) deleteLater(b.dir);
        compactions.incrementAndGet();
        hooks.compactionNanos.accept(System.nanoTime() - t0);
    }

    private void retention() throws IOException {
        long newest = headMaxT.get();
        if (newest == Long.MIN_VALUE || opt.retentionRawMs <= 0) return;
        long rawCut = newest - opt.retentionRawMs;
        long rollCut = opt.retentionRollupMs > opt.retentionRawMs ? newest - opt.retentionRollupMs : rawCut;
        List<Block> nb = new ArrayList<>();
        boolean changed = false;
        for (Block b : blocks) {
            if (b.meta.rangeEnd() <= rollCut) {
                deleteLater(b.dir);
                changed = true;
            } else if (b.meta.rangeEnd() <= rawCut && b.meta.hasRaw()) {
                Block.dropRaw(b.dir);
                Block reopened = Block.open(b.dir);
                int[] g = new int[reopened.seriesCount()];
                for (int i = 0; i < g.length; i++) g[i] = byLabels.get(reopened.labels(i)).id;
                reopened.bindGlobalIds(g);
                nb.add(reopened);
                changed = true;
            } else {
                nb.add(b);
            }
        }
        if (changed) blocks = List.copyOf(nb);
    }

    private void deleteLater(Path dir) {
        try {
            Block.deleteDir(dir);
        } catch (IOException e) {
            // on Windows a mapped file cannot be deleted until it is unmapped; retry later
            pendingDeletes.add(dir);
        }
    }

    private void retryDeletes() {
        List<Path> again = new ArrayList<>(pendingDeletes);
        pendingDeletes.clear();
        for (Path p : again) deleteLater(p);
    }

    // ---------------------------------------------------------------- stats

    public record Stats(int series, long headSamples, long headChunkBytes, int blocks, long blockSamples,
                        long blockChunkBytes, long blockRollupBytes, long blockBytesOnDisk, long walBytesOnDisk,
                        long headMaxT, long minValidT) {}

    public Stats stats() {
        long hs = 0, hb = 0;
        MemSeries[] arr = byId;
        int n;
        synchronized (createLock) { n = nextId; }
        for (int i = 0; i < n; i++) { hs += arr[i].sampleCount(); hb += arr[i].chunkBytes(); }
        long bs = 0, bc = 0, br = 0, bd = 0;
        for (Block b : blocks) { bs += b.meta.samples(); bc += b.chunkBytes(); br += b.rollupBytes(); bd += b.bytesOnDisk(); }
        long walBytes = 0;
        try (Stream<Path> s = Files.list(opt.dir.resolve("wal"))) {
            for (Path p : s.toList()) walBytes += Files.size(p);
        } catch (IOException ignored) {
        }
        return new Stats(n, hs, hb, blocks.size(), bs, bc, br, bd, walBytes, headMaxT.get(), minValidT);
    }

    public List<Block> blocks() { return blocks; }
    public Wal.ReplayStats replayStats() { return replayStats; }
    public long replayMillis() { return replayMillis; }
    public Options options() { return opt; }
    public Wal wal() { return wal; }

    @Override
    public void close() {
        if (maint != null) {
            maint.shutdownNow();
            try { maint.awaitTermination(10, TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
        }
        wal.close();
    }
}
