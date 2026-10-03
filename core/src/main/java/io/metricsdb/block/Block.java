package io.metricsdb.block;

import io.metricsdb.model.Labels;
import io.metricsdb.storage.Chunk;
import io.metricsdb.util.ByteIn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.CRC32;

/**
 * An immutable, time-partitioned block opened for reads. The chunk files are memory-mapped, so
 * reading a chunk is a page-cache hit or a page fault, never a copy; the series table (labels
 * and chunk references) is parsed into compact arrays on open.
 */
public final class Block {
    public final Path dir;
    public final BlockMeta meta;
    private final ByteBuffer chunks;
    private final ByteBuffer rollup;
    private final Labels[] labels;
    private final int[] chunkStart;   // series i has chunks [chunkStart[i], chunkStart[i+1])
    private final long[] cMinT, cMaxT;
    private final int[] cOff, cLen;
    private final long[] rMinT, rMaxT; // 5 per series, rMinT = Long.MIN_VALUE when absent
    private final int[] rOff, rLen;
    private int[] sortedGlobal;        // global ids, sorted
    private int[] localOfSorted;       // local index for sortedGlobal[k]
    private final long bytes;

    private Block(Path dir) throws IOException {
        this.dir = dir;
        this.meta = BlockMeta.read(dir.resolve("meta.properties"));
        this.chunks = meta.hasRaw() ? map(dir.resolve("chunks")) : ByteBuffer.allocate(0);
        this.rollup = map(dir.resolve("rollup"));
        byte[] idx = Files.readAllBytes(dir.resolve("index"));
        CRC32 crc = new CRC32();
        crc.update(idx, 0, idx.length - 4);
        if ((int) crc.getValue() != ByteBuffer.wrap(idx).getInt(idx.length - 4)) {
            throw new IOException("index checksum mismatch in " + dir);
        }
        ByteIn in = new ByteIn(idx, 0, idx.length - 4);
        if (in.i32() != BlockWriter.INDEX_MAGIC) throw new IOException("bad index magic in " + dir);
        in.u8();
        int n = in.i32();
        labels = new Labels[n];
        chunkStart = new int[n + 1];
        int cap = (int) Math.max(16, meta.chunks());
        long[] minT = new long[cap], maxT = new long[cap];
        int[] off = new int[cap], len = new int[cap];
        rMinT = new long[n * Rollup.AGGS];
        rMaxT = new long[n * Rollup.AGGS];
        rOff = new int[n * Rollup.AGGS];
        rLen = new int[n * Rollup.AGGS];
        int c = 0;
        for (int i = 0; i < n; i++) {
            int nl = (int) in.uvarint();
            String[] kv = new String[nl * 2];
            for (int j = 0; j < kv.length; j++) kv[j] = in.str().intern();
            labels[i] = Labels.fromSorted(kv);
            chunkStart[i] = c;
            int nc = (int) in.uvarint();
            for (int j = 0; j < nc; j++) {
                if (c == minT.length) {
                    minT = Arrays.copyOf(minT, c * 2); maxT = Arrays.copyOf(maxT, c * 2);
                    off = Arrays.copyOf(off, c * 2); len = Arrays.copyOf(len, c * 2);
                }
                minT[c] = in.varint();
                maxT[c] = minT[c] + in.uvarint();
                off[c] = (int) in.uvarint();
                len[c] = (int) in.uvarint();
                c++;
            }
            if (in.u8() == 1) {
                for (int k = 0; k < Rollup.AGGS; k++) {
                    int p = i * Rollup.AGGS + k;
                    rMinT[p] = in.varint();
                    rMaxT[p] = rMinT[p] + in.uvarint();
                    rOff[p] = (int) in.uvarint();
                    rLen[p] = (int) in.uvarint();
                }
            } else {
                for (int k = 0; k < Rollup.AGGS; k++) rMinT[i * Rollup.AGGS + k] = Long.MIN_VALUE;
            }
        }
        chunkStart[n] = c;
        cMinT = minT; cMaxT = maxT; cOff = off; cLen = len;
        bytes = dirSize(dir);
    }

    public static Block open(Path dir) throws IOException { return new Block(dir); }

    private static ByteBuffer map(Path p) throws IOException {
        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
            long size = ch.size();
            if (size == 0) return ByteBuffer.allocate(0);
            if (size > Integer.MAX_VALUE) throw new IOException("block file over 2 GB: " + p);
            MappedByteBuffer m = ch.map(FileChannel.MapMode.READ_ONLY, 0, size);
            return m;
        }
    }

    public int seriesCount() { return labels.length; }
    public Labels labels(int local) { return labels[local]; }
    public long bytesOnDisk() { return bytes; }
    public long chunkBytes() { return chunks.capacity(); }
    public long rollupBytes() { return rollup.capacity(); }

    /** Maps each local series to the node-wide series id (assigned by the caller at load). */
    public void bindGlobalIds(int[] globalOfLocal) {
        Integer[] order = new Integer[globalOfLocal.length];
        for (int i = 0; i < order.length; i++) order[i] = i;
        Arrays.sort(order, Comparator.comparingInt(i -> globalOfLocal[i]));
        int[] sg = new int[order.length], lo = new int[order.length];
        for (int k = 0; k < order.length; k++) { sg[k] = globalOfLocal[order[k]]; lo[k] = order[k]; }
        sortedGlobal = sg;
        localOfSorted = lo;
    }

    private int local(int globalId) {
        int k = Arrays.binarySearch(sortedGlobal, globalId);
        return k < 0 ? -1 : localOfSorted[k];
    }

    public boolean overlaps(long mint, long maxt) { return meta.minT() <= maxt && meta.maxT() >= mint; }

    public void chunks(int globalId, long mint, long maxt, List<Chunk> out) {
        if (!meta.hasRaw()) return;
        int i = local(globalId);
        if (i < 0) return;
        for (int c = chunkStart[i]; c < chunkStart[i + 1]; c++) {
            if (cMinT[c] <= maxt && cMaxT[c] >= mint) out.add(new Chunk(cMinT[c], cMaxT[c], chunks, cOff[c], cLen[c]));
        }
    }

    public void chunksByLocal(int i, List<Chunk> out) {
        if (!meta.hasRaw()) return;
        for (int c = chunkStart[i]; c < chunkStart[i + 1]; c++) out.add(new Chunk(cMinT[c], cMaxT[c], chunks, cOff[c], cLen[c]));
    }

    /** Appends this block's rollup chunks for the series to {@code out[agg]}; false if none. */
    public boolean rollups(int globalId, long mint, long maxt, List<Chunk>[] out) {
        int i = local(globalId);
        return i >= 0 && rollupsByLocal(i, mint, maxt, out);
    }

    public boolean rollupsByLocal(int i, long mint, long maxt, List<Chunk>[] out) {
        int p = i * Rollup.AGGS;
        if (rMinT[p] == Long.MIN_VALUE) return false;
        if (rMinT[p] > maxt || rMaxT[p] < mint) return false;
        for (int k = 0; k < Rollup.AGGS; k++) {
            out[k].add(new Chunk(rMinT[p + k], rMaxT[p + k], rollup, rOff[p + k], rLen[p + k]));
        }
        return true;
    }

    public Chunk[] rollupByLocal(int i) {
        int p = i * Rollup.AGGS;
        if (rMinT[p] == Long.MIN_VALUE) return null;
        Chunk[] out = new Chunk[Rollup.AGGS];
        for (int k = 0; k < Rollup.AGGS; k++) out[k] = new Chunk(rMinT[p + k], rMaxT[p + k], rollup, rOff[p + k], rLen[p + k]);
        return out;
    }

    public static String dirName(long rangeStart, long rangeEnd, int level) {
        return String.format("b-%013d-%013d-L%d", rangeStart, rangeEnd, level);
    }

    public static void deleteDir(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path p : s.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
        }
    }

    static long dirSize(Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            long total = 0;
            for (Path p : s.toList()) total += Files.size(p);
            return total;
        }
    }

    public static void fsyncDir(Path dir) {
        try (FileChannel c = FileChannel.open(dir, StandardOpenOption.READ)) {
            c.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
        }
    }

    /** Drops the raw chunks, keeping the rollups (retention of raw data shorter than rollups). */
    public static void dropRaw(Path dir) throws IOException {
        BlockMeta m = BlockMeta.read(dir.resolve("meta.properties"));
        m.withoutRaw().write(dir.resolve("meta.properties.tmp"));
        Files.move(dir.resolve("meta.properties.tmp"), dir.resolve("meta.properties"),
                java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        Files.deleteIfExists(dir.resolve("chunks"));
    }
}
