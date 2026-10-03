package io.metricsdb.block;

import io.metricsdb.model.Labels;
import io.metricsdb.storage.Chunk;
import io.metricsdb.util.ByteOut;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Writes one immutable block: {@code chunks} (raw Gorilla chunks back to back), {@code rollup}
 * (downsampled chunks), {@code index} (series labels and chunk references) and
 * {@code meta.properties}. Everything goes to a temp directory that is fsynced and renamed
 * into place, so a crash leaves either the whole block or nothing.
 */
public final class BlockWriter {
    public static final int INDEX_MAGIC = 0x4D444249; // "MDBI"

    private final Path parent;
    private final Path tmp;
    private final long rangeStart, rangeEnd, rollupRes;
    private final int level;
    private final OutputStream chunksOut, rollupOut;
    private final ByteOut index = new ByteOut(1 << 16);
    private long chunkOff, rollupOff;
    private long series, samples, chunks;
    private long minT = Long.MAX_VALUE, maxT = Long.MIN_VALUE;

    public BlockWriter(Path parent, long rangeStart, long rangeEnd, int level, long rollupRes) throws IOException {
        this.parent = parent;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
        this.level = level;
        this.rollupRes = rollupRes;
        this.tmp = parent.resolve(Block.dirName(rangeStart, rangeEnd, level) + ".tmp");
        if (Files.exists(tmp)) Block.deleteDir(tmp);
        Files.createDirectories(tmp);
        chunksOut = new BufferedOutputStream(Files.newOutputStream(tmp.resolve("chunks")), 1 << 16);
        rollupOut = new BufferedOutputStream(Files.newOutputStream(tmp.resolve("rollup")), 1 << 16);
        index.i32(INDEX_MAGIC).u8(1);
        index.i32(0); // series count, patched in finish()
    }

    /** Raw chunks must be sorted by time and non-overlapping; rollup may be null. */
    public void addSeries(Labels labels, List<Chunk> raw, Chunk[] rollup) throws IOException {
        if (raw.isEmpty() && rollup == null) return;
        series++;
        index.uvarint(labels.size());
        for (int i = 0; i < labels.size(); i++) index.str(labels.name(i)).str(labels.value(i));
        index.uvarint(raw.size());
        for (Chunk c : raw) {
            byte[] b = c.bytes();
            chunksOut.write(b);
            index.varint(c.minT()).uvarint(c.maxT() - c.minT()).uvarint(chunkOff).uvarint(b.length);
            chunkOff += b.length;
            samples += c.count();
            chunks++;
            minT = Math.min(minT, c.minT());
            maxT = Math.max(maxT, c.maxT());
        }
        if (rollup == null) {
            index.u8(0);
        } else {
            index.u8(1);
            for (Chunk c : rollup) {
                byte[] b = c.bytes();
                rollupOut.write(b);
                index.varint(c.minT()).uvarint(c.maxT() - c.minT()).uvarint(rollupOff).uvarint(b.length);
                rollupOff += b.length;
            }
        }
    }

    public long seriesWritten() { return series; }

    public Path finish() throws IOException {
        chunksOut.close();
        rollupOut.close();
        index.setI32(5, (int) series);
        CRC32 crc = new CRC32();
        crc.update(index.array(), 0, index.length());
        index.i32((int) crc.getValue());
        Files.write(tmp.resolve("index"), index.toByteArray());
        new BlockMeta(rangeStart, rangeEnd, series == 0 ? rangeStart : minT, series == 0 ? rangeStart : maxT,
                level, series, samples, chunks, rollupRes, true).write(tmp.resolve("meta.properties"));
        for (String f : new String[]{"chunks", "rollup", "index", "meta.properties"}) {
            try (FileChannel c = FileChannel.open(tmp.resolve(f), StandardOpenOption.WRITE)) {
                c.force(true);
            }
        }
        Path fin = parent.resolve(Block.dirName(rangeStart, rangeEnd, level));
        if (Files.exists(fin)) Block.deleteDir(fin);
        Files.move(tmp, fin, StandardCopyOption.ATOMIC_MOVE);
        Block.fsyncDir(parent);
        return fin;
    }

    public void abort() {
        try {
            chunksOut.close();
            rollupOut.close();
            Block.deleteDir(tmp);
        } catch (IOException ignored) {
        }
    }
}
