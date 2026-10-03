package io.metricsdb.block;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** Block metadata, stored as {@code meta.properties} next to the block's files. */
public record BlockMeta(long rangeStart, long rangeEnd, long minT, long maxT, int level,
                        long series, long samples, long chunks, long rollupRes, boolean hasRaw) {

    public void write(Path file) throws IOException {
        String s = "rangeStart=" + rangeStart + "\nrangeEnd=" + rangeEnd + "\nminT=" + minT + "\nmaxT=" + maxT
                + "\nlevel=" + level + "\nseries=" + series + "\nsamples=" + samples + "\nchunks=" + chunks
                + "\nrollupRes=" + rollupRes + "\nhasRaw=" + hasRaw + "\n";
        Files.writeString(file, s);
    }

    public static BlockMeta read(Path file) throws IOException {
        Properties p = new Properties();
        p.load(new StringReader(Files.readString(file)));
        return new BlockMeta(l(p, "rangeStart"), l(p, "rangeEnd"), l(p, "minT"), l(p, "maxT"),
                (int) l(p, "level"), l(p, "series"), l(p, "samples"), l(p, "chunks"), l(p, "rollupRes"),
                Boolean.parseBoolean(p.getProperty("hasRaw", "true")));
    }

    private static long l(Properties p, String k) { return Long.parseLong(p.getProperty(k).trim()); }

    public BlockMeta withoutRaw() {
        return new BlockMeta(rangeStart, rangeEnd, minT, maxT, level, series, samples, chunks, rollupRes, false);
    }
}
