package io.metricsdb.wal;

import io.metricsdb.util.ByteOut;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

/**
 * Write-ahead log with group commit. Callers enqueue a record and get a future; one writer
 * thread drains everything queued, writes it with a single {@code write}, issues one
 * {@code fsync} for the whole group and only then completes the futures. A write is acknowledged
 * to the client after its future completes, so an acknowledged sample is on disk.
 *
 * <p>Frame: 4-byte length, 4-byte CRC32C of the payload, payload. On replay a short or
 * mismatching frame marks a torn tail write: the segment is truncated there.
 *
 * <p>Files: numbered segments {@code 00000001, 00000002, ...} and at most one
 * {@code checkpoint.N}, a compacted image of the head that replaces segments 1..N.
 */
public final class Wal implements Closeable {
    public enum Sync { ALWAYS, NONE }

    private static final int FRAME_HEADER = 8;
    private static final int MAX_GROUP_BYTES = 8 << 20;

    private sealed interface Op permits Rec, Rotate {}
    private record Rec(byte[] payload, CompletableFuture<Void> done) implements Op {}
    private record Rotate(CompletableFuture<Integer> done) implements Op {}

    private final Path dir;
    private final Sync sync;
    private final long segmentBytes;
    private final LinkedBlockingQueue<Op> queue = new LinkedBlockingQueue<>();
    private final Thread writer;
    private volatile boolean closed;
    private volatile LongConsumer fsyncListener = n -> {};
    private final AtomicLong bytesWritten = new AtomicLong();
    private final AtomicLong fsyncs = new AtomicLong();

    private FileChannel ch;
    private int segNo;
    private long segLen;

    public Wal(Path dir, Sync sync, long segmentBytes) {
        this.dir = dir;
        this.sync = sync;
        this.segmentBytes = segmentBytes;
        try {
            Files.createDirectories(dir);
            segNo = lastSegment(dir) + 1;
            openSegment();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        writer = Thread.ofPlatform().name("wal-writer").daemon(true).start(this::run);
    }

    public void onFsync(LongConsumer nanos) { this.fsyncListener = nanos; }
    public long bytesWritten() { return bytesWritten.get(); }
    public long fsyncCount() { return fsyncs.get(); }
    public int currentSegment() { return segNo; }

    private void openSegment() throws IOException {
        ch = FileChannel.open(dir.resolve(segName(segNo)), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        segLen = 0;
        fsyncDir(dir);
    }

    public CompletableFuture<Void> append(byte[] payload) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (closed) {
            f.completeExceptionally(new IOException("WAL closed"));
            return f;
        }
        queue.add(new Rec(payload, f));
        return f;
    }

    /**
     * Seals the current segment and starts a new one. Returns the sealed segment number N: every
     * record enqueued before this call is in a segment numbered N or lower.
     */
    public int rotate() {
        CompletableFuture<Integer> f = new CompletableFuture<>();
        queue.add(new Rotate(f));
        return f.join();
    }

    private void run() {
        List<Op> batch = new ArrayList<>();
        ByteOut out = new ByteOut(1 << 20);
        CRC32C crc = new CRC32C();
        while (!closed || !queue.isEmpty()) {
            try {
                Op first = queue.poll(200, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (first == null) continue;
                batch.add(first);
                int bytes = first instanceof Rec r ? r.payload.length : 0;
                Op next;
                while (bytes < MAX_GROUP_BYTES && !(batch.get(batch.size() - 1) instanceof Rotate)
                        && (next = queue.peek()) != null) {
                    if (next instanceof Rotate && bytes > 0) break; // flush records first, rotate next round
                    queue.poll();
                    batch.add(next);
                    if (next instanceof Rec r) bytes += r.payload.length;
                }
                process(batch, out, crc);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                for (Op op : batch) fail(op, t);
            } finally {
                batch.clear();
            }
        }
    }

    private static void fail(Op op, Throwable t) {
        if (op instanceof Rec r) r.done.completeExceptionally(t);
        else if (op instanceof Rotate ro) ro.done.completeExceptionally(t);
    }

    private void process(List<Op> batch, ByteOut out, CRC32C crc) throws IOException {
        out.reset();
        List<Rec> recs = new ArrayList<>(batch.size());
        Rotate rotate = null;
        for (Op op : batch) {
            if (op instanceof Rec r) {
                crc.reset();
                crc.update(r.payload, 0, r.payload.length);
                out.i32(r.payload.length).i32((int) crc.getValue()).bytes(r.payload);
                recs.add(r);
            } else {
                rotate = (Rotate) op;
            }
        }
        if (out.length() > 0) {
            ByteBuffer bb = ByteBuffer.wrap(out.array(), 0, out.length());
            while (bb.hasRemaining()) ch.write(bb);
            segLen += out.length();
            bytesWritten.addAndGet(out.length());
            if (sync == Sync.ALWAYS) {
                long t0 = System.nanoTime();
                ch.force(false);
                fsyncs.incrementAndGet();
                fsyncListener.accept(System.nanoTime() - t0);
            }
        }
        for (Rec r : recs) r.done.complete(null);
        if (rotate != null || segLen >= segmentBytes) {
            int sealed = segNo;
            ch.force(false);
            ch.close();
            segNo++;
            openSegment();
            if (rotate != null) rotate.done.complete(sealed);
        }
    }

    /**
     * Writes {@code checkpoint.N} from the given records (atomically, via a temp file and
     * rename), then deletes segments 1..N and older checkpoints.
     */
    public void writeCheckpoint(int n, Iterable<byte[]> records) throws IOException {
        Path tmp = dir.resolve(String.format("checkpoint.%08d.tmp", n));
        CRC32C crc = new CRC32C();
        try (FileChannel c = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteOut out = new ByteOut(1 << 20);
            for (byte[] p : records) {
                crc.reset();
                crc.update(p, 0, p.length);
                out.i32(p.length).i32((int) crc.getValue()).bytes(p);
                if (out.length() > (4 << 20)) { writeAll(c, out); out.reset(); }
            }
            writeAll(c, out);
            c.force(true);
        }
        Path fin = dir.resolve(String.format("checkpoint.%08d", n));
        Files.move(tmp, fin, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        fsyncDir(dir);
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : s.toList()) {
                String name = p.getFileName().toString();
                if (name.startsWith("checkpoint.") && !name.endsWith(".tmp") && !p.equals(fin)
                        && parseNum(name.substring(11)) < n) Files.deleteIfExists(p);
                else if (isSegment(name) && Integer.parseInt(name) <= n) Files.deleteIfExists(p);
            }
        }
        fsyncDir(dir);
    }

    private static void writeAll(FileChannel c, ByteOut out) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(out.array(), 0, out.length());
        while (bb.hasRemaining()) c.write(bb);
    }

    /** Stats from a replay: how many frames were read, and whether a torn tail was cut off. */
    public record ReplayStats(long records, long bytes, int segments, long tornTails) {}

    /** Feeds every intact record to the consumer: the newest checkpoint, then later segments in order. */
    public static ReplayStats replay(Path dir, Consumer<byte[]> consumer) throws IOException {
        if (!Files.isDirectory(dir)) return new ReplayStats(0, 0, 0, 0);
        int cp = -1;
        Path cpFile = null;
        List<Integer> segs = new ArrayList<>();
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : s.toList()) {
                String name = p.getFileName().toString();
                if (name.startsWith("checkpoint.") && !name.endsWith(".tmp")) {
                    int n = parseNum(name.substring(11));
                    if (n > cp) { cp = n; cpFile = p; }
                } else if (name.endsWith(".tmp")) {
                    Files.deleteIfExists(p);
                } else if (isSegment(name)) {
                    segs.add(Integer.parseInt(name));
                }
            }
        }
        segs.sort(Integer::compare);
        long[] stats = new long[3];
        int segments = 0;
        if (cpFile != null) { readFile(cpFile, consumer, stats); segments++; }
        for (int n : segs) {
            if (n <= cp) continue;
            readFile(dir.resolve(segName(n)), consumer, stats);
            segments++;
        }
        return new ReplayStats(stats[0], stats[1], segments, stats[2]);
    }

    private static void readFile(Path p, Consumer<byte[]> consumer, long[] stats) throws IOException {
        byte[] all = Files.readAllBytes(p);
        ByteBuffer b = ByteBuffer.wrap(all);
        CRC32C crc = new CRC32C();
        int pos = 0;
        while (pos + FRAME_HEADER <= all.length) {
            int len = b.getInt(pos);
            int sum = b.getInt(pos + 4);
            if (len < 0 || pos + FRAME_HEADER + len > all.length) break;
            crc.reset();
            crc.update(all, pos + FRAME_HEADER, len);
            if ((int) crc.getValue() != sum) break;
            byte[] payload = new byte[len];
            System.arraycopy(all, pos + FRAME_HEADER, payload, 0, len);
            consumer.accept(payload);
            stats[0]++;
            stats[1] += len;
            pos += FRAME_HEADER + len;
        }
        if (pos < all.length) {
            // torn or corrupt tail: everything after the last intact frame was never acknowledged
            stats[2]++;
            try (FileChannel c = FileChannel.open(p, StandardOpenOption.WRITE)) {
                c.truncate(pos);
                c.force(true);
            }
        }
    }

    private static int lastSegment(Path dir) throws IOException {
        int max = 0;
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : s.toList()) {
                String name = p.getFileName().toString();
                if (isSegment(name)) max = Math.max(max, Integer.parseInt(name));
                else if (name.startsWith("checkpoint.") && !name.endsWith(".tmp")) max = Math.max(max, parseNum(name.substring(11)));
            }
        }
        return max;
    }

    private static boolean isSegment(String name) {
        if (name.length() != 8) return false;
        for (int i = 0; i < 8; i++) if (!Character.isDigit(name.charAt(i))) return false;
        return true;
    }

    private static int parseNum(String s) {
        try { return Integer.parseInt(s); } catch (NumberFormatException e) { return -1; }
    }

    private static String segName(int n) { return String.format("%08d", n); }

    static void fsyncDir(Path dir) {
        // directory fsync makes file creation and rename durable on Linux; it is a no-op elsewhere
        try (FileChannel c = FileChannel.open(dir, StandardOpenOption.READ)) {
            c.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
        }
    }

    @Override public void close() {
        closed = true;
        try {
            writer.join(5000);
            if (ch != null && ch.isOpen()) {
                ch.force(false);
                ch.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
