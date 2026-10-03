package io.metricsdb.server.cluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

/**
 * Hinted handoff: when a replica cannot take a write, the router keeps that replica's share of
 * the batch in an fsynced log on its own disk and replays it when the node is back. The hint is
 * durable before the client is acknowledged, so an acknowledged write is on at least one
 * replica's WAL plus, for every replica that missed it, the router's hint log.
 */
public final class HintStore {
    private final Path dir;
    private final Object[] locks;
    private final FileChannel[] current;
    private final int[] seq;
    public final AtomicLong written = new AtomicLong();
    public final AtomicLong replayed = new AtomicLong();
    private final AtomicLong[] pendingBytes;
    private final java.util.concurrent.locks.ReentrantLock[] replaying;

    public HintStore(Path dir, int nodes) {
        this.dir = dir;
        this.locks = new Object[nodes];
        this.current = new FileChannel[nodes];
        this.seq = new int[nodes];
        this.pendingBytes = new AtomicLong[nodes];
        this.replaying = new java.util.concurrent.locks.ReentrantLock[nodes];
        try {
            for (int i = 0; i < nodes; i++) {
                locks[i] = new Object();
                replaying[i] = new java.util.concurrent.locks.ReentrantLock();
                pendingBytes[i] = new AtomicLong();
                Path d = dir.resolve("node-" + i);
                Files.createDirectories(d);
                try (Stream<Path> s = Files.list(d)) {
                    for (Path p : s.toList()) {
                        seq[i] = Math.max(seq[i], Integer.parseInt(p.getFileName().toString().replace(".hints", "")));
                        pendingBytes[i].addAndGet(Files.size(p));
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long pendingBytes(int node) { return pendingBytes[node].get(); }

    public void append(int node, byte[] payload) throws IOException {
        synchronized (locks[node]) {
            if (current[node] == null) {
                seq[node]++;
                current[node] = FileChannel.open(file(node, seq[node]), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            }
            CRC32C crc = new CRC32C();
            crc.update(payload);
            ByteBuffer b = ByteBuffer.allocate(8 + payload.length);
            b.putInt(payload.length).putInt((int) crc.getValue()).put(payload).flip();
            while (b.hasRemaining()) current[node].write(b);
            current[node].force(false);
            pendingBytes[node].addAndGet(8 + payload.length);
            written.incrementAndGet();
        }
    }

    private Path file(int node, int n) {
        return dir.resolve("node-" + node).resolve(String.format("%08d.hints", n));
    }

    public interface Sender {
        void send(byte[] payload) throws IOException;
    }

    /**
     * Delivers every hint for the node in order. A file is deleted only after all of its records
     * were accepted; on the first failure replay stops and resumes on the next call.
     */
    public boolean replay(int node, Sender sender) throws IOException {
        if (!replaying[node].tryLock()) return false;
        try {
            return replayLocked(node, sender);
        } finally {
            replaying[node].unlock();
        }
    }

    private boolean replayLocked(int node, Sender sender) throws IOException {
        List<Path> files;
        synchronized (locks[node]) {
            if (current[node] != null) {
                current[node].close();
                current[node] = null; // new hints go to a fresh file while this one replays
            }
            try (Stream<Path> s = Files.list(dir.resolve("node-" + node))) {
                files = new ArrayList<>(s.sorted().toList());
            }
        }
        for (Path f : files) {
            byte[] all = Files.readAllBytes(f);
            ByteBuffer b = ByteBuffer.wrap(all);
            CRC32C crc = new CRC32C();
            while (b.remaining() >= 8) {
                int len = b.getInt();
                int sum = b.getInt();
                if (len < 0 || len > b.remaining()) break;
                byte[] p = new byte[len];
                b.get(p);
                crc.reset();
                crc.update(p);
                if ((int) crc.getValue() != sum) break;
                sender.send(p); // throws: keep the file, retry later (duplicates are idempotent)
                replayed.incrementAndGet();
            }
            Files.delete(f);
            pendingBytes[node].addAndGet(-all.length);
        }
        return true;
    }

    public boolean hasHints(int node) { return pendingBytes[node].get() > 0; }
}
