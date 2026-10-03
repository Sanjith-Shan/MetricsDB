package io.metricsdb.server.cluster;

import io.metricsdb.cluster.Wire;
import io.metricsdb.server.Ingester;
import io.metricsdb.server.NodeMetrics;
import io.metricsdb.storage.WriteBatch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The router's write path. Each sample goes to the {@code rf} replicas its series hashes to;
 * the batch is split per node and the per-node shares are sent concurrently on virtual threads.
 * A sample is acknowledged once {@code writeQuorum} of its replicas made it durable; for every
 * replica that failed (or is known to be down) its share is written to the hint log first.
 */
public final class ReplicatingIngester implements Ingester {
    private final ClusterState cluster;
    private final HintStore hints;
    private final int writeQuorum;
    private final NodeMetrics metrics;
    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    public ReplicatingIngester(ClusterState cluster, HintStore hints, int writeQuorum, NodeMetrics metrics) {
        this.cluster = cluster;
        this.hints = hints;
        this.writeQuorum = Math.min(writeQuorum, cluster.ring.replicationFactor());
        this.metrics = metrics;
        cluster.onNodeUp(this::replayHintsAsync);
        Thread.ofVirtual().name("hint-replayer").start(() -> {
            while (true) {
                try { Thread.sleep(2_000); } catch (InterruptedException e) { return; }
                for (int i = 0; i < cluster.size(); i++) if (cluster.isUp(i) && hints != null && hints.hasHints(i)) replayHints(i);
            }
        });
    }

    @Override
    public Result write(WriteBatch b) {
        int nodes = cluster.size();
        int[][] idx = new int[nodes][];
        int[] cnt = new int[nodes];
        int[][] replicasOf = new int[b.n][];
        for (int i = 0; i < b.n; i++) {
            int[] reps = cluster.ring.replicas(b.labels[i]);
            replicasOf[i] = reps;
            for (int r : reps) {
                if (idx[r] == null) idx[r] = new int[Math.max(16, b.n / nodes * 2)];
                if (cnt[r] == idx[r].length) idx[r] = Arrays.copyOf(idx[r], cnt[r] * 2);
                idx[r][cnt[r]++] = i;
            }
        }
        byte[][] payload = new byte[nodes][];
        boolean[] ok = new boolean[nodes];
        String[] err = new String[nodes];
        long[][] refused = new long[nodes][2];
        List<Future<?>> fs = new ArrayList<>();
        for (int n = 0; n < nodes; n++) {
            if (cnt[n] == 0) continue;
            payload[n] = Wire.encodeBatch(b, idx[n], cnt[n]);
            if (!cluster.isUp(n)) {
                err[n] = cluster.nodes.get(n).name + " is down";
                continue;
            }
            final int node = n;
            fs.add(pool.submit(() -> {
                long t0 = System.nanoTime();
                try {
                    byte[] resp = cluster.nodes.get(node).post("/internal/write", payload[node]);
                    String[] f = new String(resp, StandardCharsets.UTF_8).trim().split(",");
                    if (f.length >= 3) { refused[node][0] = Long.parseLong(f[1]); refused[node][1] = Long.parseLong(f[2]); }
                    ok[node] = true;
                    metrics.recordNanos("metricsdb.router.replica.write", System.nanoTime() - t0, "node", cluster.nodes.get(node).name, "result", "ok");
                } catch (Exception e) {
                    err[node] = e.getMessage();
                    if (!(e instanceof NodeClient.NodeException ne && ne.status / 100 == 4)) cluster.markDown(node);
                    metrics.recordNanos("metricsdb.router.replica.write", System.nanoTime() - t0, "node", cluster.nodes.get(node).name, "result", "error");
                }
            }));
        }
        for (Future<?> f : fs) {
            try { f.get(); } catch (Exception ignored) { }
        }
        // per sample: enough durable replicas?
        long accepted = 0, unavailable = 0;
        for (int i = 0; i < b.n; i++) {
            int good = 0;
            for (int r : replicasOf[i]) if (ok[r]) good++;
            if (good >= writeQuorum) accepted++; else unavailable++;
        }
        // hints for every replica that missed its share, written before the client hears back
        for (int n = 0; n < nodes; n++) {
            if (payload[n] == null || ok[n]) continue;
            if (hints == null) continue;
            try {
                hints.append(n, payload[n]);
                metrics.counter("metricsdb.router.hints.written", "node", cluster.nodes.get(n).name).increment(cnt[n]);
            } catch (Exception e) {
                // without a durable hint, these samples count only if quorum was met without this node
                metrics.counter("metricsdb.router.hints.failed").increment();
            }
        }
        String detail = null;
        if (unavailable > 0) {
            List<String> why = new ArrayList<>();
            for (int n = 0; n < nodes; n++) if (err[n] != null) why.add(err[n]);
            detail = unavailable + " samples reached fewer than " + writeQuorum + " replicas (" + String.join("; ", why) + ")";
        }
        long overCard = 0, tooOld = 0;
        for (int n = 0; n < nodes; n++) { overCard = Math.max(overCard, refused[n][0]); tooOld = Math.max(tooOld, refused[n][1]); }
        if (overCard > 0 && detail == null) detail = "a storage node reached its series limit (metricsdb.storage.max-series); " + overCard + " samples for new series were refused";
        return new Result(Math.max(0, accepted - overCard - tooOld), overCard, tooOld, unavailable, detail);
    }

    private void replayHintsAsync(int node) {
        if (hints != null) pool.submit(() -> replayHints(node));
    }

    private void replayHints(int node) {
        try {
            hints.replay(node, p -> cluster.nodes.get(node).post("/internal/write", p));
        } catch (Exception e) {
            cluster.markDown(node);
        }
    }

    public String status() {
        StringBuilder sb = new StringBuilder();
        for (int n = 0; n < cluster.size(); n++) {
            sb.append(cluster.nodes.get(n).name).append(cluster.isUp(n) ? " up" : " down")
                    .append(", hint bytes pending ").append(hints == null ? 0 : hints.pendingBytes(n)).append('\n');
        }
        return sb.toString();
    }
}
