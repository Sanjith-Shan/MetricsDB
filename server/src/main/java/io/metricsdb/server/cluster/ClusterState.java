package io.metricsdb.server.cluster;

import io.metricsdb.cluster.HashRing;
import io.metricsdb.server.MetricsDbProperties;
import io.metricsdb.server.NodeMetrics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.IntConsumer;

/**
 * The router's view of the cluster: the ring, a client per storage node, and which nodes are up.
 * A health loop probes every node; a failed request marks a node down at once, and a node that
 * answers again is marked up, which triggers hint replay and repair.
 */
public final class ClusterState implements AutoCloseable {
    public final HashRing ring;
    public final List<NodeClient> nodes;
    private final AtomicIntegerArray up;
    private final List<IntConsumer> onUp = new CopyOnWriteArrayList<>();
    private final Thread health;
    private volatile boolean closed;

    public ClusterState(MetricsDbProperties.Cluster cfg, NodeMetrics metrics) {
        List<String> names = new ArrayList<>();
        List<NodeClient> clients = new ArrayList<>();
        for (String spec : cfg.getNodes()) {
            String[] kv = spec.split("=", 2);
            if (kv.length != 2) throw new IllegalArgumentException("metricsdb.cluster.nodes entries are name=url, got " + spec);
            names.add(kv[0].trim());
            clients.add(new NodeClient(kv[0].trim(), kv[1].trim(), cfg.getRequestTimeout()));
        }
        if (clients.isEmpty()) throw new IllegalArgumentException("router needs metricsdb.cluster.nodes");
        this.nodes = List.copyOf(clients);
        this.ring = new HashRing(names, cfg.getVnodes(), cfg.getReplicationFactor());
        this.up = new AtomicIntegerArray(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            up.set(i, 1);
            final int n = i;
            metrics.gauge("metricsdb.router.node.up", () -> up.get(n), "node", nodes.get(i).name);
        }
        Duration every = cfg.getHealthInterval();
        health = Thread.ofVirtual().name("cluster-health").start(() -> {
            while (!closed) {
                for (int i = 0; i < nodes.size(); i++) probe(i);
                try { Thread.sleep(every.toMillis()); } catch (InterruptedException e) { return; }
            }
        });
    }

    private void probe(int i) {
        try {
            nodes.get(i).get("/internal/health", Duration.ofMillis(400));
            markUp(i);
        } catch (Exception e) {
            markDown(i);
        }
    }

    public boolean isUp(int i) { return up.get(i) == 1; }

    public void markDown(int i) { up.set(i, 0); }

    public void markUp(int i) {
        if (up.getAndSet(i, 1) == 0) for (IntConsumer c : onUp) c.accept(i);
    }

    public void onNodeUp(IntConsumer c) { onUp.add(c); }

    public int size() { return nodes.size(); }

    @Override public void close() {
        closed = true;
        health.interrupt();
    }
}
