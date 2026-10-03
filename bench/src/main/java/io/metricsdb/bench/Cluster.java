package io.metricsdb.bench;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Runs a local cluster as real processes: N storage nodes and a router, each its own JVM, with a
 * fault-injecting TCP proxy on every router-to-node link. Supports kill -9, SIGSTOP/SIGCONT, link
 * drops and restarts (optionally with an empty disk).
 */
public final class Cluster implements AutoCloseable {
    public final int n;
    public final int basePort;
    private final String java;
    private final String jar;
    private final Path work;
    private final Process[] nodes;
    private Process router;
    public final FaultProxy[] proxies;
    private final List<String> routerArgs;

    public Cluster(String jar, Path work, int n, int basePort, int rf, List<String> extraRouterArgs) throws IOException {
        this.n = n;
        this.basePort = basePort;
        this.jar = jar;
        this.work = work;
        this.java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        this.nodes = new Process[n];
        this.proxies = new FaultProxy[n];
        deleteTree(work);
        Files.createDirectories(work);
        List<String> ra = new ArrayList<>();
        ra.add("--metricsdb.role=router");
        ra.add("--metricsdb.node-name=router");
        ra.add("--metricsdb.cluster.replication-factor=" + rf);
        ra.add("--metricsdb.cluster.request-timeout=3s");
        ra.add("--metricsdb.cluster.health-interval=250ms");
        StringBuilder spec = new StringBuilder();
        for (int i = 0; i < n; i++) {
            proxies[i] = new FaultProxy(proxyPort(i), "127.0.0.1", nodePort(i));
            if (i > 0) spec.append(',');
            spec.append("n").append(i).append("=http://127.0.0.1:").append(proxyPort(i));
        }
        ra.add("--metricsdb.cluster.nodes=" + spec);
        ra.addAll(extraRouterArgs);
        this.routerArgs = ra;
    }

    public int nodePort(int i) { return basePort + 1 + i; }
    public int proxyPort(int i) { return basePort + 101 + i; }
    public String routerUrl() { return "http://127.0.0.1:" + basePort; }
    public String nodeUrl(int i) { return "http://127.0.0.1:" + nodePort(i); }
    public List<String> nodeNames() {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < n; i++) out.add("n" + i);
        return out;
    }

    public void startAll() throws Exception {
        for (int i = 0; i < n; i++) startNode(i, false);
        router = spawn("router", routerArgs, work.resolve("router"));
        for (int i = 0; i < n; i++) waitHealthy(nodeUrl(i));
        waitHealthy(routerUrl());
    }

    public void startNode(int i, boolean wipe) throws Exception {
        Path dir = work.resolve("n" + i);
        if (wipe) deleteTree(dir);
        nodes[i] = spawn("n" + i, List.of("--metricsdb.role=storage", "--metricsdb.node-name=n" + i,
                "--metricsdb.storage.maintenance-interval=2s"), dir);
    }

    private Process spawn(String name, List<String> args, Path dir) throws IOException {
        Files.createDirectories(dir);
        List<String> cmd = new ArrayList<>(List.of(java, "-Xmx384m", "-XX:+UseSerialGC", "-jar", jar,
                "--server.port=" + (name.equals("router") ? basePort : nodePort(Integer.parseInt(name.substring(1)))),
                "--metricsdb.data-dir=" + dir.resolve("data")));
        cmd.addAll(args);
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(work.resolve(name + ".log").toFile()));
        return pb.start();
    }

    public void waitHealthy(String url) throws Exception {
        long until = System.currentTimeMillis() + 90_000;
        while (System.currentTimeMillis() < until) {
            try {
                HttpResponse<String> r = Queries.HTTP.send(HttpRequest.newBuilder(URI.create(url + "/internal/health"))
                        .timeout(Duration.ofSeconds(1)).GET().build(), HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() == 200) return;
            } catch (Exception ignored) {
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException(url + " did not become healthy");
    }

    /** SIGKILL: no shutdown hooks, no flush. */
    public void kill(int i) throws InterruptedException {
        nodes[i].destroyForcibly();
        nodes[i].waitFor();
    }

    public void pause(int i) throws Exception { signal(i, "STOP"); }
    public void resume(int i) throws Exception { signal(i, "CONT"); }

    private void signal(int i, String sig) throws Exception {
        new ProcessBuilder("kill", "-" + sig, String.valueOf(nodes[i].pid())).inheritIO().start().waitFor();
    }

    public boolean alive(int i) { return nodes[i] != null && nodes[i].isAlive(); }

    public String post(String url, String body) throws Exception {
        HttpResponse<String> r = Queries.HTTP.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMinutes(5))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        return r.body();
    }

    public String get(String url) throws Exception {
        HttpResponse<String> r = Queries.HTTP.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMinutes(5))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        return r.body();
    }

    @Override public void close() {
        for (FaultProxy p : proxies) if (p != null) p.close();
        if (router != null) router.destroyForcibly();
        for (Process p : nodes) if (p != null) {
            try { new ProcessBuilder("kill", "-CONT", String.valueOf(p.pid())).start().waitFor(); } catch (Exception ignored) { }
            p.destroyForcibly();
        }
    }

    static void deleteTree(Path p) throws IOException {
        if (!Files.exists(p)) return;
        try (Stream<Path> s = Files.walk(p)) {
            for (Path x : s.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(x);
        }
    }
}
