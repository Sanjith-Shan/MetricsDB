package io.metricsdb.bench;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A TCP proxy on one router-to-node link that can drop the link: when dropped, open connections
 * are reset and new ones are accepted and immediately closed, which the router sees as a node
 * that stops answering mid-request.
 */
public final class FaultProxy implements AutoCloseable {
    private final ServerSocket server;
    private final String host;
    private final int port;
    private volatile boolean dropped;
    private volatile boolean closed;
    private final Set<Socket> open = ConcurrentHashMap.newKeySet();

    public FaultProxy(int listenPort, String host, int port) throws IOException {
        this.server = new ServerSocket(listenPort, 128, InetAddress.getLoopbackAddress());
        this.host = host;
        this.port = port;
        Thread.ofPlatform().daemon(true).name("proxy-" + listenPort).start(this::acceptLoop);
    }

    public void drop() {
        dropped = true;
        for (Socket s : open) closeQuietly(s);
        open.clear();
    }

    public void heal() { dropped = false; }

    public boolean isDropped() { return dropped; }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket client = server.accept();
                if (dropped) { closeQuietly(client); continue; }
                Socket upstream;
                try {
                    upstream = new Socket(host, port);
                } catch (IOException e) {
                    closeQuietly(client);
                    continue;
                }
                open.add(client);
                open.add(upstream);
                Thread.ofVirtual().start(() -> pump(client, upstream));
                Thread.ofVirtual().start(() -> pump(upstream, client));
            } catch (IOException e) {
                if (closed) return;
            }
        }
    }

    private void pump(Socket from, Socket to) {
        byte[] buf = new byte[1 << 16];
        try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
            int r;
            while ((r = in.read(buf)) > 0) {
                if (dropped) break;
                out.write(buf, 0, r);
            }
        } catch (IOException ignored) {
        } finally {
            closeQuietly(from);
            closeQuietly(to);
            open.remove(from);
            open.remove(to);
        }
    }

    private static void closeQuietly(Socket s) {
        try { s.close(); } catch (IOException ignored) { }
    }

    @Override public void close() {
        closed = true;
        drop();
        try { server.close(); } catch (IOException ignored) { }
    }
}
