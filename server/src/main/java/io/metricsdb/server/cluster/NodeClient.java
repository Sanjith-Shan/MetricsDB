package io.metricsdb.server.cluster;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;

/** HTTP client for one storage node's internal API. */
public final class NodeClient {
    public final String name;
    public final String baseUrl;
    private final HttpClient http;
    private final Duration timeout;

    private static final HttpClient SHARED = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(1))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();

    public NodeClient(String name, String baseUrl, Duration timeout) {
        this.name = name;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.http = SHARED;
        this.timeout = timeout;
    }

    public static final class NodeException extends IOException {
        public final int status;
        public NodeException(String msg, int status) {
            super(msg);
            this.status = status;
        }
    }

    public byte[] post(String path, byte[] body) throws IOException {
        return post(path, body, timeout);
    }

    public byte[] post(String path, byte[] body, Duration t) throws IOException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(t)
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        return send(req);
    }

    public byte[] get(String path, Duration t) throws IOException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(t).GET().build();
        return send(req);
    }

    private byte[] send(HttpRequest req) throws IOException {
        try {
            HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() / 100 != 2) {
                String msg = new String(resp.body(), StandardCharsets.UTF_8);
                throw new NodeException(name + " returned " + resp.statusCode() + ": " + msg, resp.statusCode());
            }
            return resp.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }
}
