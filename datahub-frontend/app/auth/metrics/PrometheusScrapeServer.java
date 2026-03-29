package auth.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

/**
 * Minimal HTTP listener for Micrometer Prometheus scrape (parity with Spring Actuator on :4319).
 */
@Slf4j
public final class PrometheusScrapeServer {

  /** Last server started by {@link #startIfConfigured}; cleared when stopped. For tests only. */
  private static volatile HttpServer activeScrapeServer;

  /**
   * When non-null, used instead of {@code System.getenv("MANAGEMENT_SERVER_PORT")}. For unit tests
   * only.
   */
  static volatile String managementServerPortEnvOverrideForTests;

  private PrometheusScrapeServer() {}

  /** Stops the server bound by {@link #startIfConfigured}, if any. For unit tests only. */
  static void stopActiveScrapeServerForTests() {
    HttpServer server = activeScrapeServer;
    activeScrapeServer = null;
    if (server != null) {
      server.stop(0);
    }
  }

  /**
   * Bound listen port after {@link #startIfConfigured} (e.g. when env was {@code 0}). For tests.
   */
  static int activeScrapeServerPortForTests() {
    HttpServer server = activeScrapeServer;
    if (server == null) {
      throw new IllegalStateException("No Micrometer Prometheus scrape server is active");
    }
    return ((InetSocketAddress) server.getAddress()).getPort();
  }

  /**
   * If {@code MANAGEMENT_SERVER_PORT} is set, binds {@code 0.0.0.0}:{port} and serves {@code GET
   * /actuator/prometheus}. Stopped on JVM shutdown.
   */
  public static void startIfConfigured(PrometheusMeterRegistry prometheusRegistry) {
    String portStr =
        managementServerPortEnvOverrideForTests != null
            ? managementServerPortEnvOverrideForTests
            : System.getenv("MANAGEMENT_SERVER_PORT");
    if (portStr == null || portStr.isBlank()) {
      return;
    }
    int port;
    try {
      port = Integer.parseInt(portStr.trim());
    } catch (NumberFormatException e) {
      log.warn("Invalid MANAGEMENT_SERVER_PORT: {}", portStr);
      return;
    }
    try {
      HttpServer server = createAndStart(prometheusRegistry, port);
      activeScrapeServer = server;
      log.info(
          "Micrometer Prometheus scrape endpoint at http://0.0.0.0:{}/actuator/prometheus", port);
      Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));
    } catch (IOException e) {
      log.error("Failed to bind Micrometer Prometheus scrape server on port {}", port, e);
    }
  }

  /**
   * Starts the scrape listener on {@code 0.0.0.0}:{@code port}. For unit tests only; caller must
   * {@link HttpServer#stop(int)}.
   */
  static HttpServer createAndStartForTests(PrometheusMeterRegistry prometheusRegistry, int port)
      throws IOException {
    return createAndStart(prometheusRegistry, port);
  }

  private static HttpServer createAndStart(PrometheusMeterRegistry prometheusRegistry, int port)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    server.createContext(
        "/actuator/prometheus",
        exchange -> {
          if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
          }
          byte[] body = prometheusRegistry.scrape().getBytes(StandardCharsets.UTF_8);
          exchange
              .getResponseHeaders()
              .add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.setExecutor(
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "prometheus-scrape");
              t.setDaemon(true);
              return t;
            }));
    server.start();
    return server;
  }
}
