package auth.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import health.FrontendProbeState;
import health.FrontendProbeState.Readiness;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * Minimal HTTP listener for Micrometer Prometheus scrape and Kubernetes probes (parity with Spring
 * Actuator on :4319).
 *
 * <p>This listener has its own thread pool and does not share the Play/Pekko connection table, so
 * {@code /health/live} and {@code /health/ready} can still respond when Play is saturated. Paths
 * are not prefixed with {@code DATAHUB_BASE_PATH}.
 */
@Slf4j
public final class PrometheusScrapeServer {

  /**
   * In-flight {@code /health/live} and {@code /health/ready} calls on this listener. Play's {@code
   * play_http_requests_inflight} counts {@code /admin} and {@code /health} on port 9002; these
   * probes are not in that chain.
   */
  static final String HEALTH_INFLIGHT = "frontend_management_health_inflight";

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
   * /actuator/prometheus}, {@code GET /health/live}, and {@code GET /health/ready}. Stopped on JVM
   * shutdown.
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
          "Management endpoints at http://0.0.0.0:{} (/actuator/prometheus, /health/live, /health/ready)",
          port);
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
    AtomicInteger healthInFlight = new AtomicInteger();
    if (prometheusRegistry != null) {
      Gauge.builder(HEALTH_INFLIGHT, healthInFlight, AtomicInteger::get)
          .description(
              "In-flight /health/live and /health/ready on the management listener, not the Play filter chain")
          .strongReference(true)
          .register(prometheusRegistry);
    }
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
    server.createContext(
        "/health/live",
        exchange -> track(healthInFlight, exchange, ex -> sendPlainText(ex, 200, "LIVE")));
    server.createContext(
        "/health/ready",
        exchange ->
            track(
                healthInFlight,
                exchange,
                ex -> {
                  Readiness readiness = FrontendProbeState.readiness();
                  if (readiness == Readiness.READY) {
                    sendPlainText(ex, 200, "READY");
                  } else {
                    sendPlainText(ex, 503, readinessBody(readiness));
                  }
                }));
    // A scrape must not block a probe, so this is not a single thread.
    ExecutorService executor =
        Executors.newFixedThreadPool(
            2,
            r -> {
              Thread t = new Thread(r, "frontend-management");
              t.setDaemon(true);
              return t;
            });
    server.setExecutor(executor);
    server.start();
    return server;
  }

  private static void track(AtomicInteger inFlight, HttpExchange exchange, HealthHandler handler)
      throws IOException {
    inFlight.incrementAndGet();
    try {
      handler.handle(exchange);
    } finally {
      inFlight.decrementAndGet();
    }
  }

  @FunctionalInterface
  private interface HealthHandler {
    void handle(HttpExchange exchange) throws IOException;
  }

  private static String readinessBody(Readiness readiness) {
    return switch (readiness) {
      case SHUTTING_DOWN -> "Shutting down";
      case SATURATED -> "Saturated";
      case STARTING, READY -> "Starting";
    };
  }

  private static void sendPlainText(HttpExchange exchange, int status, String body)
      throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      exchange.sendResponseHeaders(405, -1);
      exchange.close();
      return;
    }
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }
}
