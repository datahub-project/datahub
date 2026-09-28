package auth.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import controllers.ProxyAdmission;
import health.FrontendProbeState;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class PrometheusScrapeServerTest {

  @AfterEach
  void tearDown() {
    PrometheusScrapeServer.managementServerPortEnvOverrideForTests = null;
    PrometheusScrapeServer.stopActiveScrapeServerForTests();
    FrontendProbeState.resetForTests();
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @Test
  void getActuatorPrometheus_returnsPrometheusText() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    registry.counter("test.frontend.scrape", "env", "unit").increment();

    int port = freePort();
    HttpServer server = PrometheusScrapeServer.createAndStartForTests(registry, port);
    try {
      URL url = new URL("http://127.0.0.1:" + port + "/actuator/prometheus");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      assertEquals(200, conn.getResponseCode());
      assertTrue(
          conn.getContentType() != null && conn.getContentType().startsWith("text/plain"),
          "content-type: " + conn.getContentType());
      String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(
          body.contains("test_frontend_scrape"),
          "Expected Micrometer counter in scrape body: " + body);
    } finally {
      server.stop(0);
    }
  }

  @Test
  void healthLiveStays200WhileReadyReflectsShutdownAndSaturation() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    registry.counter("test.frontend.scrape", "env", "unit").increment();
    int port = freePort();
    HttpServer server = PrometheusScrapeServer.createAndStartForTests(registry, port);
    ProxyAdmission admission = new ProxyAdmission(10, null);
    try {
      FrontendProbeState.bind(admission, () -> false);
      assertEquals(200, getStatus(port, "/health/live"));
      assertEquals("LIVE", getBody(port, "/health/live"));
      assertEquals(200, getStatus(port, "/health/ready"));

      for (int i = 0; i < 8; i++) {
        assertTrue(admission.tryAcquire());
      }
      assertEquals(200, getStatus(port, "/health/live"));
      assertEquals(503, getStatus(port, "/health/ready"));
      assertEquals("Saturated", getBody(port, "/health/ready"));

      int releases = 0;
      while (!admission.isAcceptingTraffic() && releases < 10) {
        admission.release();
        releases++;
      }
      assertTrue(admission.isAcceptingTraffic());
      assertEquals(200, getStatus(port, "/health/ready"));

      FrontendProbeState.bind(admission, () -> true);
      assertEquals(200, getStatus(port, "/health/live"));
      assertEquals(503, getStatus(port, "/health/ready"));
      assertEquals("Shutting down", getBody(port, "/health/ready"));

      assertEquals(200, getStatus(port, "/actuator/prometheus"));
      assertTrue(getBody(port, "/actuator/prometheus").contains("test_frontend_scrape"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void healthReady_beforeBind_returnsStarting() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    int port = freePort();
    HttpServer server = PrometheusScrapeServer.createAndStartForTests(registry, port);
    try {
      FrontendProbeState.resetForTests();
      assertEquals(200, getStatus(port, "/health/live"));
      assertEquals(503, getStatus(port, "/health/ready"));
      assertEquals("Starting", getBody(port, "/health/ready"));
    } finally {
      server.stop(0);
    }
  }

  private static int getStatus(int port, String path) throws Exception {
    HttpURLConnection conn = open(port, path);
    return conn.getResponseCode();
  }

  private static String getBody(int port, String path) throws Exception {
    HttpURLConnection conn = open(port, path);
    int status = conn.getResponseCode();
    java.io.InputStream stream = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
    if (stream == null) {
      return "";
    }
    return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
  }

  private static HttpURLConnection open(int port, String path) throws Exception {
    URL url = new URL("http://127.0.0.1:" + port + path);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    return conn;
  }

  @Test
  void postActuatorPrometheus_returns405() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    int port = freePort();
    HttpServer server = PrometheusScrapeServer.createAndStartForTests(registry, port);
    try {
      URL url = new URL("http://127.0.0.1:" + port + "/actuator/prometheus");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      assertEquals(405, conn.getResponseCode());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void getUnknownPath_returns404() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    int port = freePort();
    HttpServer server = PrometheusScrapeServer.createAndStartForTests(registry, port);
    try {
      URL url = new URL("http://127.0.0.1:" + port + "/actuator/health");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      assertEquals(404, conn.getResponseCode());
    } finally {
      server.stop(0);
    }
  }

  @Test
  @ClearEnvironmentVariable(key = "MANAGEMENT_SERVER_PORT")
  void startIfConfigured_missingEnvVar_isNoOp() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    PrometheusScrapeServer.startIfConfigured(registry);
  }

  @Test
  @SetEnvironmentVariable(key = "MANAGEMENT_SERVER_PORT", value = "   ")
  void startIfConfigured_blankEnvVar_isNoOp() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    PrometheusScrapeServer.startIfConfigured(registry);
  }

  @Test
  @SetEnvironmentVariable(key = "MANAGEMENT_SERVER_PORT", value = "not-a-port")
  void startIfConfigured_invalidPort_isNoOp() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    PrometheusScrapeServer.startIfConfigured(registry);
  }

  @Test
  @SetEnvironmentVariable(key = "MANAGEMENT_SERVER_PORT", value = "0")
  void startIfConfigured_portZero_bindsEphemeralAndServesGet() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    registry.counter("start_if_configured.test").increment();
    PrometheusScrapeServer.startIfConfigured(registry);
    try {
      int localPort = PrometheusScrapeServer.activeScrapeServerPortForTests();
      URL url = new URL("http://127.0.0.1:" + localPort + "/actuator/prometheus");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      assertEquals(200, conn.getResponseCode());
      String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(body.contains("start_if_configured_test"), body);
    } finally {
      PrometheusScrapeServer.stopActiveScrapeServerForTests();
    }
  }

  @Test
  void startIfConfigured_portInUse_catchesIOException() throws Exception {
    PrometheusMeterRegistry blockerRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    int port = freePort();
    HttpServer blocker = PrometheusScrapeServer.createAndStartForTests(blockerRegistry, port);
    try {
      PrometheusMeterRegistry second = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      PrometheusScrapeServer.managementServerPortEnvOverrideForTests = String.valueOf(port);
      PrometheusScrapeServer.startIfConfigured(second);
    } finally {
      blocker.stop(0);
    }
  }
}
