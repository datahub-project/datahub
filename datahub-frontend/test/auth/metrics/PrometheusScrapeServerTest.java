package auth.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
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
