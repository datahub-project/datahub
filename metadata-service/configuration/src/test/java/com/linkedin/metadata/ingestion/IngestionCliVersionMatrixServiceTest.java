package com.linkedin.metadata.ingestion;

import static org.testng.Assert.*;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class IngestionCliVersionMatrixServiceTest {

  private static final String SERVER_VERSION = "1.3.1.4";

  // JSON matching the Notion-documented schema. Server version "1.3.1.4" has snowflake (with two
  // cohorts) and bigquery (_default only); server version "2.0.0" is intentionally absent so we
  // can exercise the "unknown server" path.
  private static final String MATRIX_JSON =
      "{\n"
          + "  \"1.3.1.4\": {\n"
          + "    \"snowflake\": {\n"
          + "      \"_default\": \"1.3.1.4\",\n"
          + "      \"cohorts\": [\n"
          + "        {\n"
          + "          \"version\": \"1.3.1.5\",\n"
          + "          \"deployments\": [\"deployment-b1\", \"deployment-b2\", \"deployment-b3\"]\n"
          + "        },\n"
          + "        {\n"
          + "          \"version\": \"1.3.1.6\",\n"
          + "          \"deployments\": [\"deployment-d1\", \"deployment-d2\"]\n"
          + "        }\n"
          + "      ]\n"
          + "    },\n"
          + "    \"bigquery\": {\n"
          + "      \"_default\": \"0.14.2\"\n"
          + "    }\n"
          + "  }\n"
          + "}";

  /** Servers started by {@link #startMatrixServer(String)}; stopped after each test method. */
  private final List<HttpServer> serversToStop = new ArrayList<>();

  @AfterMethod
  public void stopServers() {
    for (HttpServer s : serversToStop) {
      try {
        s.stop(0);
      } catch (Exception ignored) {
      }
    }
    serversToStop.clear();
  }

  /**
   * Spins up a one-shot embedded {@link HttpServer} on a random localhost port that serves {@code
   * matrixJson} at {@code /matrix}. The server is tracked and stopped by {@link #stopServers()}
   * after each test. Returns the URL to point a matrix source at.
   */
  private String startMatrixServer(String matrixJson) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/matrix",
        exchange -> {
          byte[] body = matrixJson.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    serversToStop.add(server);
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/matrix";
  }

  /**
   * Returns a service backed by {@link HttpUrlIngestionCliVersionMatrixSource} pointed at an
   * embedded HTTP server serving {@link #MATRIX_JSON}. Polls briefly so the asynchronous initial
   * fetch has a chance to populate the cache before the assertions run.
   */
  private IngestionCliVersionMatrixService serviceWithMatrix(
      String serverVersion, String deploymentId) throws IOException {
    return serviceWithMatrix(MATRIX_JSON, serverVersion, deploymentId);
  }

  private IngestionCliVersionMatrixService serviceWithMatrix(
      String matrixJson, String serverVersion, String deploymentId) throws IOException {
    HttpUrlIngestionCliVersionMatrixSource httpSource =
        new HttpUrlIngestionCliVersionMatrixSource(startMatrixServer(matrixJson), 3600);
    IngestionCliVersionMatrixService svc =
        new IngestionCliVersionMatrixService(httpSource, serverVersion, deploymentId);

    // Wait briefly for the initial fetch to complete (delay=0 in the scheduled executor).
    for (int i = 0; i < 20; i++) {
      if (httpSource.getLastFetchedAtMillis() > 0) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
      }
    }
    return svc;
  }

  // -------------------------------------------------------------------------
  // Feature disabled (NoOpIngestionCliVersionMatrixSource — what the factory binds when URL is
  // unset)
  // -------------------------------------------------------------------------

  @Test
  public void testDisabled_noOpSource() {
    IngestionCliVersionMatrixService svc =
        new IngestionCliVersionMatrixService(
            new NoOpIngestionCliVersionMatrixSource(), SERVER_VERSION, "deployment-b1");
    assertEquals(svc.resolveVersion("snowflake"), Optional.empty());
  }

  // -------------------------------------------------------------------------
  // Connector default (no cohort match)
  // -------------------------------------------------------------------------

  @Test
  public void testCustomerNotInAnyCohort_usesConnectorDefault() throws Exception {
    IngestionCliVersionMatrixService svc = serviceWithMatrix(SERVER_VERSION, "deployment-unknown");
    assertEquals(svc.resolveVersion("snowflake"), Optional.of("1.3.1.4"));
  }

  @Test
  public void testConnectorWithoutCohorts_returnsDefault() throws Exception {
    // bigquery is in the matrix with only a `default` and no `cohorts` array.
    IngestionCliVersionMatrixService svc = serviceWithMatrix(SERVER_VERSION, "deployment-b1");
    assertEquals(svc.resolveVersion("bigquery"), Optional.of("0.14.2"));
  }

  // -------------------------------------------------------------------------
  // Cohort match
  // -------------------------------------------------------------------------

  @Test
  public void testCustomerInFirstCohort_getsCohortVersion() throws Exception {
    IngestionCliVersionMatrixService svc = serviceWithMatrix(SERVER_VERSION, "deployment-b2");
    assertEquals(svc.resolveVersion("snowflake"), Optional.of("1.3.1.5"));
  }

  @Test
  public void testCustomerInSecondCohort_getsCohortVersion() throws Exception {
    IngestionCliVersionMatrixService svc = serviceWithMatrix(SERVER_VERSION, "deployment-d1");
    assertEquals(svc.resolveVersion("snowflake"), Optional.of("1.3.1.6"));
  }

  // -------------------------------------------------------------------------
  // Missing deployment identity
  // -------------------------------------------------------------------------

  @Test
  public void testNullCustomerId_neverMatchesCohort() throws Exception {
    // A deployment that didn't wire DATAHUB_EXECUTOR_CUSTOMER_ID should fall through to the
    // connector default rather than throw or behave unpredictably.
    IngestionCliVersionMatrixService svc = serviceWithMatrix(SERVER_VERSION, null);
    assertEquals(svc.resolveVersion("snowflake"), Optional.of("1.3.1.4"));
  }

  @Test
  public void testEmptyCustomerId_neverMatchesCohort() throws Exception {
    IngestionCliVersionMatrixService svc = serviceWithMatrix(SERVER_VERSION, "");
    assertEquals(svc.resolveVersion("snowflake"), Optional.of("1.3.1.4"));
  }

  // -------------------------------------------------------------------------
  // Misses
  // -------------------------------------------------------------------------

  @Test
  public void testUnknownConnector_returnsEmpty() throws Exception {
    // Connectors not present in the matrix should return empty so the caller falls back to the
    // application-wide defaultCliVersion. This is intentionally different from the old `_default`
    // server-level fallback the prior schema had.
    IngestionCliVersionMatrixService svc = serviceWithMatrix(SERVER_VERSION, "deployment-b1");
    assertEquals(svc.resolveVersion("redshift"), Optional.empty());
  }

  @Test
  public void testUnknownServerVersion_returnsEmpty() throws Exception {
    IngestionCliVersionMatrixService svc = serviceWithMatrix("2.0.0", "deployment-b1");
    assertEquals(svc.resolveVersion("snowflake"), Optional.empty());
  }

  // -------------------------------------------------------------------------
  // Fetch failure / malformed JSON
  // -------------------------------------------------------------------------

  @Test
  public void testUnreachableUrlReturnsEmpty() {
    // A URL that will always fail — the source should log a warning and the service returns empty
    // (no prior matrix to fall back to).
    HttpUrlIngestionCliVersionMatrixSource httpSource =
        new HttpUrlIngestionCliVersionMatrixSource("http://localhost:19999/does-not-exist", 3600);
    IngestionCliVersionMatrixService svc =
        new IngestionCliVersionMatrixService(httpSource, SERVER_VERSION, "deployment-b1");

    try {
      Thread.sleep(200);
    } catch (InterruptedException ignored) {
    }

    assertEquals(svc.resolveVersion("snowflake"), Optional.empty());
  }

  @Test
  public void testMalformedJsonReturnsEmpty() throws Exception {
    HttpUrlIngestionCliVersionMatrixSource httpSource =
        new HttpUrlIngestionCliVersionMatrixSource(startMatrixServer("not valid json"), 3600);
    IngestionCliVersionMatrixService svc =
        new IngestionCliVersionMatrixService(httpSource, SERVER_VERSION, "deployment-b1");

    try {
      Thread.sleep(300);
    } catch (InterruptedException ignored) {
    }

    assertEquals(svc.resolveVersion("snowflake"), Optional.empty());
  }

  // -------------------------------------------------------------------------
  // Cohort entries with missing fields
  // -------------------------------------------------------------------------

  @Test
  public void testCohortWithoutVersion_isSkipped() throws Exception {
    // A cohort missing a version should be skipped — we should not silently use the deployment's
    // first deployments-list hit to mean "_default". Falling back to the connector _default is the
    // safe behavior.
    String json =
        "{\n"
            + "  \"1.3.1.4\": {\n"
            + "    \"snowflake\": {\n"
            + "      \"_default\": \"1.3.1.4\",\n"
            + "      \"cohorts\": [\n"
            + "        { \"deployments\": [\"deployment-b1\"] },\n"
            + "        { \"version\": \"1.3.1.6\", \"deployments\": [\"deployment-d1\"] }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    HttpUrlIngestionCliVersionMatrixSource httpSource =
        new HttpUrlIngestionCliVersionMatrixSource(startMatrixServer(json), 3600);
    IngestionCliVersionMatrixService svc =
        new IngestionCliVersionMatrixService(httpSource, SERVER_VERSION, "deployment-b1");

    for (int i = 0; i < 20; i++) {
      Optional<String> v = svc.resolveVersion("snowflake");
      if (v.isPresent()) {
        // deployment-b1 is in the malformed cohort's deployments, but that cohort lacks a
        // `version`. Should fall through to the connector _default rather than match the bad
        // cohort.
        assertEquals(v, Optional.of("1.3.1.4"));
        return;
      }
      Thread.sleep(50);
    }
    fail("Matrix never loaded");
  }

  @Test
  public void testCohortWithMissingDeployments_neverMatches() throws Exception {
    // No deployments field at all → no deployment should ever match this cohort, even one that
    // "looks empty". Use the _default.
    String json =
        "{\n"
            + "  \"1.3.1.4\": {\n"
            + "    \"snowflake\": {\n"
            + "      \"_default\": \"1.3.1.4\",\n"
            + "      \"cohorts\": [\n"
            + "        { \"version\": \"1.3.1.5\" }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    HttpUrlIngestionCliVersionMatrixSource httpSource =
        new HttpUrlIngestionCliVersionMatrixSource(startMatrixServer(json), 3600);
    IngestionCliVersionMatrixService svc =
        new IngestionCliVersionMatrixService(httpSource, SERVER_VERSION, "deployment-b1");

    for (int i = 0; i < 20; i++) {
      Optional<String> v = svc.resolveVersion("snowflake");
      if (v.isPresent()) {
        assertEquals(v, Optional.of("1.3.1.4"));
        return;
      }
      Thread.sleep(50);
    }
    fail("Matrix never loaded");
  }

  // -------------------------------------------------------------------------
  // HttpUrlIngestionCliVersionMatrixSource HTTP-level behavior (auth header, fetch-failure cache
  // retention). These tests inspect request headers and response status codes that the simpler
  // serviceWithMatrix() helper does not expose.
  // -------------------------------------------------------------------------

  @Test
  public void testAuthHeader_sentVerbatimWhenConfigured() throws Exception {
    AtomicReference<String> captured = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/matrix",
        exchange -> {
          captured.set(exchange.getRequestHeaders().getFirst("Authorization"));
          byte[] body = MATRIX_JSON.getBytes();
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/matrix";
      HttpUrlIngestionCliVersionMatrixSource source =
          new HttpUrlIngestionCliVersionMatrixSource(url, 3600, "token ghp_test_xyz");
      waitForFirstFetch(source);
      assertEquals(
          captured.get(),
          "token ghp_test_xyz",
          "Authorization header should be sent verbatim when authHeader is configured");
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testAuthHeader_omittedWhenUnset() throws Exception {
    AtomicBoolean sawAuthHeader = new AtomicBoolean(false);
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/matrix",
        exchange -> {
          sawAuthHeader.set(exchange.getRequestHeaders().containsKey("Authorization"));
          byte[] body = MATRIX_JSON.getBytes();
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/matrix";
      // 2-arg constructor — no authHeader.
      HttpUrlIngestionCliVersionMatrixSource source =
          new HttpUrlIngestionCliVersionMatrixSource(url, 3600);
      waitForFirstFetch(source);
      assertFalse(
          sawAuthHeader.get(),
          "Authorization header should NOT be sent when authHeader is null (public-URL semantics)");
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testFetchFailureAfterSuccess_retainsCachedMatrix() throws Exception {
    AtomicInteger callCount = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/matrix",
        exchange -> {
          int n = callCount.incrementAndGet();
          if (n == 1) {
            byte[] body = MATRIX_JSON.getBytes();
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(body);
            }
          } else {
            // 500 on subsequent refreshes. The source should keep its previously cached value.
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
          }
        });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/matrix";
      HttpUrlIngestionCliVersionMatrixSource source =
          new HttpUrlIngestionCliVersionMatrixSource(url, 3600);
      waitForFirstFetch(source);

      IngestionCliVersionMatrix beforeFailure = source.getMatrix();
      long firstFetchTimestamp = source.getLastFetchedAtMillis();
      assertTrue(firstFetchTimestamp > 0, "Initial fetch should have populated the cache");

      // Force a second refresh — the server now returns 500.
      source.refresh();

      assertSame(
          source.getMatrix(),
          beforeFailure,
          "Matrix cache should retain the previously fetched value when a refresh fails");
      assertEquals(
          source.getLastFetchedAtMillis(),
          firstFetchTimestamp,
          "lastFetchedAtMillis should not advance on a failed refresh");
    } finally {
      server.stop(0);
    }
  }

  /** Polls until the source's initial scheduled fetch has populated the cache, or fails fast. */
  private static void waitForFirstFetch(HttpUrlIngestionCliVersionMatrixSource source)
      throws InterruptedException {
    for (int i = 0; i < 30; i++) {
      if (source.getLastFetchedAtMillis() > 0) {
        return;
      }
      Thread.sleep(50);
    }
    fail("Initial matrix fetch did not complete within 1.5s");
  }

  @Test
  public void testAuthHeader_omittedWhenEmptyString() throws Exception {
    AtomicBoolean sawAuthHeader = new AtomicBoolean(false);
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/matrix",
        exchange -> {
          sawAuthHeader.set(exchange.getRequestHeaders().containsKey("Authorization"));
          byte[] body = MATRIX_JSON.getBytes();
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/matrix";
      // Explicit empty string — distinct branch from null in
      // HttpUrlIngestionCliVersionMatrixSource.refresh().
      HttpUrlIngestionCliVersionMatrixSource source =
          new HttpUrlIngestionCliVersionMatrixSource(url, 3600, "");
      waitForFirstFetch(source);
      assertFalse(
          sawAuthHeader.get(),
          "Empty-string authHeader should be treated like null — no Authorization header sent");
    } finally {
      server.stop(0);
    }
  }

  // -------------------------------------------------------------------------
  // POJO direct construction — covers the null-input defensive branches that the
  // HTTP parser never exercises (it always passes non-null empty collections).
  // -------------------------------------------------------------------------

  @Test
  public void testNoOpIngestionCliVersionMatrixSource_returnsEmptyAndZeroTimestamp() {
    NoOpIngestionCliVersionMatrixSource source = new NoOpIngestionCliVersionMatrixSource();
    assertSame(
        source.getMatrix(),
        IngestionCliVersionMatrix.EMPTY,
        "NoOpIngestionCliVersionMatrixSource should always return IngestionCliVersionMatrix.EMPTY");
    assertEquals(
        source.getLastFetchedAtMillis(),
        0L,
        "NoOpIngestionCliVersionMatrixSource should report 0 for last-fetched timestamp");
  }

  @Test
  public void testCohort_nullDeploymentsBecomesEmpty() {
    Cohort cohort = new Cohort("1.5.0.20", null);
    assertEquals(cohort.getVersion(), "1.5.0.20");
    assertTrue(
        cohort.getDeployments().isEmpty(),
        "null deployments should default to an empty set (defensive null-handling)");
  }

  @Test
  public void testConnectorEntry_nullCohortsBecomesEmpty() {
    ConnectorEntry entry = new ConnectorEntry("1.5.0.14", null);
    assertEquals(entry.getDefaultVersion(), "1.5.0.14");
    assertTrue(
        entry.getCohorts().isEmpty(),
        "null cohorts should default to an empty list (defensive null-handling)");
  }

  @Test
  public void testConnectorWithoutDefault_andNoCohortMatch_returnsEmpty() throws Exception {
    // Connector exists in the matrix but has no `_default` field AND we don't match any cohort.
    // Caller should fall through to application-wide defaultCliVersion.
    String json =
        "{\n"
            + "  \"1.3.1.4\": {\n"
            + "    \"snowflake\": {\n"
            + "      \"cohorts\": [\n"
            + "        { \"version\": \"1.3.1.5\", \"deployments\": [\"deployment-x\"] }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";
    HttpUrlIngestionCliVersionMatrixSource httpSource =
        new HttpUrlIngestionCliVersionMatrixSource(startMatrixServer(json), 3600);
    IngestionCliVersionMatrixService svc =
        new IngestionCliVersionMatrixService(httpSource, SERVER_VERSION, "deployment-unknown");

    for (int i = 0; i < 20; i++) {
      if (httpSource.getLastFetchedAtMillis() > 0) {
        break;
      }
      Thread.sleep(50);
    }
    assertEquals(svc.resolveVersion("snowflake"), Optional.empty());
  }

  // -------------------------------------------------------------------------
  // Thread-factory plumbing — name + daemon flag. Generic `pool-N-thread-1`
  // names are useless in production thread dumps; daemon is belt-and-suspenders
  // with the @PreDestroy shutdown hook in case the hook never fires.
  // -------------------------------------------------------------------------

  @Test
  public void testRefreshThreadIsNamedAndDaemon() throws Exception {
    // Any HTTP endpoint suffices — we're inspecting the refresh thread, not the fetched data.
    HttpUrlIngestionCliVersionMatrixSource source =
        new HttpUrlIngestionCliVersionMatrixSource(startMatrixServer(MATRIX_JSON), 3600);
    try {
      waitForFirstFetch(source);

      // Scan live threads for ours. Using Thread.getAllStackTraces() means we don't need to reach
      // into the executor's internals — we just verify the thread exists with the expected name
      // and is daemon, which is what an operator reading a thread dump would care about.
      Thread refreshThread =
          Thread.getAllStackTraces().keySet().stream()
              .filter(t -> "ingestion-cli-version-matrix-refresh".equals(t.getName()))
              .findFirst()
              .orElse(null);

      assertNotNull(
          refreshThread,
          "Refresh thread should carry a descriptive name (not the default pool-N-thread-1) so it "
              + "is identifiable in thread dumps");
      assertTrue(
          refreshThread.isDaemon(),
          "Refresh thread should be a daemon thread so the JVM can exit cleanly even if "
              + "@PreDestroy is never called (e.g. forced kill, container-runtime edge cases)");
    } finally {
      // Clean shutdown so the daemon thread doesn't linger into the next test.
      source.shutdown();
    }
  }

  // -------------------------------------------------------------------------
  // Shutdown lifecycle — Spring @PreDestroy must terminate the refresh thread.
  // Without this, a redeployment / context restart would leak the
  // single-thread scheduled executor and keep the JVM from exiting cleanly.
  // -------------------------------------------------------------------------

  @Test
  public void testShutdown_stopsBackgroundRefresh() throws Exception {
    AtomicInteger callCount = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/matrix",
        exchange -> {
          callCount.incrementAndGet();
          byte[] body = MATRIX_JSON.getBytes();
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/matrix";
      // 1-second refresh so we can see the cadence without slowing the test suite.
      HttpUrlIngestionCliVersionMatrixSource source =
          new HttpUrlIngestionCliVersionMatrixSource(url, 1);
      waitForFirstFetch(source);

      source.shutdown();
      int callsAtShutdown = callCount.get();

      // Wait well past two refresh intervals; callCount must not move because shutdown stopped
      // the scheduled executor.
      Thread.sleep(2500);
      assertEquals(
          callCount.get(),
          callsAtShutdown,
          "Refresh thread must stop firing after shutdown() — no new HTTP calls expected");
    } finally {
      server.stop(0);
    }
  }
}
