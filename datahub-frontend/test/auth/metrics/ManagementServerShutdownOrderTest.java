package auth.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.typesafe.config.ConfigFactory;
import config.GracefulShutdownModule;
import controllers.ProxyAdmission;
import health.FrontendProbeState;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.pekko.Done;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The management listener must keep answering probes while Play drains, and stop only at the end of
 * coordinated shutdown.
 */
class ManagementServerShutdownOrderTest {

  @AfterEach
  void tearDown() {
    PrometheusScrapeServer.managementServerPortEnvOverrideForTests = null;
    PrometheusScrapeServer.stopActiveScrapeServerForTests();
    FrontendProbeState.resetForTests();
  }

  @Test
  void readyReports503DuringRequestDrainAndListenerStopsAfterShutdown() throws Exception {
    GracefulShutdownModule module = new GracefulShutdownModule();
    int port = startManagementServer(module);
    ActorSystem system = newActorSystem();
    CoordinatedShutdown cs = CoordinatedShutdown.get(system);
    newProvider(cs, module, true).get();
    AtomicInteger readyDuringDrain = probeReadyDuring(cs, port);

    cs.runAll(CoordinatedShutdown.unknownReason()).toCompletableFuture().get(30, TimeUnit.SECONDS);

    assertEquals(503, readyDuringDrain.get());
    assertThrows(ConnectException.class, () -> getStatus(port, "/health/ready"));
  }

  @Test
  void listenerStopsAfterShutdownEvenWhenGracefulShutdownDisabled() throws Exception {
    GracefulShutdownModule module = new GracefulShutdownModule();
    int port = startManagementServer(module);
    ActorSystem system = newActorSystem();
    CoordinatedShutdown cs = CoordinatedShutdown.get(system);
    newProvider(cs, module, false).get();
    AtomicInteger readyDuringDrain = probeReadyDuring(cs, port);

    cs.runAll(CoordinatedShutdown.unknownReason()).toCompletableFuture().get(30, TimeUnit.SECONDS);

    // Disabled: the readiness flag never flips, but the listener still serves until the end.
    assertEquals(200, readyDuringDrain.get());
    assertThrows(ConnectException.class, () -> getStatus(port, "/health/ready"));
  }

  private static int startManagementServer(GracefulShutdownModule module) {
    FrontendProbeState.bind(new ProxyAdmission(10, null), module::isShuttingDown);
    PrometheusScrapeServer.managementServerPortEnvOverrideForTests = "0";
    PrometheusScrapeServer.startIfConfigured(null);
    return PrometheusScrapeServer.activeScrapeServerPortForTests();
  }

  private static ActorSystem newActorSystem() {
    return ActorSystem.create(
        "management-server-shutdown-order",
        ConfigFactory.parseString("pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off")
            .withFallback(ConfigFactory.defaultReference()));
  }

  private static GracefulShutdownModule.FrontendShutdownHookProvider newProvider(
      CoordinatedShutdown cs, GracefulShutdownModule module, boolean enabled) {
    return new GracefulShutdownModule.FrontendShutdownHookProvider(
        ConfigFactory.parseString("frontend.graceful_shutdown_enabled = " + enabled),
        cs,
        mock(CloseableHttpClient.class),
        module);
  }

  /** Records the /health/ready status while Play would be draining requests. */
  private static AtomicInteger probeReadyDuring(CoordinatedShutdown cs, int port) {
    AtomicInteger status = new AtomicInteger(-1);
    cs.addTask(
        CoordinatedShutdown.PhaseServiceRequestsDone(),
        "probe-ready-during-drain",
        () ->
            CompletableFuture.supplyAsync(
                () -> {
                  try {
                    status.set(getStatus(port, "/health/ready"));
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                  return Done.done();
                }));
    return status;
  }

  private static int getStatus(int port, String path) throws IOException {
    HttpURLConnection conn =
        (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
    conn.setRequestMethod("GET");
    try {
      return conn.getResponseCode();
    } finally {
      conn.disconnect();
    }
  }
}
