package config;

import auth.metrics.PrometheusScrapeServer;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import controllers.ProxyAdmission;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.pekko.Done;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guice module managing graceful shutdown for the DataHub Frontend pod.
 *
 * <p>When enabled via FRONTEND_GRACEFUL_SHUTDOWN_ENABLED, this module coordinates with Pekko's
 * CoordinatedShutdown to gracefully handle Kubernetes pod termination (e.g. spot reclaim):
 *
 * <ol>
 *   <li>Kubernetes marks the pod Terminating and starts removing it from Service endpoints. The
 *       preStop hook (a sleep) runs first, so the load balancer stops sending new traffic while the
 *       app is still serving. The terminationGracePeriodSeconds countdown already includes preStop.
 *   <li>After preStop, the container gets SIGTERM and Pekko CoordinatedShutdown runs the phases
 *       below.
 *   <li>before-service-unbind: FrontendShutdownHook flips isShuttingDown, so {@code /admin}, {@code
 *       /health} and management-port {@code /health/ready} return 503, and {@link
 *       filters.ShutdownConnectionCloseFilter} adds {@code Connection: close} to every response so
 *       upstream keep-alive pools (which kube-proxy updates do not affect) move to other pods.
 *   <li>service-unbind: Play stops accepting new connections.
 *   <li>service-requests-done: Play waits {@code play.server.waitBeforeTermination} (Pekko HTTP
 *       does not track streamed response bodies during termination, so this gives SSE and other
 *       streamed proxy responses time to finish), then terminates remaining connections within
 *       {@code play.server.terminationTimeout}. The two must fit inside the phase timeout.
 *   <li>service-stop: close the HTTP client used for GMS calls.
 *   <li>before-actor-system-terminate: stop the management listener, so probes and metrics answer
 *       for the whole drain. This step runs whether or not graceful shutdown is enabled.
 *   <li>SIGKILL arrives at terminationGracePeriodSeconds if the JVM has not exited.
 * </ol>
 *
 * <p>Thread-safe via AtomicBoolean to prevent race conditions between shutdown hook and concurrent
 * request handling.
 *
 * <p>Disabled by default (FRONTEND_GRACEFUL_SHUTDOWN_ENABLED=false) for backward compatibility.
 * When disabled, a no-op FrontendShutdownHook is instantiated, ensuring graceful shutdown can be
 * toggled without code changes.
 */
@Singleton
public class GracefulShutdownModule extends AbstractModule {

  /**
   * Flag indicating if the service is shutting down.
   *
   * <p>Set to true by FrontendShutdownHook during the before-service-unbind phase of Pekko
   * CoordinatedShutdown. Read by the health endpoints to return 503 responses.
   *
   * <p>Uses AtomicBoolean for thread-safe reads/writes from concurrent request handlers and the
   * shutdown hook.
   */
  private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

  @Override
  protected void configure() {
    bind(GracefulShutdownModule.class).toInstance(this);
    bind(FrontendShutdownHook.class)
        .toProvider(FrontendShutdownHookProvider.class)
        .asEagerSingleton();
    // Eager so management-port readiness is bound before the first proxied request.
    bind(ProxyAdmission.class).asEagerSingleton();
  }

  /**
   * Checks if the service is currently shutting down.
   *
   * <p>Called by the health endpoints to decide whether readiness should return 503 (Service
   * Unavailable). Non-blocking atomic read.
   *
   * @return true if shutdown has been initiated, false otherwise
   */
  public boolean isShuttingDown() {
    return isShuttingDown.get();
  }

  /**
   * Marks the service as shutting down (one-way operation for production use).
   *
   * <p>Called by FrontendShutdownHook during the before-service-unbind phase of Pekko
   * CoordinatedShutdown to signal that the service is shutting down and should not accept new
   * requests.
   *
   * <p>This is a one-way latch that only transitions from false→true, matching the GMS handler
   * pattern (GracefulShutdownHandler.onApplicationClosed). This prevents external code from
   * reversing a shutdown state.
   */
  public void markShuttingDown() {
    isShuttingDown.set(true);
  }

  /**
   * Sets the shutdown flag to an arbitrary value.
   *
   * <p><b>@VisibleForTesting</b> — Used by tests to reset state between test runs. This method
   * exists only for test cleanup, allowing tests to verify behavior in both shutdown and running
   * states.
   *
   * <p>Production code should use {@link #markShuttingDown()} instead, which enforces one-way
   * semantics and prevents accidental state reversals.
   *
   * @param value true to mark service as shutting down, false to reset state (tests only)
   */
  public void setShuttingDown(boolean value) {
    isShuttingDown.set(value);
  }

  /**
   * Stops the management listener in before-actor-system-terminate, after Play has drained and
   * closed its connections. Registered regardless of FRONTEND_GRACEFUL_SHUTDOWN_ENABLED, since the
   * listener no longer stops itself from a JVM shutdown hook.
   */
  static void registerManagementServerStop(CoordinatedShutdown coordinatedShutdown) {
    coordinatedShutdown.addTask(
        CoordinatedShutdown.PhaseBeforeActorSystemTerminate(),
        "stop-management-server",
        () -> {
          // Stop on this thread. The listen socket close does not need the common pool.
          PrometheusScrapeServer.stopIfRunning();
          return CompletableFuture.completedFuture(Done.done());
        });
  }

  /**
   * Provider factory for FrontendShutdownHook that conditionally enables graceful shutdown.
   *
   * <p>This indirection allows graceful shutdown to be toggled via configuration (
   * FRONTEND_GRACEFUL_SHUTDOWN_ENABLED) without code changes or complex conditional bean
   * definitions. Returns either an active hook (when enabled) or a no-op hook (when disabled).
   *
   * <p><b>Design Pattern (No-Op Construction)</b>: When disabled, a FrontendShutdownHook is still
   * instantiated but with null arguments. The constructor's null-check detects this and skips task
   * registration, creating a safe no-op. This avoids the complexity of conditional bean definitions
   * while maintaining clear intent through documentation.
   */
  @Singleton
  public static class FrontendShutdownHookProvider implements Provider<FrontendShutdownHook> {

    private final Config config;
    private final CoordinatedShutdown coordinatedShutdown;
    private final CloseableHttpClient httpClient;
    private final GracefulShutdownModule module;

    @Inject
    public FrontendShutdownHookProvider(
        Config config,
        CoordinatedShutdown coordinatedShutdown,
        CloseableHttpClient httpClient,
        GracefulShutdownModule module) {
      this.config = config;
      this.coordinatedShutdown = coordinatedShutdown;
      this.httpClient = httpClient;
      this.module = module;
    }

    @Override
    public FrontendShutdownHook get() {
      registerManagementServerStop(coordinatedShutdown);
      if (config.getBoolean("frontend.graceful_shutdown_enabled")) {
        return new FrontendShutdownHook(coordinatedShutdown, httpClient, module);
      }
      // Return a no-op hook if graceful shutdown is disabled.
      // Passing null for coordinatedShutdown signals the constructor to skip all task
      // registration, making this hook a no-op.
      // This is the no-op construction pattern — allows feature toggle without code changes.
      return new FrontendShutdownHook(null, null, module);
    }
  }

  /**
   * Registers Pekko CoordinatedShutdown phase tasks to gracefully shut down the frontend.
   *
   * <p>Pekko CoordinatedShutdown runs phases in order; each timeout below is a maximum, not a
   * delay:
   *
   * <ol>
   *   <li><b>before-service-unbind</b>: sets isShuttingDown=true so health endpoints return 503.
   *   <li><b>service-unbind</b>: Play unbinds the HTTP port (registered by Play, not here).
   *   <li><b>service-requests-done</b>: Play waits {@code play.server.waitBeforeTermination}, then
   *       terminates in-flight connections within {@code play.server.terminationTimeout}
   *       (registered by Play, not here).
   *   <li><b>service-stop</b>: closes the HTTP client used for GMS calls.
   * </ol>
   *
   * <p><b>Timing budget</b>: Kubernetes starts the terminationGracePeriodSeconds countdown before
   * the preStop hook, and SIGTERM is sent only after preStop finishes. preStop and these phases
   * therefore add up: preStop + before-service-unbind + service-requests-done + service-stop must
   * stay below terminationGracePeriodSeconds. On AWS spot the instance is reclaimed 120s after the
   * interruption notice, and node drain starts a few seconds after that notice, so keep the total
   * well below 120s.
   *
   * <p><b>No-op behavior</b>: If coordinatedShutdown is null (feature disabled), no tasks are
   * registered, and the hook becomes a no-op. This allows graceful shutdown to be toggled via
   * configuration alone.
   */
  @Singleton
  public static class FrontendShutdownHook {

    private static final Logger log = LoggerFactory.getLogger(FrontendShutdownHook.class);
    private final CoordinatedShutdown coordinatedShutdown;
    private final CloseableHttpClient httpClient;
    private final GracefulShutdownModule module;

    public FrontendShutdownHook(
        CoordinatedShutdown coordinatedShutdown,
        CloseableHttpClient httpClient,
        GracefulShutdownModule module) {
      this.coordinatedShutdown = coordinatedShutdown;
      this.httpClient = httpClient;
      this.module = module;

      if (coordinatedShutdown != null) {
        // Phase: before-service-unbind
        // Flip the readiness flag so health endpoints return 503 while Play drains.
        coordinatedShutdown.addTask(
            CoordinatedShutdown.PhaseBeforeServiceUnbind(),
            "mark-unhealthy",
            () ->
                CompletableFuture.runAsync(
                        () -> {
                          log.info("Frontend shutdown initiated - stopping new connections soon");
                          module.markShuttingDown();
                        })
                    .thenApply(v -> Done.done()));

        // Phase: service-stop
        // After in-flight requests drain, close long-lived connections (HTTP client for GMS calls).
        coordinatedShutdown.addTask(
            CoordinatedShutdown.PhaseServiceStop(),
            "close-http-clients",
            () ->
                CompletableFuture.runAsync(
                        () -> {
                          try {
                            log.info("Frontend shutdown initiated - shutting down open resources");
                            if (httpClient != null) {
                              httpClient.close();
                            }
                          } catch (IOException e) {
                            log.error("Error closing CloseableHttpClient during shutdown", e);
                          }
                        })
                    .thenApply(v -> Done.done()));
      }
    }
  }
}
