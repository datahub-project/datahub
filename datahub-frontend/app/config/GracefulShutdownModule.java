package config;

import akka.Done;
import akka.actor.CoordinatedShutdown;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guice module managing graceful shutdown for the DataHub Frontend pod.
 *
 * <p>When enabled via FRONTEND_GRACEFUL_SHUTDOWN_ENABLED, this module coordinates with Akka's
 * CoordinatedShutdown to gracefully handle Kubernetes pod termination:
 *
 * <ol>
 *   <li>AWS Spot/K8s sends SIGTERM to the pod
 *   <li>Akka CoordinatedShutdown triggers, calling registered phase tasks
 *   <li>In before-service-unbind phase: FrontendShutdownHook flips isShuttingDown flag to true
 *   <li>HealthCheckController reads this flag and returns 503, signaling readiness to LB
 *   <li>Load balancer stops routing new requests and drains existing connections
 *   <li>Remaining phases (service-requests-done: 65s, service-stop: 15s) allow graceful cleanup
 *   <li>SIGKILL arrives after terminationGracePeriodSeconds (120s) if not already terminated
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
   * <p>Set to true by FrontendShutdownHook during the before-service-unbind phase of Akka
   * CoordinatedShutdown. Read by HealthCheckController to return 503 responses, signaling
   * Kubernetes that the pod should stop receiving new requests.
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
  }

  /**
   * Checks if the service is currently shutting down.
   *
   * <p>This is called by HealthCheckController to determine if the readiness probe should return
   * 503 (Service Unavailable). Non-blocking atomic read.
   *
   * @return true if shutdown has been initiated, false otherwise
   */
  public boolean isShuttingDown() {
    return isShuttingDown.get();
  }

  /**
   * Marks the service as shutting down (one-way operation for production use).
   *
   * <p>Called by FrontendShutdownHook during the before-service-unbind phase of Akka
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
   * Provider factory for FrontendShutdownHook that conditionally enables graceful shutdown.
   *
   * <p>This indirection allows graceful shutdown to be toggled via configuration (
   * FRONTEND_GRACEFUL_SHUTDOWN_ENABLED) without code changes or complex conditional bean
   * definitions. Returns either an active hook (when enabled) or a no-op hook (when disabled).
   *
   * <p><b>Design Pattern (No-Op Construction)</b>: When disabled, a FrontendShutdownHook is still
   * instantiated but with null arguments. The constructor's null-check (line 171) detects this and
   * skips task registration, creating a safe no-op. This avoids the complexity of conditional bean
   * definitions while maintaining clear intent through documentation.
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
      if (config.getBoolean("frontend.graceful_shutdown_enabled")) {
        return new FrontendShutdownHook(coordinatedShutdown, httpClient, module);
      }
      // Return a no-op hook if graceful shutdown is disabled.
      // Passing null for coordinatedShutdown signals the constructor (see line 171)
      // to skip all task registration, making this hook a no-op.
      // This is the no-op construction pattern — allows feature toggle without code changes.
      return new FrontendShutdownHook(null, null, module);
    }
  }

  /**
   * Registers Akka CoordinatedShutdown phase tasks to gracefully shut down the frontend.
   *
   * <p>Akka CoordinatedShutdown provides a multi-phase shutdown mechanism that coordinates resource
   * cleanup with the termination signal. The phases execute in order:
   *
   * <ol>
   *   <li><b>before-service-unbind (10s timeout)</b>: Signal intent to clients (e.g., via status
   *       pages, server header). Sets isShuttingDown=true so HealthCheckController returns 503.
   *   <li><b>service-requests-done (65s timeout)</b>: Wait for in-flight HTTP requests to complete.
   *       Play Framework drains the HTTP server during this phase.
   *   <li><b>service-stop (15s timeout)</b>: Cleanup remaining resources (WebSocket clients, HTTP
   *       caches, etc.).
   * </ol>
   *
   * <p><b>Timing Budget Analysis</b>: Akka phases total 10 + 65 + 15 = 90 seconds. Kubernetes
   * terminationGracePeriodSeconds is set to 120s. The preStop hook (70s sleep) and Akka shutdown
   * overlap after SIGTERM, so the effective maximum is max(70, 90) = 90 seconds, safely within the
   * 120s K8s budget. AWS Spot instances provide 120 seconds before SIGKILL.
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
        // Phase 1: before-service-unbind (10s default timeout)
        // Signal clients that shutdown is starting and flip the readiness flag.
        // HealthCheckController reads isShuttingDown and returns 503 (Service Unavailable),
        // which Kubernetes interprets as readiness failure. The load balancer stops routing
        // new traffic while existing connections are allowed to drain.
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

        // Phase 3: service-stop (15s default timeout)
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
