package config;

import akka.Done;
import akka.actor.CoordinatedShutdown;
import com.google.common.annotations.VisibleForTesting;
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

@Singleton
public class GracefulShutdownModule extends AbstractModule {

  private static final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

  @Override
  protected void configure() {
    bind(FrontendShutdownHook.class)
        .toProvider(FrontendShutdownHookProvider.class)
        .asEagerSingleton();
  }

  public static boolean isShuttingDown() {
    return isShuttingDown.get();
  }

  @VisibleForTesting
  public static void setShuttingDownForTesting(boolean value) {
    isShuttingDown.set(value);
  }

  @Singleton
  public static class FrontendShutdownHookProvider implements Provider<FrontendShutdownHook> {

    private final Config config;
    private final CoordinatedShutdown coordinatedShutdown;
    private final CloseableHttpClient httpClient;

    @Inject
    public FrontendShutdownHookProvider(
        Config config, CoordinatedShutdown coordinatedShutdown, CloseableHttpClient httpClient) {
      this.config = config;
      this.coordinatedShutdown = coordinatedShutdown;
      this.httpClient = httpClient;
    }

    @Override
    public FrontendShutdownHook get() {
      if (config.getBoolean("frontend.graceful_shutdown_enabled")) {
        return new FrontendShutdownHook(coordinatedShutdown, httpClient);
      }
      // Return a no-op hook if disabled
      return new FrontendShutdownHook(null, null);
    }
  }

  @Singleton
  public static class FrontendShutdownHook {

    private static final Logger log = LoggerFactory.getLogger(FrontendShutdownHook.class);
    private final CoordinatedShutdown coordinatedShutdown;
    private final CloseableHttpClient httpClient;

    public FrontendShutdownHook(
        CoordinatedShutdown coordinatedShutdown, CloseableHttpClient httpClient) {
      this.coordinatedShutdown = coordinatedShutdown;
      this.httpClient = httpClient;

      if (coordinatedShutdown != null) {
        // Phase 1: before connections close — log intent
        coordinatedShutdown.addTask(
            CoordinatedShutdown.PhaseBeforeServiceUnbind(),
            "mark-unhealthy",
            () ->
                CompletableFuture.runAsync(
                        () -> {
                          log.info("Frontend shutdown initiated - stopping new connections soon");
                          isShuttingDown.set(true);
                        })
                    .thenApply(v -> Done.done()));

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
