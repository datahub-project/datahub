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
import javax.inject.Singleton;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class GracefulShutdownModule extends AbstractModule {

  private static final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
  private final Config config;

  public GracefulShutdownModule(Config config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    if (config.getBoolean("frontend.graceful_shutdown_enabled")) {
      bind(FrontendShutdownHook.class).asEagerSingleton();
    }
  }

  public static boolean isShuttingDown() {
    return isShuttingDown.get();
  }

  @VisibleForTesting
  public static void setShuttingDownForTesting(boolean value) {
    isShuttingDown.set(value);
  }

  @Singleton
  public static class FrontendShutdownHook {

    private static final Logger log = LoggerFactory.getLogger(FrontendShutdownHook.class);

    @Inject
    public FrontendShutdownHook(
        CoordinatedShutdown coordinatedShutdown, CloseableHttpClient httpClient) {

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
                  .thenApply(v -> Done.getInstance()));

      coordinatedShutdown.addTask(
          CoordinatedShutdown.PhaseServiceStop(),
          "close-http-clients",
          () ->
              CompletableFuture.runAsync(
                      () -> {
                        try {
                          log.info("Frontend shutdown initiated - shutting down open resources");
                          httpClient.close();
                        } catch (IOException e) {
                          log.error("Error closing CloseableHttpClient during shutdown", e);
                        }
                      })
                  .thenApply(v -> Done.done()));
    }
  }
}
