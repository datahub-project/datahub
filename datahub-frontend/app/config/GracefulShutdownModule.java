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

@Singleton
public class GracefulShutdownModule extends AbstractModule {

  private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

  @Override
  protected void configure() {
    bind(GracefulShutdownModule.class).toInstance(this);
    bind(FrontendShutdownHook.class)
        .toProvider(FrontendShutdownHookProvider.class)
        .asEagerSingleton();
  }

  public boolean isShuttingDown() {
    return isShuttingDown.get();
  }

  public void setShuttingDown(boolean value) {
    isShuttingDown.set(value);
  }

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
      // Return a no-op hook if disabled
      return new FrontendShutdownHook(null, null, module);
    }
  }

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
        // Phase 1: before connections close — log intent
        coordinatedShutdown.addTask(
            CoordinatedShutdown.PhaseBeforeServiceUnbind(),
            "mark-unhealthy",
            () ->
                CompletableFuture.runAsync(
                        () -> {
                          log.info("Frontend shutdown initiated - stopping new connections soon");
                          module.setShuttingDown(true);
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
