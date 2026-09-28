package auth.metrics;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.typesafe.config.Config;
import filters.InFlightRequestsFilter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToDoubleFunction;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.dispatch.Dispatcher;
import org.apache.pekko.dispatch.Dispatchers;
import org.apache.pekko.dispatch.ExecutorServiceDelegate;
import org.apache.pekko.dispatch.MessageDispatcher;

/**
 * Micrometer gauges for the Play/Pekko HTTP server pool. Registered on the Prometheus registry only
 * and scraped at {@code /actuator/prometheus}. Names mirror GMS Jetty pool gauges under {@code
 * play.http.*}. {@code baseUnit} is omitted: the Prometheus naming convention appends it, which
 * would export {@code play.http.threads.busy} as {@code play_http_threads_busy_threads}.
 *
 * <p>Pekko HTTP does not expose a live connection count, so connection gauges are the configured
 * limits. Thread gauges read the actor-system default dispatcher, which runs HTTP requests and
 * probes and is shared with other Pekko actors. These are not registered as {@code executor.*}:
 * that family is already used by the entity-client pools.
 */
@Slf4j
@Singleton
public class PlayHttpServerMetrics {

  static final String THREADS_BUSY = "play.http.threads.busy";
  static final String THREADS_IDLE = "play.http.threads.idle";
  static final String THREADS_CURRENT = "play.http.threads.current";
  static final String THREADS_JOBS = "play.http.threads.jobs";
  static final String THREADS_PARALLELISM = "play.http.threads.parallelism";
  static final String THREADS_CONFIG_MIN = "play.http.threads.config.min";
  static final String THREADS_CONFIG_MAX = "play.http.threads.config.max";
  static final String CONNECTIONS_MAX = "play.http.connections.max";
  static final String CONNECTIONS_BACKLOG = "play.http.connections.backlog";
  static final String REQUESTS_INFLIGHT = "play.http.requests.inflight";

  static final String PARALLELISM_MIN_PATH =
      "pekko.actor.default-dispatcher.fork-join-executor.parallelism-min";
  static final String PARALLELISM_MAX_PATH =
      "pekko.actor.default-dispatcher.fork-join-executor.parallelism-max";
  static final String MAX_CONNECTIONS_PATH = "pekko.http.server.max-connections";
  static final String BACKLOG_PATH = "pekko.http.server.backlog";

  @Inject
  public PlayHttpServerMetrics(
      ActorSystem actorSystem,
      Config config,
      MetricUtils metricUtils,
      InFlightRequestsFilter inFlightRequestsFilter) {
    PrometheusMeterRegistry prometheus = prometheusRegistry(metricUtils.getRegistry());
    if (prometheus == null) {
      log.warn("Prometheus meter registry is unset; Play HTTP pool metrics will not be exported");
      return;
    }
    register(
        prometheus,
        resolveDispatcherExecutor(actorSystem),
        config,
        inFlightRequestsFilter.inFlightCount());
  }

  /**
   * The Prometheus registry these gauges are exported on. Composite registries are unwrapped; JMX
   * and other registries are ignored.
   */
  @Nullable
  static PrometheusMeterRegistry prometheusRegistry(@Nullable MeterRegistry registry) {
    if (registry instanceof PrometheusMeterRegistry) {
      return (PrometheusMeterRegistry) registry;
    }
    if (registry instanceof CompositeMeterRegistry) {
      for (MeterRegistry child : ((CompositeMeterRegistry) registry).getRegistries()) {
        PrometheusMeterRegistry prometheus = prometheusRegistry(child);
        if (prometheus != null) {
          return prometheus;
        }
      }
    }
    return null;
  }

  static void register(
      PrometheusMeterRegistry registry,
      @Nullable ExecutorService executor,
      Config config,
      AtomicInteger inFlight) {
    if (executor instanceof ForkJoinPool) {
      registerForkJoin(registry, (ForkJoinPool) executor);
    } else if (executor instanceof ThreadPoolExecutor) {
      registerThreadPool(registry, (ThreadPoolExecutor) executor);
    } else if (executor != null) {
      log.warn(
          "Play HTTP dispatcher executor {} is not a ForkJoinPool or ThreadPoolExecutor; thread"
              + " pool gauges will not be exported",
          executor.getClass().getName());
    }

    registerConfigGauge(
        registry,
        config,
        THREADS_CONFIG_MIN,
        PARALLELISM_MIN_PATH,
        "Configured Pekko fork-join parallelism-min");
    registerConfigGauge(
        registry,
        config,
        THREADS_CONFIG_MAX,
        PARALLELISM_MAX_PATH,
        "Configured Pekko fork-join parallelism-max");
    registerConfigGauge(
        registry,
        config,
        CONNECTIONS_MAX,
        MAX_CONNECTIONS_PATH,
        "Configured Pekko HTTP max-connections");
    registerConfigGauge(
        registry,
        config,
        CONNECTIONS_BACKLOG,
        BACKLOG_PATH,
        "Configured Pekko HTTP accept backlog");
    gauge(
        registry,
        REQUESTS_INFLIGHT,
        inFlight,
        count -> count.get(),
        "HTTP requests currently inside the Play filter chain");
  }

  @Nullable
  static ExecutorService resolveDispatcherExecutor(ActorSystem actorSystem) {
    try {
      MessageDispatcher dispatcher =
          actorSystem.dispatchers().lookup(Dispatchers.DefaultDispatcherId());
      if (!(dispatcher instanceof Dispatcher)) {
        log.warn(
            "Default dispatcher is {}; skipping Play HTTP thread pool gauges",
            dispatcher.getClass().getName());
        return null;
      }
      ExecutorService executor = ((Dispatcher) dispatcher).executorService().executor();
      while (executor instanceof ExecutorServiceDelegate) {
        executor = ((ExecutorServiceDelegate) executor).executor();
      }
      return executor;
    } catch (RuntimeException e) {
      log.warn("Failed to read the Play HTTP dispatcher for pool metrics", e);
      return null;
    }
  }

  private static void registerForkJoin(PrometheusMeterRegistry registry, ForkJoinPool pool) {
    gauge(
        registry,
        THREADS_BUSY,
        pool,
        ForkJoinPool::getActiveThreadCount,
        "Threads actively executing on the Play HTTP dispatcher");
    gauge(
        registry,
        THREADS_CURRENT,
        pool,
        ForkJoinPool::getPoolSize,
        "Current threads in the Play HTTP dispatcher");
    gauge(
        registry,
        THREADS_IDLE,
        pool,
        p -> Math.max(0, p.getPoolSize() - p.getActiveThreadCount()),
        "Idle threads in the Play HTTP dispatcher");
    gauge(
        registry,
        THREADS_JOBS,
        pool,
        ForkJoinPool::getQueuedSubmissionCount,
        "Tasks queued on the Play HTTP dispatcher");
    gauge(
        registry,
        THREADS_PARALLELISM,
        pool,
        ForkJoinPool::getParallelism,
        "Live parallelism of the Play HTTP dispatcher");
  }

  private static void registerThreadPool(
      PrometheusMeterRegistry registry, ThreadPoolExecutor pool) {
    gauge(
        registry,
        THREADS_BUSY,
        pool,
        ThreadPoolExecutor::getActiveCount,
        "Threads actively executing on the Play HTTP dispatcher");
    gauge(
        registry,
        THREADS_CURRENT,
        pool,
        ThreadPoolExecutor::getPoolSize,
        "Current threads in the Play HTTP dispatcher");
    gauge(
        registry,
        THREADS_IDLE,
        pool,
        p -> Math.max(0, p.getPoolSize() - p.getActiveCount()),
        "Idle threads in the Play HTTP dispatcher");
    gauge(
        registry,
        THREADS_JOBS,
        pool,
        p -> p.getQueue().size(),
        "Tasks queued on the Play HTTP dispatcher");
    gauge(
        registry,
        THREADS_PARALLELISM,
        pool,
        ThreadPoolExecutor::getMaximumPoolSize,
        "Maximum threads the Play HTTP dispatcher will run");
  }

  private static void registerConfigGauge(
      PrometheusMeterRegistry registry,
      Config config,
      String name,
      String path,
      String description) {
    if (!config.hasPath(path)) {
      log.warn("Play HTTP metric {} not registered; config path {} is unset", name, path);
      return;
    }
    gauge(registry, name, config, c -> c.getDouble(path), description);
  }

  private static <T> void gauge(
      PrometheusMeterRegistry registry,
      String name,
      T state,
      ToDoubleFunction<T> value,
      String description) {
    Gauge.builder(name, state, value)
        .description(description)
        .strongReference(true)
        .register(registry);
  }
}
