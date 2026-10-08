package controllers;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import config.GracefulShutdownModule;
import health.FrontendProbeState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

/**
 * Caps concurrent Play requests that are waiting on an upstream.
 *
 * <p>Pekko HTTP's default {@code max-connections} is 1024, and this app does not override it. GMS
 * ({@code /api}, {@code /openapi}), auth calls, SSO, and in-flight OTEL forwards all hold one of
 * those connections until the upstream responds, so they share this one budget. The OTEL collector
 * also has its own smaller cap, so a trace storm is shed before it can take the rest of this
 * budget. Past the cap, the request returns 503 immediately. Readiness uses hysteresis: not ready
 * at 90% of the cap, ready again at 70%.
 */
@Singleton
public final class ProxyAdmission {

  /** Matches {@code pekko.http.server.max-connections} when Play leaves it unset. */
  public static final int DEFAULT_MAX_IN_FLIGHT = 1024;

  static final String CONFIG_PATH = "frontend.proxy.maxInFlight";

  private static final Logger log = LoggerFactory.getLogger(ProxyAdmission.class);

  private final int maxInFlight;
  private final int highWater;
  private final int lowWater;
  private final Semaphore permits;
  private final Counter rejected;

  /** Latched so readiness stays failed between the high-water and low-water marks. */
  private volatile boolean saturated;

  @Inject
  public ProxyAdmission(
      Config config, MetricUtils metricUtils, GracefulShutdownModule shutdownModule) {
    this(resolveMaxInFlight(config), metricUtils == null ? null : metricUtils.getRegistry());
    FrontendProbeState.bind(this, shutdownModule::isShuttingDown);
    log.info(
        "Upstream admission limit {} (readiness not-ready at {}, ready again at {})",
        maxInFlight,
        highWater,
        lowWater);
  }

  /** Test and direct construction. Does not register management-port probe state. */
  public ProxyAdmission(int maxInFlight, @Nullable MeterRegistry registry) {
    if (maxInFlight < 1) {
      throw new IllegalArgumentException("maxInFlight must be >= 1");
    }
    this.maxInFlight = maxInFlight;
    // Recover at 70% rather than 50%: with the trip point at 90%, waiting for 40% of the budget
    // to drain keeps a pod out of rotation too long when each proxy call can hold a slot for 120s.
    // A 20% band is still wide enough not to flap on a handful of requests.
    int high = Math.max(1, (maxInFlight * 9) / 10);
    int low = (maxInFlight * 7) / 10;
    if (low >= high) {
      low = high - 1;
    }
    this.highWater = high;
    this.lowWater = low;
    this.permits = new Semaphore(maxInFlight);
    if (registry != null) {
      Gauge.builder(
              "frontend_proxy_inflight",
              permits,
              semaphore -> (double) (maxInFlight - semaphore.availablePermits()))
          .register(registry);
      this.rejected = Counter.builder("frontend_proxy_rejected_total").register(registry);
    } else {
      this.rejected = null;
    }
  }

  static int resolveMaxInFlight(Config config) {
    if (config == null || !config.hasPath(CONFIG_PATH)) {
      return DEFAULT_MAX_IN_FLIGHT;
    }
    int value;
    try {
      Object raw = config.getValue(CONFIG_PATH).unwrapped();
      value =
          raw instanceof Number
              ? ((Number) raw).intValue()
              : Integer.parseInt(raw.toString().trim());
    } catch (ConfigException | NumberFormatException e) {
      log.warn("{} is not an integer; using {}", CONFIG_PATH, DEFAULT_MAX_IN_FLIGHT, e);
      return DEFAULT_MAX_IN_FLIGHT;
    }
    if (value < 1) {
      log.warn("{}={} is invalid; using {}", CONFIG_PATH, value, DEFAULT_MAX_IN_FLIGHT);
      return DEFAULT_MAX_IN_FLIGHT;
    }
    return value;
  }

  /**
   * Runs {@code action} while holding one permit. When the cap is full, returns 503 and does not
   * run {@code action}. A null admission (tests that construct controllers without Guice) runs
   * {@code action} with no cap.
   */
  public static Result admit(@Nullable ProxyAdmission admission, Supplier<Result> action) {
    if (admission == null) {
      return action.get();
    }
    if (!admission.tryAcquire()) {
      return admission.overloadedResult();
    }
    try {
      return action.get();
    } finally {
      admission.release();
    }
  }

  /**
   * Same as {@link #admit} for an async action. The permit is held until the stage completes.
   * Synchronous failure before the stage is returned releases the permit.
   */
  public static CompletionStage<Result> admitAsync(
      @Nullable ProxyAdmission admission, Supplier<CompletionStage<Result>> action) {
    if (admission == null) {
      return action.get();
    }
    if (!admission.tryAcquire()) {
      return CompletableFuture.completedFuture(admission.overloadedResult());
    }
    try {
      return action.get().whenComplete((result, error) -> admission.release());
    } catch (RuntimeException e) {
      admission.release();
      throw e;
    }
  }

  public Result overloadedResult() {
    return Results.status(Http.Status.SERVICE_UNAVAILABLE, "Proxy overloaded.")
        .withHeader(Http.HeaderNames.RETRY_AFTER, "1");
  }

  /**
   * @return false when the cap is already full; caller must not call upstream and must not {@link
   *     #release()}
   */
  public boolean tryAcquire() {
    if (!permits.tryAcquire()) {
      saturated = true;
      if (rejected != null) {
        rejected.increment();
      }
      return false;
    }
    if (inFlight() >= highWater) {
      saturated = true;
    }
    return true;
  }

  public void release() {
    permits.release();
    if (inFlight() <= lowWater) {
      saturated = false;
    }
  }

  /** False from the high-water mark until in-flight calls fall back to the low-water mark. */
  public boolean isAcceptingTraffic() {
    return !saturated;
  }

  int inFlight() {
    return maxInFlight - permits.availablePermits();
  }

  int highWater() {
    return highWater;
  }

  int lowWater() {
    return lowWater;
  }
}
