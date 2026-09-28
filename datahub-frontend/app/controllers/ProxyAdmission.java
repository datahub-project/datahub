package controllers;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import config.GracefulShutdownModule;
import health.FrontendProbeState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.Semaphore;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caps concurrent GMS proxy calls so a slow upstream cannot fill the Play connection table.
 *
 * <p>Each in-flight proxy holds a Pekko HTTP connection until GMS responds (up to the 120s proxy
 * timeout). Past the cap, new proxy calls return immediately so health checks and other requests
 * can still be accepted. Readiness uses hysteresis: not ready at 80% of the cap, ready again at
 * 50%, so a pod is removed from rotation before the cap is exhausted and does not flap on every
 * request.
 */
@Singleton
public final class ProxyAdmission {

  public static final int DEFAULT_MAX_IN_FLIGHT = 256;
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
        "GMS proxy admission limit {} (readiness not-ready at {}, ready again at {})",
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
    int high = Math.max(1, (maxInFlight * 4) / 5);
    int low = maxInFlight / 2;
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
   * @return false when the cap is already full; caller must not call GMS and must not {@link
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
