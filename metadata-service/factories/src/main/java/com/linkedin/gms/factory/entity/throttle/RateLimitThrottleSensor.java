package com.linkedin.gms.factory.entity.throttle;

import com.google.common.annotations.VisibleForTesting;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.linkedin.gms.factory.common.CacheConfig;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleEvent;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.dao.throttle.ThrottleType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * An always-on sensor that throttles if the number of events exceeds a certain amount in a given
 * time window.
 *
 * <p>Rate limit only activated if # events above quota within window. Upon activation, reject
 * events if exceeding per-minute limit. Rate limiting is applied per gms instance / mce consumer,
 * so rate limit scales linearly with number of instances.
 */
@Slf4j
@Builder
public class RateLimitThrottleSensor implements ThrottleSensor {
  private final int activationQuota;
  private final int activationIntervalMinutes;
  private final int rateLimitIntervalSeconds;
  private final int limitPerInterval;
  private final int updateIntervalMs;
  private final double jitterRatio;
  @Nullable private final HazelcastInstance hazelcastInstance;

  private final List<Function<ThrottleEvent, ThrottleControl>> throttleCallbacks =
      new ArrayList<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private final AtomicBoolean isCheckingThrottle = new AtomicBoolean(false);
  @VisibleForTesting final AtomicBoolean isLocallyThrottled = new AtomicBoolean(false);
  private final EventCounter eventCounter = new EventCounter();

  @Override
  public RateLimitThrottleSensor addCallback(Function<ThrottleEvent, ThrottleControl> callback) {
    throttleCallbacks.add(callback);
    return this;
  }

  @Override
  @Nullable
  public Consumer<Integer> eventRecorder() {
    return eventCounter::recordEvents;
  }

  public RateLimitThrottleSensor start() {
    scheduler.scheduleAtFixedRate(
        () -> {
          try {
            checkThrottle();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        },
        0,
        updateIntervalMs,
        TimeUnit.MILLISECONDS);

    scheduler.scheduleAtFixedRate(
        () -> {
          eventCounter.numEventsInActivationInterval.set(0);
        },
        activationIntervalMinutes,
        activationIntervalMinutes,
        TimeUnit.MINUTES);
    return this;
  }

  @VisibleForTesting
  public void checkThrottle() throws InterruptedException {
    if (isCheckingThrottle.getAndSet(true)) {
      // Already throttled, allow throttle control to handle unthrottling
      // Avoid race condition between throttle control callbacks and later calls of checkThrottle
      log.info("Already throttling, skipping check.");
      return;
    }

    long activationIntervalCount = eventCounter.numEventsInActivationInterval.get();
    int rateLimitIntervalCount = eventCounter.getTotalCount(rateLimitIntervalSeconds);

    boolean shouldThrottle =
        activationIntervalCount > activationQuota && rateLimitIntervalCount >= limitPerInterval;
    log.info(
        "{}: throttling={} (totalCount={}, windowCount={})",
        this,
        shouldThrottle,
        activationIntervalCount,
        rateLimitIntervalCount);

    if (shouldThrottle) {
      int eventsUntilUnthrottle = rateLimitIntervalCount - limitPerInterval;
      // Wait until we expect throttle to end, with some buffer
      // Add +999 because Retry-After header is truncated to seconds
      long msUntilUnthrottle = eventCounter.getMsUntilNumEventsElapsed(eventsUntilUnthrottle) + 999;

      log.info(
          "{}: Throttling {} callbacks for {}ms.",
          this,
          throttleCallbacks.size(),
          msUntilUnthrottle);
      ThrottleEvent throttleEvent =
          ThrottleEvent.builder()
              .backoffWaitMs(Map.of(ThrottleType.RATE_LIMIT, msUntilUnthrottle))
              .jitterRatio(jitterRatio)
              .build();

      // Throttle
      setIsThrottled(true);
      List<ThrottleControl> throttleControls =
          throttleCallbacks.stream().map(callback -> callback.apply(throttleEvent)).toList();

      // Unthrottle after sleep
      Thread.sleep(msUntilUnthrottle);
      log.info(
          "{}: Unthrottling {} callbacks after waiting {}ms.",
          this,
          throttleControls.size(),
          msUntilUnthrottle);
      throttleControls.forEach(
          control -> control.execute(ThrottleEvent.clearThrottle(throttleEvent)));
      setIsThrottled(false);
    }

    isCheckingThrottle.set(false);
  }

  public boolean getIsThrottled() {
    if (hazelcastInstance != null) {
      try {
        IMap<String, Boolean> throttleStatusMap =
            hazelcastInstance.getMap(CacheConfig.IS_RATE_LIMIT_THROTTLED);
        Boolean status = throttleStatusMap.get("status");
        return status != null && status;
      } catch (Exception e) {
        log.warn("Failed to get rate limit throttle status from Hazelcast cache", e);
        return isLocallyThrottled.get();
      }
    } else {
      return isLocallyThrottled.get();
    }
  }

  @VisibleForTesting
  public void setIsThrottled(boolean isThrottled) {
    if (isThrottled && hazelcastInstance != null) {
      // Do not unset in Hazelcast, let it expire
      try {
        IMap<String, Boolean> throttleStatusMap =
            hazelcastInstance.getMap(CacheConfig.IS_RATE_LIMIT_THROTTLED);
        throttleStatusMap.put("status", true);
        log.debug("Set rate limit throttle status in Hazelcast cache");
      } catch (Exception e) {
        log.warn("Failed to set rate limit throttle status in Hazelcast cache", e);
      }
    }
    isLocallyThrottled.set(isThrottled);
  }
}
