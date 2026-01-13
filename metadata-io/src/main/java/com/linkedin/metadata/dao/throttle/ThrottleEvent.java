package com.linkedin.metadata.dao.throttle;

import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * Describes whether a particular throttle type should be throttled and for how long. Each throttle
 * sensor produces a throttle event.
 */
@Value
@Accessors(fluent = true)
@Builder
public class ThrottleEvent {
  @Nonnull Map<ThrottleType, Long> backoffWaitMs;
  double jitterRatio;

  public static ThrottleEvent throttle(Map<ThrottleType, Long> backoffMap) {
    return new ThrottleEvent(backoffMap, 0);
  }

  public static ThrottleEvent booleanThrottle(Map<ThrottleType, Boolean> throttled) {
    Map<ThrottleType, Long> backoffMap =
        throttled.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> (entry.getValue() ? -1L : 0L)));
    return new ThrottleEvent(backoffMap, 0);
  }

  public static ThrottleEvent clearThrottle(ThrottleEvent throttleEvent) {
    return clearThrottle(throttleEvent.getActiveThrottles());
  }

  public static ThrottleEvent clearThrottle(Set<ThrottleType> clear) {
    return booleanThrottle(clear.stream().collect(Collectors.toMap(t -> t, t -> false)));
  }

  /** Returns the set of throttle types this event is throttling. */
  @Nonnull
  public Set<ThrottleType> getActiveThrottles() {
    return backoffWaitMs.entrySet().stream()
        .filter(entry -> entry.getValue() > 0)
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  /**
   * Returns the set of throttle types this event could throttle but is currently not throttling.
   */
  @Nonnull
  public Set<ThrottleType> getDisabledThrottles() {
    return Sets.difference(backoffWaitMs.keySet(), getActiveThrottles());
  }

  /**
   * Returns how long the client should wait before retrying, -1 if indefinite, or null if no wait
   * is necessary.
   *
   * @param filterTypes The throttle types to consider; if empty, consider all active throttle
   *     types.
   * @return The suggested wait time in milliseconds, -1 if indefinite, or null if no wait is
   *     necessary.
   */
  @Nullable
  public Long getActiveThrottleMaxWaitMs(Set<ThrottleType> filterTypes) {
    Set<ThrottleType> activeThrottles =
        filterTypes.isEmpty()
            ? getActiveThrottles()
            : Sets.intersection(getActiveThrottles(), filterTypes);

    if (activeThrottles.isEmpty()) {
      return null;
    }
    if (activeThrottles.contains(ThrottleType.MANUAL)
        || activeThrottles.stream().map(backoffWaitMs::get).anyMatch(wait -> wait.equals(-1L))) {
      return -1L;
    }

    return activeThrottles.stream()
        .map(backoffWaitMs::get)
        .map(v -> v + (long) (Math.random() * v * jitterRatio))
        .max(Comparator.naturalOrder())
        .orElse(-1L);
  }

  /** Returns true if this event is currently throttling any throttle types. */
  public boolean isThrottled() {
    return !getActiveThrottles().isEmpty();
  }
}
