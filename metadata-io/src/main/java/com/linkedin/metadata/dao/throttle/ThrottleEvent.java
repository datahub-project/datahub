package com.linkedin.metadata.dao.throttle;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@Builder
public class ThrottleEvent {
  public static ThrottleEvent throttle(Map<ThrottleType, Long> backoffWaitMs) {
    return ThrottleEvent.builder()
        .backoffWaitMs(backoffWaitMs)
        .throttled(
            backoffWaitMs.entrySet().stream()
                .filter(entry -> entry.getValue() > 0)
                .map(entry -> Map.entry(entry.getKey(), true))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .build();
  }

  public static ThrottleEvent clearThrottle(ThrottleEvent throttleEvent) {
    return clearThrottle(throttleEvent.getActiveThrottles());
  }

  public static ThrottleEvent clearThrottle(Set<ThrottleType> clear) {
    return ThrottleEvent.builder()
        .throttled(
            clear.stream()
                .map(t -> Map.entry(t, false))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .build();
  }

  Map<ThrottleType, Boolean> throttled;
  Map<ThrottleType, Long> backoffWaitMs;

  public Set<ThrottleType> getActiveThrottles() {
    return streamTypes().filter(this::isThrottled).collect(Collectors.toSet());
  }

  /**
   * Return the suggested wait time in milliseconds given an optional list filter types.
   *
   * @param filterTypes empty for no filters
   * @return suggested wait time in milliseconds, negative if no suggestion is possible, null if no
   *     wait
   */
  @Nullable
  public Long getActiveThrottleMaxWaitMs(Set<ThrottleType> filterTypes) {
    Set<ThrottleType> activeThrottles =
        getActiveThrottles().stream()
            .filter(a -> filterTypes.isEmpty() || filterTypes.contains(a))
            .collect(Collectors.toSet());

    if (activeThrottles.isEmpty()) {
      return null;
    }

    if (!activeThrottles.contains(ThrottleType.MANUAL) && backoffWaitMs != null) {
      return getActiveThrottles().stream()
          .map(t -> backoffWaitMs.getOrDefault(t, -1L))
          .max(Comparator.naturalOrder())
          .orElse(-1L);
    }

    return -1L;
  }

  public Set<ThrottleType> getDisabledThrottles() {
    return streamTypes().filter(t -> !isThrottled(t)).collect(Collectors.toSet());
  }

  public boolean isThrottled() {
    return (throttled != null && throttled.values().stream().anyMatch(b -> b))
        || (backoffWaitMs != null && backoffWaitMs.values().stream().anyMatch(wait -> wait > 0));
  }

  private boolean isThrottled(ThrottleType throttleType) {
    return (throttled != null && throttled.getOrDefault(throttleType, false))
        || (backoffWaitMs != null && backoffWaitMs.getOrDefault(throttleType, 0L) > 0);
  }

  private Stream<ThrottleType> streamTypes() {
    return Stream.concat(
            throttled != null ? throttled.keySet().stream() : Stream.empty(),
            backoffWaitMs != null ? backoffWaitMs.keySet().stream() : Stream.empty())
        .distinct();
  }
}
