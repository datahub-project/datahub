package com.linkedin.gms.factory.entity.throttle;

import static com.linkedin.gms.factory.common.CacheConfig.THROTTLE_MAP;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapEvent;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleEvent;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.dao.throttle.ThrottleType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import org.springframework.stereotype.Component;

/**
 * Uses the distributed cache to propagate a manual throttle event when GMS is run in a distributed
 * mode.
 */
@Component
public class ManualThrottleSensor implements ThrottleSensor {
  private static final ThrottleEvent ENABLE =
      ThrottleEvent.builder().throttled(Map.of(ThrottleType.MANUAL, true)).build();
  private static final ThrottleEvent DISABLE =
      ThrottleEvent.builder().throttled(Map.of(ThrottleType.MANUAL, false)).build();

  /** A list of throttle event listeners to execute when throttling occurs and ceases */
  private final List<Function<ThrottleEvent, ThrottleControl>> throttleCallbacks =
      new ArrayList<>();

  private final Set<ThrottleControl> registeredThrottles = new HashSet<>();

  @Nullable private final ReplicatedMap<String, String> throttleState;

  public ManualThrottleSensor(@Nullable final HazelcastInstance hazelcastInstance) {
    if (hazelcastInstance != null) {
      throttleState = hazelcastInstance.getReplicatedMap(THROTTLE_MAP);
      throttleState.addEntryListener(
          ManualThrottleTypeListener.builder().manualThrottleSensor(this).build());
    } else {
      throttleState = null;
    }
  }

  @Override
  public ManualThrottleSensor addCallback(Function<ThrottleEvent, ThrottleControl> callback) {
    throttleCallbacks.add(callback);
    return this;
  }

  public void setThrottle(boolean enabled) {
    if (throttleState == null) {
      // set local only
      setLocalThrottle(enabled);
    } else {
      // set shared location for distribution
      throttleState.put(ThrottleType.MANUAL.toString(), enabled ? "true" : "false");
    }
  }

  private void setLocalThrottle(boolean enabled) {
    synchronized (this) {
      registeredThrottles.forEach(listener -> listener.execute(DISABLE));
      registeredThrottles.clear();

      if (enabled) {
        registeredThrottles.addAll(
            throttleCallbacks.stream()
                .map(listener -> listener.apply(ENABLE))
                .collect(Collectors.toSet()));
      }
    }
  }

  @Builder
  private record ManualThrottleTypeListener(@Nonnull ManualThrottleSensor manualThrottleSensor)
      implements EntryListener<String, String> {
    @Override
    public void entryAdded(EntryEvent<String, String> event) {
      if (ThrottleType.MANUAL.equals(ThrottleType.valueOf(event.getKey()))) {
        manualThrottleSensor.setLocalThrottle(Boolean.parseBoolean(event.getValue()));
      }
    }

    @Override
    public void entryUpdated(EntryEvent<String, String> event) {
      if (ThrottleType.MANUAL.equals(ThrottleType.valueOf(event.getKey()))) {
        manualThrottleSensor.setLocalThrottle(Boolean.parseBoolean(event.getValue()));
      }
    }

    @Override
    public void entryRemoved(EntryEvent<String, String> event) {}

    @Override
    public void entryEvicted(EntryEvent<String, String> entryEvent) {}

    @Override
    public void entryExpired(EntryEvent<String, String> entryEvent) {}

    @Override
    public void mapCleared(MapEvent mapEvent) {}

    @Override
    public void mapEvicted(MapEvent mapEvent) {}
  }
}
