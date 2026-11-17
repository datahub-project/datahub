package com.datahub.authorization;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import javax.annotation.Nonnull;

/**
 * Signals committed policy changes to every GMS through the Hazelcast cluster that GMS already runs
 * (POLICY_CACHE_BROADCAST_ENABLED). The key is the changed policy URN and the value is the change
 * time in epoch millis. The entry carries no policy: each member reads the policy by key from the
 * primary store, so concurrent changes to different policies cannot overwrite each other.
 *
 * <p>Entries live as long as the authorizer's recent-change window, so a member that joins within
 * that window still reads the change from the key set.
 *
 * <p>The map has no near cache. Members never read values: they get entry events and the key set,
 * which a near cache does not serve, so it would only use memory.
 */
public final class PolicyChangeMap {

  public static final String NAME = "datahubPolicyChanges";

  private PolicyChangeMap() {}

  @Nonnull
  public static MapConfig mapConfig(int ttlSeconds) {
    return new MapConfig().setName(NAME).setTimeToLiveSeconds(ttlSeconds);
  }

  @Nonnull
  public static IMap<String, Long> get(@Nonnull HazelcastInstance hazelcast) {
    return hazelcast.getMap(NAME);
  }
}
