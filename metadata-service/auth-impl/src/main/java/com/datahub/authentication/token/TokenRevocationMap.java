package com.datahub.authentication.token;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

/**
 * Cluster-wide revocation state for stateful access tokens. A local cache cannot invalidate another
 * GMS, so create and revoke publish through this map.
 *
 * <p>The map has no near cache. A near cache would delay a revoke by the instance-wide invalidation
 * batch, and turning that batch off would change every other map on the same instance. {@link
 * IMap#get} reads the partition owner, so a {@code put} is visible to the next get on every member.
 */
public final class TokenRevocationMap {

  public static final String NAME = "datahubAccessTokenRevoked";

  /** Matches the previous in-process cache {@code expireAfterWrite}. */
  public static final int TTL_SECONDS = 300;

  private TokenRevocationMap() {}

  public static MapConfig mapConfig() {
    return new MapConfig().setName(NAME).setTimeToLiveSeconds(TTL_SECONDS);
  }

  public static IMap<String, Boolean> get(HazelcastInstance hazelcast) {
    return hazelcast.getMap(NAME);
  }
}
