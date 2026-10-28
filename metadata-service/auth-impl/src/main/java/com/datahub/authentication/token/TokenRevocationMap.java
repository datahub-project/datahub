package com.datahub.authentication.token;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Revocation state for stateful access tokens. GMS publishes through a Hazelcast map so a revoke is
 * visible on every GMS. Processes that do not start Hazelcast (MCE, and tests that do not) keep the
 * same state in-process.
 *
 * <p>The Hazelcast map has no near cache. A near cache would delay a revoke by the instance-wide
 * invalidation batch, and turning that batch off would change every other map on the same instance.
 * {@link IMap#get} reads the partition owner, so a {@code put} is visible to the next get on every
 * member.
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

  public static State cluster(@Nonnull HazelcastInstance hazelcast) {
    return new HazelcastState(get(hazelcast));
  }

  public static State local() {
    Cache<String, Boolean> cache =
        CacheBuilder.newBuilder().expireAfterWrite(TTL_SECONDS, TimeUnit.SECONDS).build();
    return new LocalState(cache);
  }

  /** {@code true} means revoked. A missing key is not a decision. */
  public interface State {
    @Nullable
    Boolean get(@Nonnull String key);

    void put(@Nonnull String key, @Nonnull Boolean value);

    void putIfAbsent(@Nonnull String key, @Nonnull Boolean value);
  }

  private static final class HazelcastState implements State {
    private final IMap<String, Boolean> map;

    private HazelcastState(IMap<String, Boolean> map) {
      this.map = map;
    }

    @Override
    public Boolean get(String key) {
      return map.get(key);
    }

    @Override
    public void put(String key, Boolean value) {
      map.put(key, value);
    }

    @Override
    public void putIfAbsent(String key, Boolean value) {
      map.putIfAbsent(key, value);
    }
  }

  private static final class LocalState implements State {
    private final Cache<String, Boolean> cache;

    private LocalState(Cache<String, Boolean> cache) {
      this.cache = cache;
    }

    @Override
    public Boolean get(String key) {
      return cache.getIfPresent(key);
    }

    @Override
    public void put(String key, Boolean value) {
      cache.put(key, value);
    }

    @Override
    public void putIfAbsent(String key, Boolean value) {
      cache.asMap().putIfAbsent(key, value);
    }
  }
}
