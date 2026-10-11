package com.linkedin.metadata.entity.storage;

import io.datahubproject.metadata.context.StorageTarget;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

/** Registry of named primary-storage pools (Ebean {@link io.ebean.Database}, Cassandra session). */
public class PrimaryStorageRegistry {

  private final Map<StorageTarget, Object> pools = new EnumMap<>(StorageTarget.class);
  private final boolean distinctReadEndpoint;

  public PrimaryStorageRegistry(boolean distinctReadEndpoint) {
    this.distinctReadEndpoint = distinctReadEndpoint;
  }

  public void register(@Nonnull StorageTarget target, @Nonnull Object pool) {
    pools.put(target, pool);
  }

  public boolean has(@Nonnull StorageTarget target) {
    return pools.containsKey(target);
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  public <T> T get(@Nonnull StorageTarget target, @Nonnull Class<T> type) {
    Object pool = pools.get(target);
    if (pool == null) {
      throw new IllegalStateException("No pool registered for target: " + target);
    }
    if (!type.isInstance(pool)) {
      throw new IllegalStateException(
          "Pool for " + target + " is not of type " + type.getName() + " but " + pool.getClass());
    }
    return (T) pool;
  }

  @Nonnull
  public Optional<Object> getOptional(@Nonnull StorageTarget target) {
    return Optional.ofNullable(pools.get(target));
  }

  /** True when READ pool contact info differs from PRIMARY (physical replica deploy). */
  public boolean isDistinctReadEndpoint() {
    return distinctReadEndpoint;
  }
}
