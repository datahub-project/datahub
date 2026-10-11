package com.linkedin.metadata.graph.cache.store;

import com.linkedin.metadata.graph.cache.CacheStatus;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Operational status entry stored in {@code entityGraphStatus} (status + optional timestamp). */
@Value
@Builder
public class EntityGraphOperationalStatus {

  @Nonnull String status;

  /**
   * Milliseconds since epoch when {@link CacheStatus#BUILDING} was claimed or {@link
   * CacheStatus#COOLDOWN} was recorded. Null for statuses that do not track time.
   */
  @Nullable Long recordedAtMillis;

  @Nonnull
  public static EntityGraphOperationalStatus of(
      @Nonnull CacheStatus cacheStatus, @Nullable Long recordedAtMillis) {
    return EntityGraphOperationalStatus.builder()
        .status(cacheStatus.name())
        .recordedAtMillis(recordedAtMillis)
        .build();
  }

  @Nonnull
  public static EntityGraphOperationalStatus of(@Nonnull CacheStatus cacheStatus) {
    return of(cacheStatus, null);
  }

  @Nonnull
  public CacheStatus cacheStatus() {
    return CacheStatus.valueOf(status);
  }
}
