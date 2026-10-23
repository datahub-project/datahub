package com.linkedin.metadata.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Whether an entity type this registry doesn't know was registered by a newer version. Each
 * version's system update ({@code IngestEntityTypesStep}) writes {@code
 * urn:li:entityType:datahub.<name>} for every type in its registry, and the row stays after a
 * rollback. A name with no row (a typo such as {@code corpusr}, or a plugin that never registered)
 * is not from a newer version, so it keeps the pre-tolerance rejection.
 *
 * <p>Found rows are cached; misses expire after a minute, because the row can lag the first writes
 * of a new type while the newer version is still coming up.
 */
@Slf4j
public final class NextVersionEntityTypes implements Predicate<String> {

  private static final String ENTITY_TYPE_URN_FORMAT = "urn:li:entityType:datahub.%s";
  private static final int MAX_CACHED_NAMES = 1000;

  private final Predicate<Urn> rowExists;
  private final Cache<String, Boolean> found =
      CacheBuilder.newBuilder().maximumSize(MAX_CACHED_NAMES).build();
  private final Cache<String, Boolean> missing =
      CacheBuilder.newBuilder()
          .maximumSize(MAX_CACHED_NAMES)
          .expireAfterWrite(1, TimeUnit.MINUTES)
          .build();

  /**
   * @param rowExists whether the given {@code entityType} urn has its {@code entityTypeInfo} row
   */
  public NextVersionEntityTypes(@Nonnull final Predicate<Urn> rowExists) {
    this.rowExists = rowExists;
  }

  @Override
  public boolean test(@Nullable final String entityType) {
    if (entityType == null || entityType.isEmpty()) {
      return false;
    }
    if (found.getIfPresent(entityType) != null) {
      return true;
    }
    if (missing.getIfPresent(entityType) != null) {
      return false;
    }
    final Urn entityTypeUrn;
    try {
      entityTypeUrn = Urn.createFromString(String.format(ENTITY_TYPE_URN_FORMAT, entityType));
    } catch (URISyntaxException e) {
      return false;
    }
    final boolean exists;
    try {
      exists = rowExists.test(entityTypeUrn);
    } catch (RuntimeException e) {
      // Without an answer, keep tolerating: failing reads of a newer version's data is what the
      // tolerance exists to prevent. Not cached, so the next lookup retries.
      log.warn("Could not look up {}; tolerating entity type {}", entityTypeUrn, entityType, e);
      return true;
    }
    (exists ? found : missing).put(entityType, Boolean.TRUE);
    return exists;
  }
}
