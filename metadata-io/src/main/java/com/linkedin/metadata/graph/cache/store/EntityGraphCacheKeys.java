package com.linkedin.metadata.graph.cache.store;

import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import java.util.Locale;
import javax.annotation.Nonnull;

public final class EntityGraphCacheKeys {

  private static final String SOURCE_SEPARATOR = "@";
  private static final String COMPONENT_SEPARATOR = ":";
  private static final String MARKER_INFIX = ":marker:";

  private EntityGraphCacheKeys() {}

  @Nonnull
  public static String sourcePrefix(@Nonnull String graphId, @Nonnull GraphSnapshotSource source) {
    return graphId + SOURCE_SEPARATOR + source.name().toLowerCase(Locale.ROOT);
  }

  @Nonnull
  public static String fullCacheKey(@Nonnull String graphId, @Nonnull GraphSnapshotSource source) {
    return sourcePrefix(graphId, source);
  }

  @Nonnull
  public static String componentCacheKey(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String componentFingerprint) {
    return sourcePrefix(graphId, source) + COMPONENT_SEPARATOR + componentFingerprint;
  }

  @Nonnull
  public static String partialFailureMarkerKey(
      @Nonnull String graphId, @Nonnull GraphSnapshotSource source, @Nonnull String root) {
    return sourcePrefix(graphId, source) + MARKER_INFIX + root.toLowerCase(Locale.ROOT);
  }

  /** True for FULL-scope keys ({@code graphId@source}) without a component suffix. */
  public static boolean isFullScopeCacheKey(@Nonnull String cacheKey) {
    return !cacheKey.contains(COMPONENT_SEPARATOR);
  }

  @Nonnull
  public static String graphIdFromCacheKey(@Nonnull String cacheKey) {
    int sourceSep = cacheKey.indexOf(SOURCE_SEPARATOR);
    return sourceSep < 0 ? cacheKey : cacheKey.substring(0, sourceSep);
  }

  @Nonnull
  public static GraphSnapshotSource sourceFromCacheKey(@Nonnull String cacheKey) {
    int sourceSep = cacheKey.indexOf(SOURCE_SEPARATOR);
    if (sourceSep < 0) {
      throw new IllegalArgumentException(
          "Invalid entity graph cache key (missing '@' source separator): " + cacheKey);
    }
    int componentSep = cacheKey.indexOf(COMPONENT_SEPARATOR, sourceSep + 1);
    String sourceToken =
        componentSep < 0
            ? cacheKey.substring(sourceSep + 1)
            : cacheKey.substring(sourceSep + 1, componentSep);
    return GraphSnapshotSource.valueOf(sourceToken.toUpperCase(Locale.ROOT));
  }

  public static boolean cacheKeyMatchesSource(
      @Nonnull String cacheKey, @Nonnull String graphId, @Nonnull GraphSnapshotSource source) {
    return cacheKey.startsWith(sourcePrefix(graphId, source));
  }

  public static boolean isFailureMarkerKey(@Nonnull String cacheKey) {
    return cacheKey.contains(MARKER_INFIX);
  }
}
