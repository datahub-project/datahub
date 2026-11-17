package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.search.RefreshIntervals;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.util.Pair;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Resolves {@code index.refresh_interval} seconds for one index.
 *
 * <p>Precedence is the entity or aspect override, then the service field. There is no per-index
 * name map and no fall-through to {@code refreshIntervalSeconds}.
 */
public final class RefreshIntervalResolver {
  private static final String V3_SUFFIX = "index_v3";

  private RefreshIntervalResolver() {}

  public static int resolveSeconds(
      @Nullable RefreshIntervals intervals,
      @Nonnull IndexConvention convention,
      @Nonnull OperationFingerprint operation,
      @Nonnull String indexName) {
    if (intervals == null) {
      throw new IllegalStateException(
          "elasticsearch.index.refreshIntervals is not configured; cannot set refresh_interval for "
              + indexName);
    }
    String normalized = indexName.toLowerCase(Locale.ROOT);
    SearchComponent component = convention.componentForIndex(operation, normalized);
    if (component == null) {
      throw new IllegalStateException(
          "Cannot resolve refresh_interval for index '"
              + indexName
              + "'; it does not match a known index service");
    }
    return switch (component) {
      case SEARCH_V2, SEARCH_V3, SEMANTIC ->
          entitySeconds(intervals, convention, operation, component, normalized);
      case TIMESERIES -> timeseriesSeconds(intervals, convention, operation, normalized);
      case GRAPH -> require(intervals.getGraphSeconds(), "graphSeconds", indexName);
      case SYSTEM_METADATA ->
          require(intervals.getSystemMetadataSeconds(), "systemMetadataSeconds", indexName);
      case USAGE -> require(intervals.getUsageSeconds(), "usageSeconds", indexName);
    };
  }

  public static String toSetting(int seconds) {
    return seconds + "s";
  }

  /** True when both values are the same refresh duration ({@code 30s} equals {@code 30000ms}). */
  public static boolean sameDuration(@Nullable String left, @Nullable String right) {
    Long leftMs = toMillis(left);
    Long rightMs = toMillis(right);
    if (leftMs == null || rightMs == null) {
      return false;
    }
    return leftMs.equals(rightMs);
  }

  @Nullable
  static Long toMillis(@Nullable String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim().toLowerCase(Locale.ROOT);
    if (trimmed.isEmpty()) {
      return null;
    }
    try {
      if (trimmed.endsWith("ms")) {
        return Long.parseLong(trimmed.substring(0, trimmed.length() - 2).trim());
      }
      if (trimmed.endsWith("s")) {
        return Long.parseLong(trimmed.substring(0, trimmed.length() - 1).trim()) * 1000L;
      }
      if (trimmed.endsWith("m")) {
        return Long.parseLong(trimmed.substring(0, trimmed.length() - 1).trim()) * 60_000L;
      }
      if (trimmed.endsWith("h")) {
        return Long.parseLong(trimmed.substring(0, trimmed.length() - 1).trim()) * 3_600_000L;
      }
      if (trimmed.chars().allMatch(Character::isDigit) || trimmed.startsWith("-")) {
        return Long.parseLong(trimmed);
      }
    } catch (NumberFormatException ignored) {
      return null;
    }
    return null;
  }

  private static int entitySeconds(
      RefreshIntervals intervals,
      IndexConvention convention,
      OperationFingerprint operation,
      SearchComponent component,
      String indexName) {
    String stem =
        switch (component) {
          case SEMANTIC -> convention.getEntityNameSemantic(operation, indexName).orElse(null);
          case SEARCH_V2 -> convention.getEntityName(operation, indexName).orElse(null);
          case SEARCH_V3 ->
              stemBeforeSuffix(convention, operation, component, indexName, V3_SUFFIX);
          default -> null;
        };
    if (stem != null) {
      Integer override = intervals.entityOverrides().get(stem.toLowerCase(Locale.ROOT));
      if (override != null) {
        return override;
      }
    }
    return require(intervals.getEntitySeconds(), "entitySeconds", indexName);
  }

  private static int timeseriesSeconds(
      RefreshIntervals intervals,
      IndexConvention convention,
      OperationFingerprint operation,
      String indexName) {
    Optional<Pair<String, String>> entityAndAspect =
        convention.getEntityAndAspectName(operation, indexName);
    if (entityAndAspect.isPresent()) {
      Integer override =
          intervals
              .aspectOverrides()
              .get(entityAndAspect.get().getSecond().toLowerCase(Locale.ROOT));
      if (override != null) {
        return override;
      }
    }
    return require(intervals.getTimeseriesSeconds(), "timeseriesSeconds", indexName);
  }

  @Nullable
  private static String stemBeforeSuffix(
      IndexConvention convention,
      OperationFingerprint operation,
      SearchComponent component,
      String indexName,
      String suffix) {
    String prefix =
        convention
            .getPrefix(operation, component)
            .map(value -> value.toLowerCase(Locale.ROOT) + "_")
            .orElse("");
    if (!prefix.isEmpty() && !indexName.startsWith(prefix)) {
      return null;
    }
    String base = indexName.substring(prefix.length());
    if (!base.endsWith(suffix) || base.length() == suffix.length()) {
      return null;
    }
    return base.substring(0, base.length() - suffix.length());
  }

  private static int require(@Nullable Integer seconds, String field, String indexName) {
    if (seconds == null) {
      throw new IllegalStateException(
          "elasticsearch.index.refreshIntervals."
              + field
              + " is not configured; cannot set refresh_interval for "
              + indexName);
    }
    return seconds;
  }
}
