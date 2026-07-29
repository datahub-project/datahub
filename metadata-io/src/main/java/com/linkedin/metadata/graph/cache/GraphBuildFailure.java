package com.linkedin.metadata.graph.cache;

import java.util.Locale;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Classifies snapshot build failure reasons for cache status and retry policy. */
public final class GraphBuildFailure {

  private static final Set<String> TRANSIENT_REASONS =
      Set.of(
          "graph_failed",
          "search_failed",
          "scroll_incomplete",
          "empty_snapshot",
          "empty_component");

  private static final Set<String> STRUCTURAL_REASONS = Set.of("vertex_limit", "edge_limit");

  private static final Set<String> CONFIG_REASONS =
      Set.of(
          "primary_full_unsupported",
          "primary_reverse_unsupported",
          "search_not_configured",
          "search_reverse_unsupported",
          "partial_requires_seeds",
          "invalid_configuration");

  private GraphBuildFailure() {}

  public enum Kind {
    TRANSIENT,
    STRUCTURAL,
    CONFIG
  }

  @Nonnull
  public static Kind classify(@Nullable String reason) {
    if (reason == null || reason.isBlank()) {
      return Kind.TRANSIENT;
    }
    String normalized = reason.toLowerCase(Locale.ROOT);
    if (CONFIG_REASONS.contains(normalized)) {
      return Kind.CONFIG;
    }
    if (STRUCTURAL_REASONS.contains(normalized)) {
      return Kind.STRUCTURAL;
    }
    if (TRANSIENT_REASONS.contains(normalized)) {
      return Kind.TRANSIENT;
    }
    return Kind.TRANSIENT;
  }

  /** Status recorded when a build produces no publishable snapshot. */
  @Nonnull
  public static CacheStatus statusForFailedBuild(@Nullable String reason) {
    return switch (classify(reason)) {
      case TRANSIENT -> CacheStatus.COOLDOWN;
      case STRUCTURAL -> CacheStatus.OVER_LIMIT;
      case CONFIG -> CacheStatus.INVALID;
    };
  }

  @Nonnull
  public static String failureMetric(@Nullable String reason) {
    return switch (classify(reason)) {
      case TRANSIENT -> "entity.graph.cache.cooldown";
      case STRUCTURAL -> "entity.graph.cache.over_limit";
      case CONFIG -> "entity.graph.cache.invalid";
    };
  }

  public static boolean suppressesAutomaticRebuild(@Nonnull CacheStatus status) {
    return status == CacheStatus.OVER_LIMIT || status == CacheStatus.INVALID;
  }
}
