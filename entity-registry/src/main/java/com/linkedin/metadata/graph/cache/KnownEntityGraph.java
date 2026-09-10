package com.linkedin.metadata.graph.cache;

import java.util.Locale;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * First-party entity graph definitions that GMS call sites reference directly. Each constant maps
 * to a {@code graphs.<configKey>} entry in {@code entity-graph-cache.yaml}. Source and scope here
 * are defaults: operators may disable a known graph or overlay {@code buildSource} within {@link
 * #allowsBuildSource(GraphSnapshotSource)}. Enabled known graphs must still match {@link
 * #expectedScope}.
 */
@Getter
public enum KnownEntityGraph {
  DOMAIN("domain", GraphSnapshotSource.SEARCH, ScopeRequirement.FULL),
  GLOSSARY("glossary", GraphSnapshotSource.GRAPH, ScopeRequirement.PARTIAL),
  CONTAINER("container", GraphSnapshotSource.GRAPH, ScopeRequirement.PARTIAL),
  MEMBERSHIP("membership", GraphSnapshotSource.GRAPH, ScopeRequirement.FULL);

  @Nonnull private final String configKey;
  @Nonnull private final GraphSnapshotSource expectedBuildSource;
  @Nonnull private final ScopeRequirement expectedScope;

  KnownEntityGraph(
      @Nonnull String configKey,
      @Nonnull GraphSnapshotSource expectedBuildSource,
      @Nonnull ScopeRequirement expectedScope) {
    this.configKey = configKey;
    this.expectedBuildSource = expectedBuildSource;
    this.expectedScope = expectedScope;
  }

  @Nullable
  public static KnownEntityGraph fromConfigKey(@Nullable String configKey) {
    if (configKey == null || configKey.isBlank()) {
      return null;
    }
    for (KnownEntityGraph known : values()) {
      if (known.configKey.equals(configKey)) {
        return known;
      }
    }
    return null;
  }

  /**
   * Overlay-compatible {@code buildSource} values for this graph's required {@link #expectedScope}.
   * FULL known graphs accept graph or search; PARTIAL known graphs accept graph only (primary and
   * search PARTIAL are FORWARD-only).
   */
  public boolean allowsBuildSource(@Nonnull GraphSnapshotSource source) {
    return switch (expectedScope) {
      case FULL -> source == GraphSnapshotSource.GRAPH || source == GraphSnapshotSource.SEARCH;
      case PARTIAL -> source == GraphSnapshotSource.GRAPH;
    };
  }

  @Nonnull
  public String allowedBuildSourcesDescription() {
    return switch (expectedScope) {
      case FULL -> "graph or search";
      case PARTIAL -> "graph";
    };
  }

  @Nonnull
  public String expectedBuildSourceYaml() {
    return expectedBuildSource.name().toLowerCase(Locale.ROOT);
  }

  /** Expected {@code scope.mode} for this graph (validated at registry build time). */
  public enum ScopeRequirement {
    FULL,
    PARTIAL
  }
}
