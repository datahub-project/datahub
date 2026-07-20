package com.linkedin.metadata.graph.cache;

import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * First-party entity graph definitions that GMS call sites reference directly. Each constant maps
 * to a {@code graphs.<configKey>} entry in {@code entity-graph-cache.yaml}. When the entity graph
 * cache is enabled, {@link com.linkedin.metadata.graph.cache.config.EntityGraphRegistry} requires
 * every value here to exist, be enabled, and match the expected {@link GraphSnapshotSource} and
 * scope mode.
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

  /** Expected {@code scope.mode} for this graph (validated at registry build time). */
  public enum ScopeRequirement {
    FULL,
    PARTIAL
  }
}
