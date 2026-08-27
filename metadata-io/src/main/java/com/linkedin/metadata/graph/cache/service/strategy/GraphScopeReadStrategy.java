package com.linkedin.metadata.graph.cache.service.strategy;

import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import java.util.Collection;
import javax.annotation.Nonnull;

/** Scope-specific read orchestration for entity graph cache snapshots. */
public interface GraphScopeReadStrategy {

  @Nonnull
  GraphReadResult expandCached(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth);

  @Nonnull
  GraphReadResult expandEphemeral(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth);

  @Nonnull
  AncestorWalkResult walkCached(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seed,
      int maxDepth);

  @Nonnull
  AncestorWalkResult walkEphemeral(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seed,
      int maxDepth);
}
