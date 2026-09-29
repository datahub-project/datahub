package com.linkedin.metadata.graph.cache.service.rebuild;

import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Scope-specific snapshot rebuild logic invoked from {@link GraphCacheRebuilder}. */
public interface GraphScopeRebuildStrategy {

  @Nonnull
  BuildResult executeRebuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nullable Collection<String> seeds,
      @Nullable String publishKeyHint,
      @Nonnull TraversalDirection direction);
}
