package com.linkedin.metadata.graph.cache.service.rebuild;

import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** FULL-scope rebuild: single scroll snapshot at the full cache key. */
public class FullGraphRebuildStrategy implements GraphScopeRebuildStrategy {
  private final EntityGraphSnapshotBuilder snapshotBuilder;
  private final OperationContext systemOperationContext;

  public FullGraphRebuildStrategy(
      @Nonnull EntityGraphSnapshotBuilder snapshotBuilder,
      @Nonnull OperationContext systemOperationContext) {
    this.snapshotBuilder = snapshotBuilder;
    this.systemOperationContext = systemOperationContext;
  }

  @Nonnull
  @Override
  public BuildResult executeRebuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nullable Collection<String> seeds,
      @Nullable String publishKeyHint,
      @Nonnull TraversalDirection direction) {
    return snapshotBuilder.build(systemOperationContext, definition, source, seeds);
  }
}
