package com.linkedin.metadata.graph.cache.client;

import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Parameters for a cache expand read via {@link EntityGraphCacheClients#expand}. */
@Value
@Builder
public class GraphExpandRequest {

  @Nonnull OperationContext opContext;

  @Nonnull EntityGraphCache cache;

  @Nullable EntityGraphBinding binding;

  @Nullable String graphId;

  @Nullable GraphSnapshotSource source;

  @Nonnull TraversalDirection direction;

  @Nonnull Collection<String> roots;

  int limit;

  @Builder.Default int maxDepth = EntityGraphCache.USE_DEFINITION_MAX_DEPTH;
}
