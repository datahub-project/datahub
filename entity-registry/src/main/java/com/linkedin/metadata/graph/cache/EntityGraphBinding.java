package com.linkedin.metadata.graph.cache;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/** Resolved graph cache binding for a call site (graph id + snapshot source). */
@Value
@Builder
public class EntityGraphBinding {
  @Nonnull String graphId;
  @Nonnull GraphSnapshotSource source;
}
