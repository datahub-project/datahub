package com.linkedin.metadata.datahubusage;

import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Retrieval of usage-event-based home recommendation entity URNs independent of persistence
 * (PostgreSQL partitioned usage events vs OpenSearch usage index).
 */
public interface UsageEventsRecommendationDataAccess {

  /** Whether usage-based recommendation modules may run (e.g. index or partitions exist). */
  boolean isDataAvailable(@Nonnull OperationContext opContext);

  @Nonnull
  List<String> recentlyViewedEntityUrns(@Nonnull OperationContext opContext);

  @Nonnull
  List<String> mostPopularEntityUrns(@Nonnull OperationContext opContext);

  @Nonnull
  List<String> recentlyEditedEntityUrns(@Nonnull OperationContext opContext);
}
