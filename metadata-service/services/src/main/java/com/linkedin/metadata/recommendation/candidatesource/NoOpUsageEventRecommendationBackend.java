package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Usage-event modules stay registered but ineligible when no ES or pgAnalytics store is present.
 */
public final class NoOpUsageEventRecommendationBackend implements UsageEventRecommendationBackend {

  @Override
  public boolean isAvailable(@Nonnull OperationContext opContext) {
    return false;
  }

  @Override
  @Nonnull
  public List<String> recentEntityUrns(
      @Nonnull OperationContext opContext,
      @Nonnull Urn actorUrn,
      @Nonnull String eventType,
      int limit) {
    return Collections.emptyList();
  }

  @Override
  @Nonnull
  public List<String> mostViewedEntityUrns(
      @Nonnull OperationContext opContext,
      @Nonnull String eventType,
      int limit,
      @Nullable List<String> actorPeers) {
    return Collections.emptyList();
  }

  @Override
  @Nonnull
  public List<String> recentSearchQueries(
      @Nonnull OperationContext opContext, @Nonnull Urn actorUrn, int limit) {
    return Collections.emptyList();
  }
}
