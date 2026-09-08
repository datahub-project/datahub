package com.linkedin.metadata.recommendation.candidatesource.postgres;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.recommendation.candidatesource.UsageEventRecommendationBackend;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PostgresUsageEventRecommendationBackend implements UsageEventRecommendationBackend {

  private final PgAnalyticsStoreRegistry registry;
  private final int recommendationLookbackDays;

  @Override
  public boolean isAvailable(@Nonnull OperationContext opContext) {
    return true;
  }

  @Override
  @Nonnull
  public List<String> recentEntityUrns(
      @Nonnull OperationContext opContext,
      @Nonnull Urn actorUrn,
      @Nonnull String eventType,
      int limit) {
    return PostgresUsageRecommendationQueries.recentEntityUrns(
        store(), actorUrn.toString(), eventType, limit, recommendationLookbackDays);
  }

  @Override
  @Nonnull
  public List<String> mostViewedEntityUrns(
      @Nonnull OperationContext opContext,
      @Nonnull String eventType,
      int limit,
      @Nullable List<String> actorPeers) {
    return PostgresUsageRecommendationQueries.mostViewedEntityUrns(
        store(), eventType, limit, actorPeers, recommendationLookbackDays);
  }

  @Override
  @Nonnull
  public List<String> recentSearchQueries(
      @Nonnull OperationContext opContext, @Nonnull Urn actorUrn, int limit) {
    return PostgresUsageRecommendationQueries.recentSearchQueries(
        store(), actorUrn.toString(), limit, recommendationLookbackDays);
  }

  private PostgresAnalyticsStore store() {
    return registry.resolve(AnalyticsMetricFamilies.DATAHUB_USAGE).getStore();
  }
}
