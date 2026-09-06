package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Store-agnostic access to DataHub usage events for recommendation modules (recently viewed,
 * edited, searched, most popular). Elasticsearch and pgAnalytics each provide an implementation.
 */
public interface UsageEventRecommendationBackend {

  boolean isAvailable(@Nonnull OperationContext opContext);

  @Nonnull
  List<String> recentEntityUrns(
      @Nonnull OperationContext opContext,
      @Nonnull Urn actorUrn,
      @Nonnull String eventType,
      int limit);

  @Nonnull
  List<String> mostViewedEntityUrns(
      @Nonnull OperationContext opContext,
      @Nonnull String eventType,
      int limit,
      @Nullable List<String> actorPeers);

  @Nonnull
  List<String> recentSearchQueries(
      @Nonnull OperationContext opContext, @Nonnull Urn actorUrn, int limit);
}
