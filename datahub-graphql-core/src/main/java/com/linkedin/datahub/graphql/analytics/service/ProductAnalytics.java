package com.linkedin.datahub.graphql.analytics.service;

import javax.annotation.Nonnull;

/**
 * Telemetry-facing analytics API that hides whether usage/metadata queries use the search cluster,
 * Postgres-backed usage events, or disabled search.
 */
public interface ProductAnalytics {

  @Nonnull
  AnalyticsService analyticsService();

  /** True when entity/document counts can be resolved via the configured search backend. */
  boolean isEntitySearchAvailable();

  /** Total CorpUser documents (match-all), or 0 when search is unavailable or on error. */
  int countCorpUsersTotal();

  /** CorpUser documents tagged as service accounts, or 0 when unavailable or on error. */
  int countCorpUserServiceAccounts();
}
