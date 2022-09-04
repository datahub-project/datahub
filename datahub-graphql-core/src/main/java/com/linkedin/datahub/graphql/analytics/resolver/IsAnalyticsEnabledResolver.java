package com.linkedin.datahub.graphql.analytics.resolver;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;


/**
 * Returns true if analytics feature flag is enabled, false otherwise.
 */
public class IsAnalyticsEnabledResolver implements DataFetcher<Boolean> {

  private final Boolean _isAnalyticsEnabled;

  public IsAnalyticsEnabledResolver(final Boolean isAnalyticsEnabled) {
      _isAnalyticsEnabled = isAnalyticsEnabled;
  }

  @Override
  public final Boolean get(DataFetchingEnvironment environment) throws Exception {
      return _isAnalyticsEnabled;
  }
}
