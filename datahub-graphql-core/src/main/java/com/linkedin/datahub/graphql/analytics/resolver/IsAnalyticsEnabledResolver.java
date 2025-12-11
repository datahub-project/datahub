/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.analytics.resolver;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

/** Returns true if analytics feature flag is enabled, false otherwise. */
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
