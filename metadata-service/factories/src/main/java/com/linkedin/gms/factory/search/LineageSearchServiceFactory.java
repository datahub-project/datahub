/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import javax.annotation.Nonnull;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class LineageSearchServiceFactory {

  public static final String LINEAGE_SEARCH_SERVICE_CACHE_NAME = "relationshipSearchService";

  @Bean(name = "relationshipSearchService")
  @Primary
  @Nonnull
  protected LineageSearchService getInstance(
      CacheManager cacheManager,
      GraphService graphService,
      SearchService searchService,
      ConfigurationProvider configurationProvider) {
    boolean cacheEnabled = configurationProvider.getFeatureFlags().isLineageSearchCacheEnabled();
    return new LineageSearchService(
        searchService,
        graphService,
        cacheEnabled ? cacheManager.getCache(LINEAGE_SEARCH_SERVICE_CACHE_NAME) : null,
        cacheEnabled,
        configurationProvider);
  }
}
