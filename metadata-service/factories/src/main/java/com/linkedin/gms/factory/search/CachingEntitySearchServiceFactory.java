/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class CachingEntitySearchServiceFactory {

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @Autowired private CacheManager cacheManager;

  @Value("${searchService.resultBatchSize}")
  private Integer batchSize;

  @Value("${searchService.enableCache}")
  private Boolean enableCache;

  @Bean(name = "cachingEntitySearchService")
  @Primary
  @Nonnull
  protected CachingEntitySearchService getInstance() {
    return new CachingEntitySearchService(
        cacheManager, entitySearchService, batchSize, enableCache);
  }
}
