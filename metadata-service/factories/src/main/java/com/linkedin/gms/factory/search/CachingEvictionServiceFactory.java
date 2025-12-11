/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.client.CacheEvictionService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class CachingEvictionServiceFactory {

  @Autowired private CacheManager cacheManager;

  @Value("${searchService.enableCache}")
  private Boolean cachingEnabled;

  @Value("${searchService.enableEviction}")
  private Boolean enableEviction;

  @Bean(name = "cachingEvictionService")
  @Primary
  @Nonnull
  protected CacheEvictionService getInstance() {
    return new CacheEvictionService(cacheManager, cachingEnabled, enableEviction);
  }
}
