/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/** Factory for components required for any services using elasticsearch */
@Configuration
@Import({
  IndexConventionFactory.class,
  ElasticSearchBulkProcessorFactory.class,
  ElasticSearchIndexBuilderFactory.class
})
public class BaseElasticSearchComponentsFactory {
  @lombok.Value
  public static class BaseElasticSearchComponents {
    ElasticSearchConfiguration config;
    SearchClientShim<?> searchClient;
    IndexConvention indexConvention;
    ESBulkProcessor bulkProcessor;
    ESIndexBuilder indexBuilder;
  }

  @Autowired
  @Qualifier("searchClientShim")
  private SearchClientShim<?> searchClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Autowired
  @Qualifier("elasticSearchBulkProcessor")
  private ESBulkProcessor bulkProcessor;

  @Autowired
  @Qualifier("elasticSearchIndexBuilder")
  private ESIndexBuilder indexBuilder;

  @Bean(name = "baseElasticSearchComponents")
  @Nonnull
  protected BaseElasticSearchComponents getInstance(ConfigurationProvider configurationProvider) {
    return new BaseElasticSearchComponents(
        configurationProvider.getElasticSearch(),
        searchClient,
        indexConvention,
        bulkProcessor,
        indexBuilder);
  }
}
