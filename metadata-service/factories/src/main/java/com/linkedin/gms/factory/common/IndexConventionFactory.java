package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Creates a {@link IndexConvention} to generate search index names.
 *
 * <p>This allows you to easily add prefixes to the index names.
 */
@Configuration
public class IndexConventionFactory {
  public static final String INDEX_CONVENTION_BEAN = "searchIndexConvention";

  @Bean(name = INDEX_CONVENTION_BEAN)
  protected IndexConvention createInstance(final ConfigurationProvider configurationProvider) {
    ElasticSearchConfiguration elasticSearchConfiguration =
        configurationProvider.getElasticSearch();
    return new IndexConventionImpl(
        IndexConventionImpl.IndexConventionConfig.builder()
            .prefix(elasticSearchConfiguration.getIndex().getPrefix())
            .hashIdAlgo(elasticSearchConfiguration.getIdHashAlgo())
            .schemaFieldDocIdHashEnabled(
                elasticSearchConfiguration
                    .getIndex()
                    .getDocIds()
                    .getSchemaField()
                    .isHashIdEnabled())
            .build());
  }
}
