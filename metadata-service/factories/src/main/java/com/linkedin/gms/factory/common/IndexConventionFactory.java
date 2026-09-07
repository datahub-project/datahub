package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.IndexPrefixResolver;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Creates a {@link IndexConvention} to generate search index names.
 *
 * <p>The index-name prefix is supplied by an {@link IndexPrefixResolver}. OSS wires the {@link
 * ConfiguredIndexPrefixResolver} default (a static deploy-wide prefix from configuration); an
 * extension module may contribute its own {@link IndexPrefixResolver} bean to resolve the prefix
 * per operation (e.g. per-namespace isolation), which suppresses this default via {@link
 * ConditionalOnMissingBean}.
 */
@Configuration
public class IndexConventionFactory {
  public static final String INDEX_CONVENTION_BEAN = "searchIndexConvention";

  @Bean
  @ConditionalOnMissingBean(IndexPrefixResolver.class)
  protected IndexPrefixResolver indexPrefixResolver(
      final ConfigurationProvider configurationProvider) {
    return new ConfiguredIndexPrefixResolver(
        configurationProvider.getElasticSearch().getIndex().getPrefix());
  }

  @Bean(name = INDEX_CONVENTION_BEAN)
  protected IndexConvention createInstance(
      final ConfigurationProvider configurationProvider,
      final IndexPrefixResolver indexPrefixResolver) {
    ElasticSearchConfiguration elasticSearchConfiguration =
        configurationProvider.getElasticSearch();
    return new IndexConventionImpl(
        IndexConventionImpl.IndexConventionConfig.builder()
            .hashIdAlgo(elasticSearchConfiguration.getIdHashAlgo())
            .schemaFieldDocIdHashEnabled(
                elasticSearchConfiguration
                    .getIndex()
                    .getDocIds()
                    .getSchemaField()
                    .isHashIdEnabled())
            .build(),
        indexPrefixResolver,
        elasticSearchConfiguration.getEntityIndex());
  }
}
