package com.linkedin.gms.factory.search.filter;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.query.filter.ContainerExpansionRewriter;
import com.linkedin.metadata.search.elasticsearch.query.filter.DomainExpansionRewriter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriter;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryFilterRewriterChainFactory {

  @Bean
  @ConditionalOnProperty(
      name = "searchService.queryFilterRewriter.containerExpansion.enabled",
      havingValue = "true")
  public QueryFilterRewriter containerExpansionRewriter(
      final ConfigurationProvider configurationProvider) {
    return ContainerExpansionRewriter.builder()
        .config(
            configurationProvider
                .getSearchService()
                .getQueryFilterRewriter()
                .getContainerExpansion())
        .build();
  }

  @Bean
  @ConditionalOnProperty(
      name = "searchService.queryFilterRewriter.domainExpansion.enabled",
      havingValue = "true")
  public QueryFilterRewriter domainExpansionRewriter(
      final ConfigurationProvider configurationProvider) {
    return DomainExpansionRewriter.builder()
        .config(
            configurationProvider.getSearchService().getQueryFilterRewriter().getDomainExpansion())
        .build();
  }

  @Bean
  @org.springframework.boot.autoconfigure.condition.ConditionalOnBean(
      com.linkedin.metadata.entity.EntityService.class)
  public QueryFilterRewriter organizationFilterRewriter(
      @org.springframework.beans.factory.annotation.Qualifier("entityService")
          final com.linkedin.metadata.entity.EntityService<?> entityService,
      @org.springframework.beans.factory.annotation.Qualifier("entityRegistry")
          final com.linkedin.metadata.models.registry.EntityRegistry entityRegistry) {
    return new com.linkedin.metadata.search.elasticsearch.query.filter.OrganizationFilterRewriter(
        com.linkedin.metadata.entity.EntityServiceAspectRetriever.builder()
            .entityService(entityService)
            .entityRegistry(entityRegistry)
            .build());
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @Bean
  public QueryFilterRewriteChain queryFilterRewriteChain(
      Optional<List<QueryFilterRewriter>> queryFilterRewriters) {
    return new QueryFilterRewriteChain(queryFilterRewriters.orElse(Collections.emptyList()));
  }
}
