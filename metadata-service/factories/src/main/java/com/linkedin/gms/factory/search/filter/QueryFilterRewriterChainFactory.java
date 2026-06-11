package com.linkedin.gms.factory.search.filter;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.query.filter.ContainerExpansionRewriter;
import com.linkedin.metadata.search.elasticsearch.query.filter.DomainExpansionRewriter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriter;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryFilterRewriterChainFactory {

  @Autowired(required = false)
  @Nullable
  private MetricUtils metricUtils;

  @Bean
  @ConditionalOnProperty(
      name = "searchService.queryFilterRewriter.containerExpansion.enabled",
      havingValue = "true")
  public QueryFilterRewriter containerExpansionRewriter(
      final ConfigurationProvider configurationProvider) {
    ContainerExpansionRewriter rewriter =
        ContainerExpansionRewriter.builder()
            .config(
                configurationProvider
                    .getSearchService()
                    .getQueryFilterRewriter()
                    .getContainerExpansion())
            .build();
    rewriter.setMetricUtils(metricUtils);
    return rewriter;
  }

  @Bean
  @ConditionalOnProperty(
      name = "searchService.queryFilterRewriter.domainExpansion.enabled",
      havingValue = "true")
  public QueryFilterRewriter domainExpansionRewriter(
      final ConfigurationProvider configurationProvider) {
    DomainExpansionRewriter rewriter =
        DomainExpansionRewriter.builder()
            .config(
                configurationProvider
                    .getSearchService()
                    .getQueryFilterRewriter()
                    .getDomainExpansion())
            .build();
    rewriter.setMetricUtils(metricUtils);
    return rewriter;
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @Bean
  public QueryFilterRewriteChain queryFilterRewriteChain(
      Optional<List<QueryFilterRewriter>> queryFilterRewriters) {
    return new QueryFilterRewriteChain(queryFilterRewriters.orElse(Collections.emptyList()));
  }
}
