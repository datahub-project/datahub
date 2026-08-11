package com.linkedin.metadata.kafka.config;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.kafka.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.kafka.usage.DataHubUsageEventIndexer;
import com.linkedin.metadata.kafka.usage.ElasticsearchDataHubUsageEventIndexer;
import com.linkedin.metadata.kafka.usage.PostgresDataHubUsageEventIndexer;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UsageEventsIndexingConfiguration {

  @Bean(name = "dataHubUsageEventIndexer")
  @Conditional(PostgresUsageEventsImplementationCondition.class)
  @Nonnull
  public DataHubUsageEventIndexer postgresDataHubUsageEventIndexer(
      PgAnalyticsStoreRegistry pgAnalyticsStoreRegistry) {
    return new PostgresDataHubUsageEventIndexer(pgAnalyticsStoreRegistry);
  }

  @Bean(name = "dataHubUsageEventIndexer")
  @Conditional(ElasticsearchUsageEventsImplementationCondition.class)
  @Nonnull
  public DataHubUsageEventIndexer elasticsearchDataHubUsageEventIndexer(
      ElasticsearchConnector elasticsearchConnector,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {
    return new ElasticsearchDataHubUsageEventIndexer(elasticsearchConnector, indexConvention);
  }
}
