package com.linkedin.gms.factory.datahubusage;

import com.linkedin.metadata.datahubusage.DataHubUsageService;
import com.linkedin.metadata.datahubusage.DataHubUsageServiceImpl;
import com.linkedin.metadata.datahubusage.PostgresDataHubUsageService;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class DataHubUsageServiceFactory {

  @Bean
  @ConditionalOnProperty(
      prefix = "platformAnalytics.usage-events",
      name = "implementation",
      havingValue = "postgres")
  public DataHubUsageService postgresDataHubUsageService(
      PostgresUsageEventsStore postgresUsageEventsStore, IndexConvention indexConvention) {
    return new PostgresDataHubUsageService(postgresUsageEventsStore, indexConvention);
  }

  @Bean
  @ConditionalOnProperty(
      prefix = "platformAnalytics.usage-events",
      name = "implementation",
      havingValue = "elasticsearch",
      matchIfMissing = true)
  public DataHubUsageService elasticsearchDataHubUsageService(
      SearchClientShim<?> elasticClient, IndexConvention indexConvention) {
    return new DataHubUsageServiceImpl(elasticClient, indexConvention);
  }
}
