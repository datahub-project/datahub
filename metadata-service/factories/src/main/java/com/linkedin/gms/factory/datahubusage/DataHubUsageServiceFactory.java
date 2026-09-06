package com.linkedin.gms.factory.datahubusage;

import com.linkedin.gms.factory.analytics.PgAnalyticsEbeanConfigFactory;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.datahubusage.DataHubUsageService;
import com.linkedin.metadata.datahubusage.DataHubUsageServiceImpl;
import com.linkedin.metadata.datahubusage.postgres.PostgresDataHubUsageService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Usage-event audit search: Elasticsearch usage index when a cluster client exists, otherwise
 * pgAnalytics {@code {prefix}_event}.
 */
@Slf4j
@Configuration
@Import({PgAnalyticsEbeanConfigFactory.class})
public class DataHubUsageServiceFactory {

  @Bean
  @Nonnull
  public DataHubUsageService dataHubUsageService(
      @Autowired(required = false) SearchClientShim<?> elasticClient,
      @Autowired(required = false) @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
          IndexConvention indexConvention,
      @Autowired(required = false) @Nullable PgAnalyticsStoreRegistry pgAnalyticsStoreRegistry) {
    if (elasticClient != null && indexConvention != null) {
      return new DataHubUsageServiceImpl(elasticClient, indexConvention);
    }
    if (pgAnalyticsStoreRegistry != null) {
      return new PostgresDataHubUsageService(pgAnalyticsStoreRegistry);
    }
    throw new IllegalStateException(
        "DataHubUsageService requires elasticsearch.enabled=true or"
            + " postgres.pgAnalytics.enabled=true");
  }
}
